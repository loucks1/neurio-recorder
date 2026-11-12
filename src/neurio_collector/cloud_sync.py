"""Harvest and tail sync for Neurio cloud hourly stats.

Two jobs, one thread, fully isolated from local ingest: any cloud failure
is recorded in ``cloud_sync_state`` and swallowed, so a cloud outage can
never stall the poller or the read API.

- Harvest: a one-time, resumable backfill from the configured floor (or
  the default floor) up to now, in fixed chunks. Progress is persisted
  after every chunk commits, so a restart resumes where it stopped
  instead of starting over.
- Tail sync: every cycle, the trailing ``cloud_tail_hours`` are
  re-fetched and upserted, so late or corrected cloud values replace the
  provisional ones in place.

Windows are chunked at 10 days (240 hourly items per response, well under
the API's paging sizes) and both API window bounds are inclusive, so
adjacent chunks share a boundary hour; the hour-end upsert makes that
overlap a no-op.

Neurio allows 144 authorized requests per hour. Chunks are paced to the
configured budget (default 72/hour, so other clients on the same
credentials keep half the window). A 429 ``rate_limit_exceeding`` waits
until the reset time in the body (or the RateLimit-Reset header) and
then resumes the same chunk. The stop flag is checked between chunks and
during that wait, so shutdown does not sit through the rest of a harvest.
"""

from __future__ import annotations

import threading
from datetime import datetime, timedelta, timezone

from .cloud_client import CloudError, NeurioCloudClient
from .cloud_ingest import read_sync_state, store_cloud_hours, update_sync_state
from .config import CLOUD_RATE_WINDOW_SECONDS, Config
from .db import Database
from .timeutil import format_utc, parse_instant, utcnow

CHUNK = timedelta(days=10)
DEFAULT_HARVEST_FLOOR = datetime(2016, 1, 1, tzinfo=timezone.utc)
# A reset hint far past the published one-hour window is not trusted.
_MAX_BACKOFF_SECONDS = CLOUD_RATE_WINDOW_SECONDS + 120
_UNSET = object()


def public_cloud_error(exc: BaseException) -> str:
    """A short status string. Response bodies are not included.

    CloudError messages are built without the response body. Other
    exceptions contribute only their type name, so a message that
    happened to contain a body or a credential cannot reach the API.
    """
    if isinstance(exc, CloudError):
        detail = str(exc).replace("\n", " ").strip()
        text = f"CloudError: {detail}" if detail else "CloudError"
    else:
        text = type(exc).__name__
    return text[:200]


class CloudSyncer:
    def __init__(self, cfg: Config, db: Database, client=None, clock=None, sleeper=None):
        self.cfg = cfg
        self.db = db
        self.client = client or NeurioCloudClient(cfg)
        self.clock = clock or utcnow
        # sleeper(seconds) -> True when the wait was interrupted. The
        # default waits on the stop event, so shutdown wakes it.
        self._sleeper = sleeper or self._stop_wait
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._last_cloud_request_at: datetime | None = None
        self._backoff_until = _UNSET

    def start(self) -> None:
        self._thread = threading.Thread(
            target=self._loop, name="neurio-cloud-sync", daemon=True
        )
        self._thread.start()

    def stop(self, timeout: float = 5) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout)

    def _stop_wait(self, seconds: float) -> bool:
        return self._stop.wait(seconds)

    def _loop(self) -> None:
        while not self._stop.is_set():
            self.run_cycle()
            if self._stop.is_set():
                return
            self._stop.wait(self.cfg.cloud_sync_interval_seconds)

    def run_cycle(self) -> None:
        """One harvest step (until complete) plus one tail sync.

        Never raises: the outcome is written to cloud_sync_state and
        nowhere else. A stop request between chunks returns without
        recording a failure.
        """
        now = self.clock()
        update_sync_state(self.db, last_attempt_at_utc=format_utc(now))
        try:
            state = read_sync_state(self.db)
            sensor_id = self._sensor_id(state)
            if sensor_id is None:
                return
            if not state["harvest_complete"]:
                if not self._harvest(sensor_id, state, now):
                    return
            if not self._tail(sensor_id, now):
                return
        except Exception as exc:
            update_sync_state(
                self.db,
                last_error=public_cloud_error(exc),
                last_error_at_utc=format_utc(self.clock()),
            )
            return
        update_sync_state(
            self.db,
            last_success_at_utc=format_utc(self.clock()),
            last_error=None,
            backoff_until_utc=None,
        )
        self._backoff_until = None

    def _sensor_id(self, state: dict) -> str | None:
        if self.cfg.cloud_sensor_id:
            return self.cfg.cloud_sensor_id
        if state.get("sensor_id"):
            return state["sensor_id"]
        sensor_id = self._call(self.client.get_sensor_id)
        if sensor_id is None:
            return None
        update_sync_state(self.db, sensor_id=sensor_id)
        return sensor_id

    def _harvest_floor(self, state: dict) -> datetime:
        if state.get("harvest_from_utc"):
            return parse_instant(state["harvest_from_utc"])
        if self.cfg.cloud_harvest_from:
            floor = parse_instant(self.cfg.cloud_harvest_from)
        else:
            floor = DEFAULT_HARVEST_FLOOR
        update_sync_state(self.db, harvest_from_utc=format_utc(floor))
        return floor

    def _harvest(self, sensor_id: str, state: dict, now: datetime) -> bool:
        floor = self._harvest_floor(state)
        if state.get("harvest_through_utc"):
            start = parse_instant(state["harvest_through_utc"])
            if start < floor:
                start = floor
        else:
            start = floor
        while start < now:
            # Checked before the request and before the pace sleep, so a
            # shutdown during a multi-year harvest returns on the chunk
            # boundary instead of after the remaining years.
            if self._stop.is_set():
                return False
            end = min(start + CHUNK, now)
            items = self._call(
                lambda start=start, end=end: self.client.get_hourly_stats(
                    sensor_id, start, end
                )
            )
            if items is None:
                return False
            store_cloud_hours(self.db, items, self.clock())
            start = end
            # Persist after the chunk's rows commit: a crash between chunks
            # resumes at this boundary, and the upsert makes the shared
            # boundary hour harmless.
            update_sync_state(self.db, harvest_through_utc=format_utc(start))
        update_sync_state(self.db, harvest_complete=1)
        return True

    def _tail(self, sensor_id: str, now: datetime) -> bool:
        start = now - timedelta(hours=self.cfg.cloud_tail_hours)
        while start < now:
            if self._stop.is_set():
                return False
            end = min(start + CHUNK, now)
            items = self._call(
                lambda start=start, end=end: self.client.get_hourly_stats(
                    sensor_id, start, end
                )
            )
            if items is None:
                return False
            store_cloud_hours(self.db, items, self.clock())
            start = end
        return True

    def _call(self, fn):
        """Call ``fn`` under the pace limit. None means the stop flag is set.

        A 429 sleeps until the published reset and retries the same call.
        Any other CloudError propagates so the cycle records it.
        """
        while not self._stop.is_set():
            if not self._wait_for_turn():
                return None
            try:
                result = fn()
            except CloudError as exc:
                self._last_cloud_request_at = self.clock()
                if exc.status == 429:
                    if not self._backoff(exc):
                        return None
                    continue
                raise
            else:
                self._last_cloud_request_at = self.clock()
                return result
        return None

    def _wait_for_turn(self) -> bool:
        if not self._wait_out_backoff():
            return False
        return self._pace()

    def _pace(self) -> bool:
        """Sleep out the gap that keeps this client under its budget.

        Returns False when the stop flag interrupts the sleep.
        """
        interval = self.cfg.cloud_request_interval_seconds
        last = self._last_cloud_request_at
        if interval <= 0 or last is None:
            return not self._stop.is_set()
        remaining = interval - (self.clock() - last).total_seconds()
        if remaining <= 0:
            return not self._stop.is_set()
        if self._wait(remaining):
            return False
        return not self._stop.is_set()

    def _backoff(self, exc: CloudError) -> bool:
        """Sleep until the rate-limit window resets. False if stopped.

        The reset time is the body's millisecond hint when present, else
        one published window. The next request after a full reset is not
        also held for the steady-state gap: the budget just refilled.
        """
        wait = _backoff_seconds(exc.retry_after_ms)
        # A zero or sub-second reset would retry in a tight loop and burn
        # the shared budget. Hold at least one steady-state gap instead.
        if wait < 1:
            wait = max(1.0, self.cfg.cloud_request_interval_seconds)
        until = self.clock() + timedelta(seconds=wait)
        stamp = format_utc(until)
        self._backoff_until = stamp
        update_sync_state(
            self.db,
            backoff_until_utc=stamp,
            last_error="CloudError: rate_limit_exceeding",
            last_error_at_utc=format_utc(self.clock()),
        )
        if self._wait(wait):
            return False
        self._backoff_until = None
        # The window has reset. Resume this chunk now; the following
        # chunks go back to the steady gap.
        self._last_cloud_request_at = None
        update_sync_state(self.db, backoff_until_utc=None, last_error=None)
        return not self._stop.is_set()

    def _wait_out_backoff(self) -> bool:
        """Honor a backoff persisted by a previous process. False if stopped."""
        if self._backoff_until is _UNSET:
            state = read_sync_state(self.db)
            self._backoff_until = state.get("backoff_until_utc")
        until_text = self._backoff_until
        if not until_text:
            return not self._stop.is_set()
        try:
            remaining = (parse_instant(until_text) - self.clock()).total_seconds()
        except ValueError:
            remaining = 0
        if remaining <= 0:
            self._backoff_until = None
            self._last_cloud_request_at = None
            update_sync_state(self.db, backoff_until_utc=None)
            return not self._stop.is_set()
        if self._wait(remaining):
            return False
        self._backoff_until = None
        self._last_cloud_request_at = None
        update_sync_state(self.db, backoff_until_utc=None, last_error=None)
        return not self._stop.is_set()

    def _wait(self, seconds: float) -> bool:
        """Return True when the stop flag interrupts the wait."""
        if self._stop.is_set():
            return True
        if seconds <= 0:
            return False
        return bool(self._sleeper(seconds))


def _backoff_seconds(retry_after_ms: int | None) -> float:
    if retry_after_ms is None:
        return CLOUD_RATE_WINDOW_SECONDS
    wait = max(0.0, retry_after_ms / 1000.0)
    if wait > _MAX_BACKOFF_SECONDS:
        return CLOUD_RATE_WINDOW_SECONDS
    return wait
