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
"""

from __future__ import annotations

import threading
from datetime import datetime, timedelta, timezone

from .cloud_client import NeurioCloudClient
from .cloud_ingest import read_sync_state, store_cloud_hours, update_sync_state
from .config import Config
from .db import Database
from .timeutil import format_utc, parse_instant, utcnow

CHUNK = timedelta(days=10)
DEFAULT_HARVEST_FLOOR = datetime(2016, 1, 1, tzinfo=timezone.utc)


class CloudSyncer:
    def __init__(self, cfg: Config, db: Database, client=None, clock=None):
        self.cfg = cfg
        self.db = db
        self.client = client or NeurioCloudClient(cfg)
        self.clock = clock or utcnow
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        self._thread = threading.Thread(
            target=self._loop, name="neurio-cloud-sync", daemon=True
        )
        self._thread.start()

    def stop(self, timeout: float = 5) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout)

    def _loop(self) -> None:
        while not self._stop.is_set():
            self.run_cycle()
            self._stop.wait(self.cfg.cloud_sync_interval_seconds)

    def run_cycle(self) -> None:
        """One harvest step (until complete) plus one tail sync.

        Never raises: the outcome is written to cloud_sync_state and
        nowhere else.
        """
        now = self.clock()
        update_sync_state(self.db, last_attempt_at_utc=format_utc(now))
        try:
            state = read_sync_state(self.db)
            sensor_id = self._sensor_id(state)
            if not state["harvest_complete"]:
                self._harvest(sensor_id, state, now)
            self._tail(sensor_id, now)
        except Exception as exc:
            update_sync_state(
                self.db,
                last_error=f"{type(exc).__name__}: {exc}"[:300],
                last_error_at_utc=format_utc(self.clock()),
            )
            return
        update_sync_state(
            self.db,
            last_success_at_utc=format_utc(self.clock()),
            last_error=None,
        )

    def _sensor_id(self, state: dict) -> str:
        if self.cfg.cloud_sensor_id:
            return self.cfg.cloud_sensor_id
        if state.get("sensor_id"):
            return state["sensor_id"]
        sensor_id = self.client.get_sensor_id()
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

    def _harvest(self, sensor_id: str, state: dict, now: datetime) -> None:
        floor = self._harvest_floor(state)
        if state.get("harvest_through_utc"):
            start = parse_instant(state["harvest_through_utc"])
            if start < floor:
                start = floor
        else:
            start = floor
        while start < now:
            end = min(start + CHUNK, now)
            items = self.client.get_hourly_stats(sensor_id, start, end)
            store_cloud_hours(self.db, items, self.clock())
            start = end
            # Persist after the chunk's rows commit: a crash between chunks
            # resumes at this boundary, and the upsert makes the shared
            # boundary hour harmless.
            update_sync_state(self.db, harvest_through_utc=format_utc(start))
        update_sync_state(self.db, harvest_complete=1)

    def _tail(self, sensor_id: str, now: datetime) -> None:
        start = now - timedelta(hours=self.cfg.cloud_tail_hours)
        while start < now:
            end = min(start + CHUNK, now)
            items = self.client.get_hourly_stats(sensor_id, start, end)
            store_cloud_hours(self.db, items, self.clock())
            start = end
