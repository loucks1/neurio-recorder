"""Single-flight poll loop.

The next request starts only after the previous one finishes. Failures back off
exponentially up to the configured cap, with optional jitter. A successful
reading schedules the next poll one interval after the attempt started, and at
least min_gap after it finished, so a slow device is not overlapped or hammered.
"""

from __future__ import annotations

import random
import sqlite3
import threading
import time
from datetime import datetime, timedelta

from .config import Config
from .db import Database
from .fetch import fetch_current_sample
from .ha_fetch import fetch_ha_state, scrub
from .ha_ingest import ingest_ha_state
from .ingest import ingest_body
from .mqtt_client import MqttClient, MqttError, MqttMessage
from .mqtt_ingest import mark_hourly_acked, store_hourly_record
from .retention import apply_retention
from .timeutil import format_utc, utcnow

FRESH_OUTCOMES = {"success", "stored_unapplied", "duplicate"}


def default_mqtt_client(cfg: Config) -> MqttClient:
    return MqttClient(
        host=cfg.mqtt_broker_host or "",
        port=cfg.mqtt_broker_port,
        client_id=cfg.mqtt_client_id,
        keepalive_seconds=cfg.mqtt_keepalive_seconds,
        username=cfg.mqtt_username,
        password=cfg.mqtt_password.reveal() if cfg.mqtt_password is not None else None,
        tls=cfg.mqtt_tls,
        timeout_seconds=cfg.poll_timeout_seconds,
        max_message_bytes=cfg.max_response_bytes,
    )


def disk_full(exc: BaseException) -> bool:
    """True only for SQLite's disk-full error. Other database errors are not this."""
    if not isinstance(exc, sqlite3.OperationalError):
        return False
    text = str(exc).lower()
    return "disk" in text and "full" in text


class Poller:
    def __init__(self, cfg: Config, db: Database, fetcher=None, ha_fetcher=None, sleeper=None, clock=None, rng=None, mqtt_client_factory=None):
        self.cfg = cfg
        self.db = db
        self.mqtt_client_factory = mqtt_client_factory
        self.fetcher = fetcher or (
            lambda url, timeout: fetch_current_sample(url, timeout, cfg.max_response_bytes)
        )
        self.device_fetcher = self.fetcher
        self.ha_fetcher = ha_fetcher or (
            lambda url, timeout: fetch_ha_state(
                cfg.ha_url or "",
                cfg.ha_entity or "",
                cfg.ha_token,
                timeout,
                cfg.max_response_bytes,
                allow_rfc1918_http=cfg.ha_allow_rfc1918_http,
            )
        )
        self.sleeper = sleeper or time.sleep
        self.clock = clock or utcnow
        self.rng = rng or random.Random()
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._state_lock = threading.Lock()
        self._failures = 0
        self._last_retention: datetime | None = None
        self.state = {
            "running": False,
            "in_flight": False,
            "consecutive_failures": 0,
            "current_backoff_seconds": 0.0,
            "last_attempt_started_at": None,
            "last_attempt_finished_at": None,
            "last_success_at": None,
            "last_outcome": None,
            "last_error": None,
            "storage_writable": True,
            "storage_error": None,
        }

    def start(self) -> None:
        self._thread = threading.Thread(target=self._loop, name="neurio-poller", daemon=True)
        self._thread.start()

    def stop(self, timeout: float = 5) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout)

    def snapshot(self) -> dict:
        with self._state_lock:
            return dict(self.state)

    def _loop(self) -> None:
        self._update(running=True)
        if self.cfg.source_mode == "mqtt":
            try:
                self._mqtt_loop()
            finally:
                self._update(running=False, in_flight=False)
            return
        while not self._stop.is_set():
            try:
                delay = self.run_once()
            except Exception:
                delay = self.cfg.poll_max_backoff_seconds
            self._interruptible_sleep(delay)
        self._update(running=False, in_flight=False)

    def _mqtt_loop(self) -> None:
        """Push loop for SOURCE_MODE=mqtt.

        Connects to the broker, subscribes to the bridge topics, and records
        each message as a poll attempt. A connection failure backs off and
        reconnects. Hourly records are acknowledged only after they are
        stored; a record that fails to store is never acknowledged, so the
        bridge keeps it queued and republishes it.
        """
        factory = self.mqtt_client_factory or (lambda: default_mqtt_client(self.cfg))
        client = None
        try:
            while not self._stop.is_set():
                try:
                    if client is None:
                        client = factory()
                        self._update(
                            in_flight=True,
                            last_attempt_started_at=format_utc(self.clock()),
                        )
                        client.connect()
                        client.subscribe(
                            [
                                (self.cfg.mqtt_raw_topic, 0),
                                (self.cfg.mqtt_hourly_topic, 1),
                            ]
                        )
                        self._update(in_flight=False)
                    message = client.poll(0.5)
                    if message is None:
                        self._maybe_retain(self.clock())
                        continue
                    self._handle_mqtt_message(client, message)
                except Exception as exc:
                    if client is not None:
                        try:
                            client.close()
                        except Exception:
                            pass
                        client = None
                    if self._stop.is_set():
                        break
                    error = str(exc) if isinstance(exc, MqttError) else type(exc).__name__
                    now = self.clock()
                    delay = self._failure(now, now, "mqtt_error", None, error)
                    self._interruptible_sleep(delay)
        finally:
            if client is not None:
                try:
                    client.close()
                except Exception:
                    pass

    def _handle_mqtt_message(self, client, message: MqttMessage) -> None:
        started = self.clock()
        self._update(in_flight=True, last_attempt_started_at=format_utc(started))
        try:
            if message.topic == self.cfg.mqtt_raw_topic:
                self._handle_mqtt_raw(started, message)
            elif message.topic == self.cfg.mqtt_hourly_topic:
                self._handle_mqtt_hourly(client, started, message)
            # Subscriptions are exact topics, so a message on any other
            # topic is not ours. Ignore it.
        finally:
            self._update(in_flight=False)

    def _handle_mqtt_raw(self, started, message: MqttMessage) -> None:
        try:
            result = ingest_body(
                self.db,
                message.payload,
                self.clock(),
                self.cfg,
                source_kind="mqtt_bridge",
            )
        except Exception as exc:
            if disk_full(exc):
                self._update(storage_writable=False, storage_error="disk_full")
            self._failure(started, self.clock(), "db_error", None, type(exc).__name__)
            return
        finished = self.clock()
        if result.outcome == "parse_error":
            self._failure(started, finished, "parse_error", None, "parse_error")
            return
        if result.outcome == "duplicate":
            outcome = "duplicate"
        elif result.sequence_status == "unavailable":
            outcome = "unavailable"
        elif result.applied:
            outcome = "success"
        else:
            outcome = "stored_unapplied"
        self._mqtt_success(started, finished, outcome, result.sample_id)
        self._maybe_retain(finished)

    def _handle_mqtt_hourly(self, client, started, message: MqttMessage) -> None:
        try:
            result = store_hourly_record(self.db, message.payload, self.clock(), self.cfg)
        except Exception as exc:
            if disk_full(exc):
                self._update(storage_writable=False, storage_error="disk_full")
            self._failure(started, self.clock(), "db_error", None, type(exc).__name__)
            return
        finished = self.clock()
        if result.outcome == "parse_error" or result.record_id is None:
            # No ACK. The bridge keeps the record queued and republishes it.
            self._failure(
                started, finished, "parse_error", None, result.error or "parse_error"
            )
            return
        # The record is durable, stored by this delivery or an earlier one.
        # Only now is it safe to acknowledge it.
        client.publish(self.cfg.mqtt_ack_topic, str(result.record_id).encode("ascii"))
        mark_hourly_acked(self.db, result.record_id, self.clock())
        outcome = "success" if result.outcome == "stored" else "duplicate"
        self._mqtt_success(started, self.clock(), outcome, None)
        self._maybe_retain(self.clock())

    def _mqtt_success(self, started, finished, outcome, sample_id) -> None:
        self._failures = 0
        self._record(started, finished, outcome, None, None, sample_id, 0.0)
        fields = {
            "in_flight": False,
            "consecutive_failures": 0,
            "current_backoff_seconds": 0.0,
            "last_attempt_finished_at": format_utc(finished),
            "last_outcome": outcome,
            "last_error": None,
            "storage_writable": True,
            "storage_error": None,
        }
        if outcome in {"success", "duplicate"}:
            fields["last_success_at"] = format_utc(finished)
        self._update(**fields)

    def run_once(self) -> float:
        started = self.clock()
        self._update(in_flight=True, last_attempt_started_at=format_utc(started))
        try:
            if self.cfg.source_mode == "homeassistant":
                fetched = self.ha_fetcher(self.cfg.ha_url, self.cfg.poll_timeout_seconds)
            else:
                fetched = self.fetcher(self.cfg.neurio_url, self.cfg.poll_timeout_seconds)
            finished = self.clock()
            if fetched.outcome != "http_ok":
                return self._failure(
                    started,
                    finished,
                    fetched.outcome,
                    fetched.status,
                    scrub(fetched.error or "", self.cfg.ha_token),
                )
            latency_ms = max(0, int((finished - started).total_seconds() * 1000))
            try:
                if self.cfg.source_mode == "homeassistant":
                    result = ingest_ha_state(
                        self.db,
                        fetched.body,
                        finished,
                        self.cfg,
                        http_status=fetched.status,
                        poll_latency_ms=latency_ms,
                    )
                else:
                    result = ingest_body(
                        self.db,
                        fetched.body,
                        finished,
                        self.cfg,
                        http_status=fetched.status,
                        poll_latency_ms=latency_ms,
                    )
            except Exception as exc:
                if disk_full(exc):
                    self._update(storage_writable=False, storage_error="disk_full")
                return self._failure(started, self.clock(), "db_error", fetched.status, type(exc).__name__)
            finished = self.clock()
            if result.outcome == "parse_error":
                return self._failure(started, finished, "parse_error", fetched.status, "parse_error")
            if result.outcome == "duplicate":
                outcome = "duplicate"
            elif result.sequence_status == "unavailable":
                outcome = "unavailable"
            elif result.applied:
                outcome = "success"
            else:
                outcome = "stored_unapplied"
            self._failures = 0
            delay = self._success_delay(started, finished)
            self._record(started, finished, outcome, fetched.status, None, result.sample_id, delay)
            fields = {
                "in_flight": False,
                "consecutive_failures": 0,
                "current_backoff_seconds": delay,
                "last_attempt_finished_at": format_utc(finished),
                "last_outcome": outcome,
                "last_error": None,
            }
            if outcome in {"success", "duplicate"}:
                fields["last_success_at"] = format_utc(finished)
            fields["storage_writable"] = True
            fields["storage_error"] = None
            self._update(**fields)
            self._maybe_retain(finished)
            return delay
        finally:
            self._update(in_flight=False)

    def _failure(self, started, finished, outcome, status, error) -> float:
        self._failures += 1
        delay = self._backoff(self._failures)
        self._record(started, finished, outcome, status, error, None, delay)
        self._update(
            in_flight=False,
            consecutive_failures=self._failures,
            current_backoff_seconds=delay,
            last_attempt_finished_at=format_utc(finished),
            last_outcome=outcome,
            last_error=error,
        )
        return delay

    def _success_delay(self, started, finished) -> float:
        elapsed = (finished - started).total_seconds()
        remain = self.cfg.poll_interval_seconds - elapsed
        if remain < self.cfg.poll_min_gap_seconds:
            return self.cfg.poll_min_gap_seconds
        return remain

    def _backoff(self, failures: int) -> float:
        base = self.cfg.poll_interval_seconds * (2 ** (failures - 1))
        capped = min(self.cfg.poll_max_backoff_seconds, base)
        if self.cfg.poll_jitter_ratio == 0:
            return capped
        low = max(self.cfg.poll_min_gap_seconds, capped * (1 - self.cfg.poll_jitter_ratio))
        return low + (capped - low) * self.rng.random()

    def _record(self, started, finished, outcome, status, error, sample_id, delay) -> None:
        self.db.record_poll(
            format_utc(started),
            format_utc(finished),
            outcome,
            status,
            None if error is None else str(error)[:300],
            sample_id,
            delay,
        )

    def _maybe_retain(self, now: datetime) -> None:
        if self.snapshot().get("storage_writable") is False:
            return
        if self._last_retention is not None and now - self._last_retention < timedelta(hours=6):
            return
        try:
            apply_retention(
                self.db,
                now,
                self.cfg.raw_retention_days,
                self.cfg.poll_log_retention_days,
            )
        except Exception:
            return
        self._last_retention = now

    def _interruptible_sleep(self, seconds: float) -> None:
        end = time.monotonic() + max(0.0, seconds)
        while not self._stop.is_set():
            remain = end - time.monotonic()
            if remain <= 0:
                return
            self.sleeper(min(0.2, remain) if self.sleeper is time.sleep else remain)
            if self.sleeper is not time.sleep:
                return

    def _update(self, **kwargs) -> None:
        with self._state_lock:
            self.state.update(kwargs)
