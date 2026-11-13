"""MQTT hourly ingest loop.

Connects to the broker, subscribes to the hourly energy topic, and acknowledges
a record only after it is durable. Connection failures back off exponentially
up to the configured cap, with optional jitter. Parse and storage failures are
not acknowledged, so the bridge keeps the record queued.
"""

from __future__ import annotations

import random
import sqlite3
import threading
import time

from .config import Config
from .db import Database
from .mqtt_client import MqttClient, MqttError, MqttMessage
from .mqtt_ingest import mark_hourly_acked, store_hourly_record
from .timeutil import format_utc, utcnow


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
    def __init__(self, cfg: Config, db: Database, sleeper=None, clock=None, rng=None, mqtt_client_factory=None):
        self.cfg = cfg
        self.db = db
        self.mqtt_client_factory = mqtt_client_factory
        self.sleeper = sleeper or time.sleep
        self.clock = clock or utcnow
        self.rng = rng or random.Random()
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._state_lock = threading.Lock()
        self._failures = 0
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
            "mqtt_hourly_messages": 0,
            "last_hourly_message_at": None,
            "ack_mark_failures": 0,
            "last_ack_mark_error": None,
            "last_ack_mark_error_at": None,
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
        try:
            self._mqtt_loop()
        finally:
            self._update(running=False, in_flight=False)

    def _mqtt_loop(self) -> None:
        """Subscribe to the hourly topic and acknowledge stored records.

        A connection failure backs off and reconnects. A record that fails to
        parse or store is never acknowledged, so the bridge republishes it.
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
                        client.subscribe([(self.cfg.mqtt_hourly_topic, 1)])
                        self._update(in_flight=False)
                    message = client.poll(0.5)
                    if message is None:
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
                    delay = self._failure(now, now, "mqtt_error", error)
                    self._interruptible_sleep(delay)
        finally:
            if client is not None:
                try:
                    client.close()
                except Exception:
                    pass

    def _note_hourly(self, at) -> None:
        with self._state_lock:
            self.state["mqtt_hourly_messages"] += 1
            self.state["last_hourly_message_at"] = format_utc(at)

    def _handle_mqtt_message(self, client, message: MqttMessage) -> None:
        started = self.clock()
        self._update(in_flight=True, last_attempt_started_at=format_utc(started))
        try:
            if message.topic == self.cfg.mqtt_hourly_topic:
                self._note_hourly(started)
                self._handle_mqtt_hourly(client, started, message)
        finally:
            self._update(in_flight=False)

    def _handle_mqtt_hourly(self, client, started, message: MqttMessage) -> None:
        try:
            result = store_hourly_record(self.db, message.payload, self.clock(), self.cfg)
        except Exception as exc:
            if disk_full(exc):
                self._update(storage_writable=False, storage_error="disk_full")
            self._failure(started, self.clock(), "db_error", type(exc).__name__)
            return
        finished = self.clock()
        if result.outcome == "parse_error" or result.record_id is None:
            # No ACK. The bridge keeps the record queued and republishes it.
            self._failure(started, finished, "parse_error", result.error or "parse_error")
            return
        # The record is durable, stored by this delivery or an earlier one.
        # Only now is it safe to acknowledge it.
        client.publish(self.cfg.mqtt_ack_topic, str(result.record_id).encode("ascii"))
        try:
            mark_hourly_acked(self.db, result.record_id, self.clock())
        except Exception as exc:
            # The ACK is already published and the bridge has dropped the
            # record from its queue. A bookkeeping failure here must not
            # tear down the connection (that looped reconnects and left
            # the record unmarked forever) and must not pass silently:
            # it is counted in the poller state, where /api/v1/status
            # exposes it, and the record stays stored with
            # acked_at_utc NULL — visible in the hourly counts as
            # stored-but-unacked.
            self._note_ack_mark_failure(exc)
        outcome = "success" if result.outcome == "stored" else "duplicate"
        self._success(started, self.clock(), outcome)

    def _note_ack_mark_failure(self, exc) -> None:
        with self._state_lock:
            self.state["ack_mark_failures"] += 1
            self.state["last_ack_mark_error"] = type(exc).__name__
            self.state["last_ack_mark_error_at"] = format_utc(self.clock())

    def _success(self, started, finished, outcome) -> None:
        self._failures = 0
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

    def _failure(self, started, finished, outcome, error) -> float:
        self._failures += 1
        delay = self._backoff(self._failures)
        self._update(
            in_flight=False,
            consecutive_failures=self._failures,
            current_backoff_seconds=delay,
            last_attempt_finished_at=format_utc(finished),
            last_outcome=outcome,
            last_error=None if error is None else str(error)[:300],
        )
        return delay

    def _backoff(self, failures: int) -> float:
        base = self.cfg.poll_interval_seconds * (2 ** (failures - 1))
        capped = min(self.cfg.poll_max_backoff_seconds, base)
        if self.cfg.poll_jitter_ratio == 0:
            return capped
        low = max(self.cfg.poll_min_gap_seconds, capped * (1 - self.cfg.poll_jitter_ratio))
        return low + (capped - low) * self.rng.random()

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
