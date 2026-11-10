"""SQLite storage. WAL, one writer, readers via mode=ro."""

from __future__ import annotations

import os
import sqlite3
import threading
from contextlib import contextmanager
from urllib.parse import quote

SCHEMA = """
CREATE TABLE IF NOT EXISTS meta (
  key TEXT PRIMARY KEY,
  value TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS sensors (
  id INTEGER PRIMARY KEY,
  sensor_id TEXT NOT NULL UNIQUE,
  sensor_token TEXT NOT NULL UNIQUE,
  first_seen_utc TEXT NOT NULL,
  last_seen_utc TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS channels (
  id INTEGER PRIMARY KEY,
  sensor_pk INTEGER NOT NULL REFERENCES sensors(id),
  ch INTEGER,
  type TEXT NOT NULL,
  label TEXT,
  identity_key TEXT NOT NULL,
  first_seen_utc TEXT NOT NULL,
  last_seen_utc TEXT NOT NULL,
  UNIQUE(sensor_pk, identity_key)
);
CREATE TABLE IF NOT EXISTS samples (
  id INTEGER PRIMARY KEY,
  sensor_pk INTEGER NOT NULL REFERENCES sensors(id),
  received_at_utc TEXT NOT NULL,
  device_timestamp_raw TEXT,
  device_timestamp_utc TEXT,
  clock_quality TEXT NOT NULL,
  sequence_status TEXT NOT NULL,
  payload_sha256 TEXT NOT NULL,
  raw_json TEXT,
  raw_retained INTEGER NOT NULL DEFAULT 1,
  http_status INTEGER,
  poll_latency_ms INTEGER,
  applied_to_counters INTEGER NOT NULL DEFAULT 0,
  flags_json TEXT NOT NULL DEFAULT '[]',
  source_kind TEXT NOT NULL DEFAULT 'device',
  ha_entity_id TEXT,
  ha_state TEXT,
  ha_last_changed_utc TEXT,
  ha_last_updated_utc TEXT,
  ha_last_reported_utc TEXT
);
CREATE TABLE IF NOT EXISTS sample_channels (
  id INTEGER PRIMARY KEY,
  sample_id INTEGER NOT NULL REFERENCES samples(id),
  channel_pk INTEGER NOT NULL REFERENCES channels(id),
  p_w REAL,
  q_var REAL,
  v_v REAL,
  e_imp_ws INTEGER,
  e_exp_ws INTEGER,
  field_quality TEXT NOT NULL,
  flags_json TEXT NOT NULL DEFAULT '[]',
  UNIQUE(sample_id, channel_pk)
);
CREATE TABLE IF NOT EXISTS sample_cts (
  id INTEGER PRIMARY KEY,
  sample_id INTEGER NOT NULL REFERENCES samples(id),
  ct INTEGER,
  p_w REAL,
  q_var REAL,
  v_v REAL
);
CREATE TABLE IF NOT EXISTS counter_epochs (
  id INTEGER PRIMARY KEY,
  channel_pk INTEGER NOT NULL REFERENCES channels(id),
  direction TEXT NOT NULL CHECK (direction IN ('import','export')),
  epoch_number INTEGER NOT NULL,
  started_received_utc TEXT NOT NULL,
  started_device_utc TEXT,
  start_counter_ws INTEGER NOT NULL,
  start_sample_id INTEGER NOT NULL,
  last_counter_ws INTEGER NOT NULL,
  last_sample_id INTEGER NOT NULL,
  last_received_utc TEXT NOT NULL,
  last_device_utc TEXT,
  ended_received_utc TEXT,
  end_reason TEXT,
  UNIQUE(channel_pk, direction, epoch_number)
);
CREATE TABLE IF NOT EXISTS energy_spans (
  id INTEGER PRIMARY KEY,
  channel_pk INTEGER NOT NULL REFERENCES channels(id),
  direction TEXT NOT NULL CHECK (direction IN ('import','export')),
  epoch_id INTEGER NOT NULL REFERENCES counter_epochs(id),
  start_sample_id INTEGER NOT NULL REFERENCES samples(id),
  end_sample_id INTEGER NOT NULL REFERENCES samples(id),
  start_received_utc TEXT NOT NULL,
  end_received_utc TEXT NOT NULL,
  start_device_utc TEXT,
  end_device_utc TEXT,
  start_counter_ws INTEGER NOT NULL,
  end_counter_ws INTEGER NOT NULL,
  delta_ws INTEGER NOT NULL CHECK (delta_ws >= 0),
  gap_seconds REAL,
  time_basis TEXT NOT NULL,
  allocation_status TEXT NOT NULL,
  quality_json TEXT NOT NULL,
  source_method TEXT NOT NULL,
  placement_start_utc TEXT,
  placement_end_utc TEXT,
  UNIQUE(channel_pk, direction, end_sample_id)
);
CREATE TABLE IF NOT EXISTS poll_attempts (
  id INTEGER PRIMARY KEY,
  started_at_utc TEXT NOT NULL,
  finished_at_utc TEXT,
  outcome TEXT NOT NULL,
  http_status INTEGER,
  error TEXT,
  sample_id INTEGER,
  backoff_seconds REAL
);
CREATE INDEX IF NOT EXISTS idx_samples_sensor ON samples(sensor_pk, id);
CREATE INDEX IF NOT EXISTS idx_samples_hash ON samples(sensor_pk, payload_sha256);
CREATE INDEX IF NOT EXISTS idx_samples_received ON samples(received_at_utc);
CREATE INDEX IF NOT EXISTS idx_spans_channel ON energy_spans(channel_pk, end_received_utc);
CREATE INDEX IF NOT EXISTS idx_poll_id ON poll_attempts(id);
"""

SCHEMA_VERSION = "2"
ACCEPTED_SCHEMA_VERSIONS = {"1", "2"}


def connect_rw(path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(path, check_same_thread=False, isolation_level=None, timeout=5)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=FULL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA busy_timeout=5000")
    return conn


def connect_ro(path: str) -> sqlite3.Connection:
    uri = "file:" + quote(path) + "?mode=ro"
    conn = sqlite3.connect(uri, uri=True, check_same_thread=False, timeout=5)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA query_only=ON")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA busy_timeout=5000")
    return conn


class Database:
    def __init__(self, path: str):
        parent = os.path.dirname(path)
        if parent:
            os.makedirs(parent, exist_ok=True)
        self.path = path
        self.write_lock = threading.Lock()
        self.conn = connect_rw(path)
        self._migrate()

    def _migrate(self) -> None:
        self.conn.executescript(SCHEMA)
        self._add_column("samples", "source_kind", "TEXT NOT NULL DEFAULT 'device'")
        self._add_column("samples", "ha_entity_id", "TEXT")
        self._add_column("samples", "ha_state", "TEXT")
        self._add_column("samples", "ha_last_changed_utc", "TEXT")
        self._add_column("samples", "ha_last_updated_utc", "TEXT")
        self._add_column("samples", "ha_last_reported_utc", "TEXT")
        self._add_column("energy_spans", "placement_start_utc", "TEXT")
        self._add_column("energy_spans", "placement_end_utc", "TEXT")
        self.conn.execute("PRAGMA user_version=2")
        self.conn.execute(
            "INSERT INTO meta(key, value) VALUES('schema_version', ?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (SCHEMA_VERSION,),
        )

    def _add_column(self, table: str, column: str, decl: str) -> None:
        present = {row[1] for row in self.conn.execute(f"PRAGMA table_info({table})")}
        if column not in present:
            self.conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {decl}")

    def close(self) -> None:
        with self.write_lock:
            try:
                self.conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
            finally:
                self.conn.close()

    @contextmanager
    def transaction(self):
        self.conn.execute("BEGIN IMMEDIATE")
        try:
            yield
        except Exception:
            self.conn.execute("ROLLBACK")
            raise
        else:
            self.conn.execute("COMMIT")

    def reader(self) -> sqlite3.Connection:
        return connect_ro(self.path)

    def record_poll(
        self,
        started_at: str,
        finished_at: str,
        outcome: str,
        http_status: int | None,
        error: str | None,
        sample_id: int | None,
        backoff_seconds: float | None,
    ) -> None:
        with self.write_lock:
            with self.transaction():
                self.conn.execute(
                    "INSERT INTO poll_attempts(started_at_utc, finished_at_utc, outcome, "
                    "http_status, error, sample_id, backoff_seconds) VALUES(?,?,?,?,?,?,?)",
                    (
                        started_at,
                        finished_at,
                        outcome,
                        http_status,
                        error,
                        sample_id,
                        backoff_seconds,
                    ),
                )
