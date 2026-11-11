"""SQLite storage. WAL, one writer, readers via mode=ro.

Schema 3 stores hourly energy, plus the sensor identity /api/v1/channels
still serves. Opening a database migrates it: raw-mode tables are dropped,
space is reclaimed with VACUUM, and schema_version becomes 3.
hourly_records, hourly_backfill_provenance, sensors, and meta are left in
place, including their rows and their CREATE statements.
"""

from __future__ import annotations

import os
import sqlite3
import threading
from contextlib import contextmanager
from urllib.parse import quote

# Child tables first. SQLite does not enforce foreign keys on DROP TABLE;
# this order matches the references anyway. sensors is not in this list:
# /api/v1/channels still reads the sensor id from it.
RAW_TABLES = (
    "sample_channels",
    "sample_cts",
    "energy_spans",
    "counter_epochs",
    "samples",
    "channels",
    "poll_attempts",
)

# hourly_backfill_provenance is not created here. A production database
# already has it; a new database does not until something else creates it.
PRESERVED_TABLES = (
    "meta",
    "sensors",
    "hourly_records",
    "hourly_backfill_provenance",
)

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
CREATE TABLE IF NOT EXISTS hourly_records (
  record_id INTEGER PRIMARY KEY,
  device_timestamp_raw TEXT,
  device_timestamp_utc TEXT,
  retries INTEGER NOT NULL DEFAULT 0,
  received_at_utc TEXT NOT NULL,
  raw_json TEXT NOT NULL,
  net_import_millikwh INTEGER,
  net_export_millikwh INTEGER,
  net_delta_net_millikwh INTEGER,
  generation_delta_export_millikwh INTEGER,
  consumption_delta_import_millikwh INTEGER,
  phase_a_delta_import_millikwh INTEGER,
  phase_b_delta_import_millikwh INTEGER,
  acked_at_utc TEXT
);
"""

SCHEMA_VERSION = "3"
# verify-backup still accepts a pre-migration copy. Startup rewrites the live
# database to SCHEMA_VERSION.
ACCEPTED_SCHEMA_VERSIONS = {"1", "2", "3"}


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
        previous = self.conn.execute(
            "SELECT value FROM meta WHERE key='schema_version'"
        ).fetchone()
        previous_version = None if previous is None else previous["value"]
        dropped = self._drop_raw_tables()
        # VACUUM cannot run inside a transaction. A fresh database has nothing
        # to reclaim. An upgrade, including a retry after a drop that committed
        # and a VACUUM that did not, rewrites the file once.
        if dropped or previous_version != SCHEMA_VERSION:
            self.conn.execute("VACUUM")
        self.conn.execute("PRAGMA user_version=3")
        self.conn.execute(
            "INSERT INTO meta(key, value) VALUES('schema_version', ?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (SCHEMA_VERSION,),
        )

    def _drop_raw_tables(self) -> list[str]:
        existing = {
            row[0]
            for row in self.conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            )
        }
        dropped = []
        for name in RAW_TABLES:
            if name not in existing:
                continue
            self.conn.execute(f'DROP TABLE "{name}"')
            dropped.append(name)
        return dropped

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
