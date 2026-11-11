"""Schema 3 drops raw-mode tables and leaves hourly data alone."""

import json
import os
import sqlite3
import tempfile
import unittest

import support
from neurio_collector.api import dispatch
from neurio_collector.db import PRESERVED_TABLES, RAW_TABLES, SCHEMA_VERSION, Database


LEGACY = """
CREATE TABLE meta (
  key TEXT PRIMARY KEY,
  value TEXT NOT NULL
);
CREATE TABLE sensors (
  id INTEGER PRIMARY KEY,
  sensor_id TEXT NOT NULL UNIQUE,
  sensor_token TEXT NOT NULL UNIQUE,
  first_seen_utc TEXT NOT NULL,
  last_seen_utc TEXT NOT NULL
);
CREATE TABLE channels (
  id INTEGER PRIMARY KEY,
  sensor_pk INTEGER NOT NULL REFERENCES sensors(id),
  identity_key TEXT NOT NULL
);
CREATE TABLE samples (
  id INTEGER PRIMARY KEY,
  sensor_pk INTEGER NOT NULL REFERENCES sensors(id),
  raw_json TEXT
);
CREATE TABLE sample_channels (
  id INTEGER PRIMARY KEY,
  sample_id INTEGER NOT NULL REFERENCES samples(id),
  channel_pk INTEGER NOT NULL REFERENCES channels(id)
);
CREATE TABLE sample_cts (
  id INTEGER PRIMARY KEY,
  sample_id INTEGER NOT NULL REFERENCES samples(id)
);
CREATE TABLE counter_epochs (
  id INTEGER PRIMARY KEY,
  channel_pk INTEGER NOT NULL REFERENCES channels(id)
);
CREATE TABLE energy_spans (
  id INTEGER PRIMARY KEY,
  channel_pk INTEGER NOT NULL REFERENCES channels(id),
  epoch_id INTEGER NOT NULL REFERENCES counter_epochs(id),
  start_sample_id INTEGER NOT NULL REFERENCES samples(id),
  delta_ws INTEGER NOT NULL CHECK (delta_ws >= 0)
);
CREATE TABLE poll_attempts (
  id INTEGER PRIMARY KEY,
  outcome TEXT NOT NULL
);
CREATE TABLE hourly_records (
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
CREATE TABLE hourly_backfill_provenance (
  record_id INTEGER PRIMARY KEY,
  backfill_source TEXT NOT NULL,
  detail TEXT NOT NULL
);
CREATE TABLE kept_notes (
  id INTEGER PRIMARY KEY,
  body TEXT NOT NULL
);
"""


def _tables(conn):
    return {
        row[0]
        for row in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
        )
    }


def _sql(conn, name):
    row = conn.execute(
        "SELECT sql FROM sqlite_master WHERE type='table' AND name=?", (name,)
    ).fetchone()
    return None if row is None else row[0]


def _rows(conn, sql):
    return [tuple(row) for row in conn.execute(sql).fetchall()]


class MigrationTests(unittest.TestCase):
    def test_raw_table_list_matches_the_live_raw_schema(self):
        self.assertEqual(
            set(RAW_TABLES),
            {
                "sample_channels",
                "sample_cts",
                "energy_spans",
                "counter_epochs",
                "samples",
                "channels",
                "poll_attempts",
            },
        )
        self.assertEqual(
            PRESERVED_TABLES,
            ("meta", "sensors", "hourly_records", "hourly_backfill_provenance"),
        )
        for name in PRESERVED_TABLES:
            self.assertNotIn(name, RAW_TABLES)

    def test_migration_drops_raw_tables_and_keeps_hourly_rows(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        path = os.path.join(tmp.name, "neurio.sqlite")
        conn = sqlite3.connect(path)
        conn.executescript(LEGACY)
        conn.execute("PRAGMA user_version=2")
        conn.execute(
            "INSERT INTO meta(key, value) VALUES('schema_version', '2'), ('note', 'keep-me')"
        )
        conn.execute(
            "INSERT INTO sensors(id, sensor_id, sensor_token, first_seen_utc, last_seen_utc) "
            "VALUES(1, 'ha-relay:sensor.neurio_raw', 'abc123token', "
            "'2026-11-01T00:00:00.000Z', '2026-11-10T00:00:00.000Z')"
        )
        conn.execute(
            "INSERT INTO channels(id, sensor_pk, identity_key) VALUES(1, 1, 'net')"
        )
        blob = "x" * 200_000
        conn.execute(
            "INSERT INTO samples(id, sensor_pk, raw_json) VALUES(1, 1, ?)", (blob,)
        )
        conn.execute(
            "INSERT INTO sample_channels(id, sample_id, channel_pk) VALUES(1, 1, 1)"
        )
        conn.execute("INSERT INTO sample_cts(id, sample_id) VALUES(1, 1)")
        conn.execute("INSERT INTO counter_epochs(id, channel_pk) VALUES(1, 1)")
        conn.execute(
            "INSERT INTO energy_spans(id, channel_pk, epoch_id, start_sample_id, delta_ws) "
            "VALUES(1, 1, 1, 1, 15)"
        )
        conn.execute("INSERT INTO poll_attempts(id, outcome) VALUES(1, 'success')")
        hourly = (
            1700000000,
            "2026-11-06T17:00:00Z",
            "2026-11-06T17:00:00.000Z",
            2,
            "2026-11-06T18:00:05.000Z",
            '{"id":1700000000}',
            10,
            20,
            30,
            40,
            50,
            60,
            70,
            "2026-11-06T18:00:06.000Z",
        )
        conn.execute(
            "INSERT INTO hourly_records VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)", hourly
        )
        provenance = (1700000000, "backfill-script", "hour was imported")
        conn.execute(
            "INSERT INTO hourly_backfill_provenance VALUES(?,?,?)", provenance
        )
        conn.execute("INSERT INTO kept_notes(id, body) VALUES(1, 'leave this')")
        before_pages = conn.execute("PRAGMA page_count").fetchone()[0]
        hourly_sql = _sql(conn, "hourly_records")
        provenance_sql = _sql(conn, "hourly_backfill_provenance")
        sensor_sql = _sql(conn, "sensors")
        notes_sql = _sql(conn, "kept_notes")
        conn.commit()
        conn.close()
        self.assertGreater(before_pages, 10)

        db = Database(path)
        try:
            names = _tables(db.conn)
            for raw in RAW_TABLES:
                self.assertNotIn(raw, names)
            for kept in (
                "meta",
                "sensors",
                "hourly_records",
                "hourly_backfill_provenance",
                "kept_notes",
            ):
                self.assertIn(kept, names)
            self.assertEqual(_rows(db.conn, "SELECT * FROM hourly_records"), [hourly])
            self.assertEqual(
                _rows(db.conn, "SELECT * FROM hourly_backfill_provenance"), [provenance]
            )
            self.assertEqual(
                _rows(db.conn, "SELECT * FROM sensors"),
                [(
                    1,
                    "ha-relay:sensor.neurio_raw",
                    "abc123token",
                    "2026-11-01T00:00:00.000Z",
                    "2026-11-10T00:00:00.000Z",
                )],
            )
            self.assertEqual(_rows(db.conn, "SELECT * FROM kept_notes"), [(1, "leave this")])
            self.assertEqual(_sql(db.conn, "hourly_records"), hourly_sql)
            self.assertEqual(_sql(db.conn, "hourly_backfill_provenance"), provenance_sql)
            self.assertEqual(_sql(db.conn, "sensors"), sensor_sql)
            self.assertEqual(_sql(db.conn, "kept_notes"), notes_sql)
            self.assertEqual(
                db.conn.execute(
                    "SELECT value FROM meta WHERE key='schema_version'"
                ).fetchone()["value"],
                SCHEMA_VERSION,
            )
            self.assertEqual(
                db.conn.execute("SELECT value FROM meta WHERE key='note'").fetchone()["value"],
                "keep-me",
            )
            self.assertEqual(db.conn.execute("PRAGMA user_version").fetchone()[0], 3)
            after_pages = db.conn.execute("PRAGMA page_count").fetchone()[0]
            self.assertLess(after_pages, before_pages)
            cfg = support.make_config()
            status, body, _ = dispatch("/api/v1/channels", cfg, db, None)
            self.assertEqual(status, 200)
            self.assertEqual(body["channels"][0]["sensor_id"], "ha-relay:sensor.neurio_raw")
            self.assertNotIn("sensor_token", body["channels"][0])
            self.assertNotIn("abc123token", json.dumps(body))
        finally:
            db.close()

        again = Database(path)
        self.addCleanup(again.close)
        self.assertEqual(_rows(again.conn, "SELECT * FROM hourly_records"), [hourly])
        self.assertEqual(
            _rows(again.conn, "SELECT * FROM hourly_backfill_provenance"), [provenance]
        )
        self.assertEqual(_sql(again.conn, "hourly_records"), hourly_sql)
        self.assertEqual(_sql(again.conn, "hourly_backfill_provenance"), provenance_sql)
        for raw in RAW_TABLES:
            self.assertNotIn(raw, _tables(again.conn))
        self.assertEqual(
            again.conn.execute(
                "SELECT value FROM meta WHERE key='schema_version'"
            ).fetchone()["value"],
            "3",
        )

    def test_fresh_database_has_no_raw_tables(self):
        db = support.open_db(self)
        names = _tables(db.conn)
        self.assertEqual(names, {"meta", "sensors", "hourly_records"})
        self.assertEqual(
            db.conn.execute("SELECT value FROM meta WHERE key='schema_version'").fetchone()["value"],
            "3",
        )
        self.assertEqual(db.conn.execute("PRAGMA user_version").fetchone()[0], 3)


if __name__ == "__main__":
    unittest.main()
