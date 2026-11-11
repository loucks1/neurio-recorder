"""Backup and restore keep hourly rows and still accept a pre-migration copy."""

import os
import sqlite3
import tempfile
import unittest

import support
from neurio_collector.backup import BackupError, online_backup, restore_backup, verify_backup
from neurio_collector.db import Database
from neurio_collector.locking import InstanceLock
from neurio_collector.mqtt_ingest import store_hourly_record
from neurio_collector.timeutil import parse_instant


def _hourly(record_id: int) -> bytes:
    return (
        '{"id": %d, "timestamp": "2026-11-06T17:00:00Z", "retries": 0, '
        '"net": {"delta_net_kwh": 0.5}}' % record_id
    ).encode()


class BackupTests(unittest.TestCase):
    def test_backup_restore_round_trip(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        path = os.path.join(tmp.name, "neurio.sqlite")
        backup = os.path.join(tmp.name, "backup.sqlite")
        received = parse_instant("2026-11-06T18:00:05Z")
        db = Database(path)
        cfg = support.make_config(data_dir=tmp.name)
        store_hourly_record(db, _hourly(1700000000), received, cfg)
        store_hourly_record(db, _hourly(1700003600), received, cfg)
        online_backup(path, backup)
        verify_backup(backup)
        store_hourly_record(db, _hourly(1700007200), received, cfg)
        self.assertEqual(db.conn.execute("SELECT COUNT(*) AS n FROM hourly_records").fetchone()["n"], 3)
        db.close()
        lock = os.path.join(tmp.name, "collector.lock")
        held = InstanceLock(lock)
        with self.assertRaises(BackupError):
            restore_backup(backup, path, lock)
        held.close()
        restore_backup(backup, path, lock)
        restored = Database(path)
        self.addCleanup(restored.close)
        ids = [
            row["record_id"]
            for row in restored.conn.execute(
                "SELECT record_id FROM hourly_records ORDER BY record_id"
            )
        ]
        self.assertEqual(ids, [1700000000, 1700003600])
        self.assertTrue(os.path.exists(path + ".pre-restore"))
        with self.assertRaises(BackupError):
            restore_backup(backup, path, lock)

    def test_verify_accepts_a_schema_2_backup_without_migrating_it(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        path = os.path.join(tmp.name, "old.sqlite")
        conn = sqlite3.connect(path)
        conn.execute("CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT NOT NULL)")
        conn.execute("INSERT INTO meta(key, value) VALUES('schema_version', '2')")
        conn.execute("CREATE TABLE samples (id INTEGER PRIMARY KEY, raw_json TEXT)")
        conn.execute("INSERT INTO samples(id, raw_json) VALUES(1, 'kept-in-the-backup')")
        conn.commit()
        conn.close()
        verify_backup(path)
        check = sqlite3.connect(path)
        self.assertEqual(check.execute("SELECT COUNT(*) FROM samples").fetchone()[0], 1)
        self.assertEqual(
            check.execute("SELECT value FROM meta WHERE key='schema_version'").fetchone()[0],
            "2",
        )
        check.close()


if __name__ == "__main__":
    unittest.main()
