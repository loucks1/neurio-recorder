"""Raw retention disable and disk-full behavior. No size estimates."""

import json
import sqlite3
import unittest

import support
import test_ha_ingest
from neurio_collector.api import dispatch
from neurio_collector.config import ConfigError
from neurio_collector.poller import Poller, disk_full
from neurio_collector.retention import apply_retention
from neurio_collector.timeutil import parse_instant


class RetentionTests(unittest.TestCase):
    def test_zero_keeps_raw_json_and_counters(self):
        db = support.open_db(self)
        cfg = support.make_config(data_dir=db.path.rsplit("/", 1)[0], raw_retention_days=0)
        self.assertEqual(cfg.raw_retention_days, 0)
        body = support.sample_bytes(
            "SYNTHETIC-NOT-A-DEVICE",
            "2026-01-01T00:00:00Z",
            [support.channel("NET", 3, 3600000, 0), support.channel("CONSUMPTION", 5, 3600000, 0)],
        )
        from neurio_collector.ingest import ingest_body

        ingest_body(db, body, parse_instant("2026-01-01T00:00:00Z"), cfg)
        cleared, spans = apply_retention(db, parse_instant("2026-08-01T00:00:00Z"), 0, 30)
        self.assertEqual(cleared, 0)
        row = db.conn.execute(
            "SELECT raw_json, e_imp_ws FROM samples s JOIN sample_channels sc ON sc.sample_id=s.id"
        ).fetchone()
        self.assertIsNotNone(row["raw_json"])
        self.assertIn("eImp_Ws", row["raw_json"])
        self.assertEqual(row["e_imp_ws"], 3600000)
        with self.assertRaises(ConfigError):
            support.make_config(raw_retention_days=-1)

    def test_positive_raw_days_nulls_only_the_payload(self):
        db = support.open_db(self)
        cfg = support.make_config(data_dir=db.path.rsplit("/", 1)[0])
        body = support.sample_bytes(
            "SYNTHETIC-NOT-A-DEVICE",
            "2026-01-01T00:00:00Z",
            [support.channel("NET", 3, 42, 0)],
        )
        from neurio_collector.ingest import ingest_body

        ingest_body(db, body, parse_instant("2026-01-01T00:00:00Z"), cfg)
        apply_retention(db, parse_instant("2026-08-01T00:00:00Z"), 14, 30)
        row = db.conn.execute(
            "SELECT raw_json, raw_retained, e_imp_ws FROM samples s "
            "JOIN sample_channels sc ON sc.sample_id=s.id"
        ).fetchone()
        self.assertIsNone(row["raw_json"])
        self.assertEqual(row["raw_retained"], 0)
        self.assertEqual(row["e_imp_ws"], 42)
        self.assertGreaterEqual(
            db.conn.execute("SELECT COUNT(*) AS n FROM energy_spans").fetchone()["n"], 0
        )

    def test_disk_full_is_unready_and_does_not_delete(self):
        self.assertTrue(disk_full(sqlite3.OperationalError("database or disk is full")))
        self.assertFalse(disk_full(sqlite3.OperationalError("database is locked")))
        db = support.open_db(self)
        cfg = test_ha_ingest.ha_config(data_dir=db.path.rsplit("/", 1)[0], raw_retention_days=0)
        body = test_ha_ingest.ha_body(
            "OK", "2026-11-07T02:00:00Z", "2026-11-07T02:00:01Z", test_ha_ingest.five(100)
        )

        def ha_fetcher(url, timeout):
            from neurio_collector.fetch import FetchResult

            return FetchResult("http_ok", 200, body, None)

        poller = Poller(cfg, db, ha_fetcher=ha_fetcher, clock=lambda: parse_instant("2026-11-07T02:00:02Z"))

        def explode(*args, **kwargs):
            raise sqlite3.OperationalError("database or disk is full")

        import neurio_collector.poller as poller_mod

        original = poller_mod.ingest_ha_state
        poller_mod.ingest_ha_state = explode
        self.addCleanup(lambda: setattr(poller_mod, "ingest_ha_state", original))
        poller.run_once()
        self.assertFalse(poller.snapshot()["storage_writable"])
        self.assertEqual(poller.snapshot()["storage_error"], "disk_full")
        self.assertEqual(db.conn.execute("SELECT COUNT(*) AS n FROM samples").fetchone()["n"], 0)
        status, payload, _ = dispatch("/readyz", cfg, db, poller, now=parse_instant("2026-11-07T02:00:03Z"))
        self.assertEqual(status, 503)
        self.assertFalse(payload["storage_writable"])
        self.assertNotIn(test_ha_ingest.TOKEN, json.dumps(payload))


if __name__ == "__main__":
    unittest.main()
