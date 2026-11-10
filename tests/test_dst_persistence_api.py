import json
import threading
import unittest
import urllib.request
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import support
from neurio_collector.api import dispatch, make_server
from neurio_collector.backup import BackupError, online_backup, restore_backup, verify_backup
from neurio_collector.db import Database
from neurio_collector.energy import build_energy_report
from neurio_collector.fetch import fetch_current_sample
from neurio_collector.ingest import ingest_body
from neurio_collector.locking import InstanceLock
from neurio_collector.observed import SYNTHETIC_SENSOR_ID
from neurio_collector.poller import Poller
from neurio_collector.retention import apply_retention
from neurio_collector.timeutil import hours_for_local_date, load_zone, parse_instant


class DstTests(unittest.TestCase):
    def test_chicago_23_and_25_hour_days(self):
        zone = load_zone("America/Chicago")
        spring = hours_for_local_date(datetime(2026, 3, 8).date(), zone)
        fall = hours_for_local_date(datetime(2026, 11, 1).date(), zone)
        self.assertEqual(len(spring), 23)
        self.assertEqual(sum(item["duration_seconds"] for item in spring), 23 * 3600)
        self.assertNotIn(2, [item["local_hour"] for item in spring])
        jump = next(item for item in spring if item["local_hour"] == 1)
        self.assertEqual(jump["start_local"], "2026-03-08T01:00:00-06:00")
        self.assertEqual(jump["end_local"], "2026-03-08T03:00:00-05:00")
        self.assertEqual(len(fall), 25)
        self.assertEqual(sum(item["duration_seconds"] for item in fall), 25 * 3600)
        repeated = [item for item in fall if item["local_hour"] == 1]
        self.assertEqual(len(repeated), 2)
        self.assertEqual(repeated[0]["utc_offset_start"], "-05:00")
        self.assertEqual(repeated[1]["utc_offset_start"], "-06:00")
        self.assertNotEqual(repeated[0]["start_utc"], repeated[1]["start_utc"])
        with self.assertRaises(ValueError):
            load_zone("-05:00")
        with self.assertRaises(ValueError):
            load_zone("CST")

    def test_repeated_fall_hour_keeps_separate_energy(self):
        db = support.open_db(self)
        cfg = support.make_config(data_dir=db.path.rsplit("/", 1)[0])
        rows = [
            ("2026-11-01T06:10:00Z", "2026-11-01T06:10:00Z", 0),
            ("2026-11-01T06:10:15Z", "2026-11-01T06:10:15Z", 3_600_000),
            ("2026-11-01T07:10:00Z", "2026-11-01T07:10:00Z", 3_600_000),
            ("2026-11-01T07:10:15Z", "2026-11-01T07:10:15Z", 7_200_000),
        ]
        support.feed(
            db,
            cfg,
            [
                (received, device, [support.channel("NET", 3, counter, 0), support.channel("CONSUMPTION", 5, counter, 0)])
                for received, device, counter in rows
            ],
        )
        report = build_energy_report(
            db.conn, cfg, {"date": "2026-11-01", "interval": "hour", "timezone": "America/Chicago"}
        )
        self.assertEqual(report["bucket_count"], 25)
        repeated = [bucket for bucket in report["buckets"] if bucket["local_hour"] == 1]
        self.assertEqual(repeated[0]["household"]["grid_import_ws"], 3_600_000)
        self.assertEqual(repeated[1]["household"]["grid_import_ws"], 3_600_000)
        self.assertEqual(repeated[0]["utc_offset_start"], "-05:00")
        self.assertEqual(repeated[1]["utc_offset_start"], "-06:00")
        day = build_energy_report(
            db.conn, cfg, {"date": "2026-11-01", "interval": "day", "timezone": "America/Chicago"}
        )
        self.assertEqual(day["buckets"][0]["duration_seconds"], 25 * 3600)
        self.assertEqual(day["buckets"][0]["utc_offset_start"], "-05:00")
        self.assertEqual(day["buckets"][0]["utc_offset_end"], "-06:00")


class PersistenceTests(unittest.TestCase):
    def test_restart_keeps_counters_and_ignores_replay(self):
        import os
        import tempfile

        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        path = os.path.join(tmp.name, "neurio.sqlite")
        cfg = support.make_config(data_dir=tmp.name)
        db = Database(path)
        support.feed(
            db,
            cfg,
            [
                ("2026-07-15T17:00:00Z", "2026-07-15T17:00:00Z", [support.channel("NET", 3, 10, 1)]),
                ("2026-07-15T17:00:15Z", "2026-07-15T17:00:15Z", [support.channel("NET", 3, 25, 1)]),
            ],
        )
        db.close()
        reopened = Database(path)
        self.addCleanup(reopened.close)
        support.feed(
            reopened,
            cfg,
            [("2026-07-15T17:00:30Z", "2026-07-15T17:00:30Z", [support.channel("NET", 3, 30, 4)])],
        )
        replay = ingest_body(
            reopened,
            support.sample_bytes(SYNTHETIC_SENSOR_ID, "2026-07-15T17:00:30Z", [support.channel("NET", 3, 30, 4)]),
            parse_instant("2026-07-15T17:00:45Z"),
            cfg,
        )
        self.assertEqual(replay.outcome, "duplicate")
        deltas = [
            row["delta_ws"]
            for row in reopened.conn.execute(
                "SELECT delta_ws FROM energy_spans WHERE direction='import' ORDER BY id"
            ).fetchall()
        ]
        self.assertEqual(deltas, [15, 5])
        samples = reopened.conn.execute("SELECT COUNT(*) AS n FROM samples").fetchone()["n"]
        self.assertEqual(samples, 3)

    def test_backup_restore_and_retention(self):
        import os
        import tempfile

        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        path = os.path.join(tmp.name, "neurio.sqlite")
        backup = os.path.join(tmp.name, "backup.sqlite")
        cfg = support.make_config(data_dir=tmp.name, raw_retention_days=1)
        db = Database(path)
        support.feed(
            db,
            cfg,
            [
                ("2026-07-15T17:00:00Z", "2026-07-15T17:00:00Z", [support.channel("NET", 3, 1, 0)]),
                ("2026-07-15T17:00:15Z", "2026-07-15T17:00:15Z", [support.channel("NET", 3, 6, 0)]),
            ],
        )
        online_backup(path, backup)
        verify_backup(backup)
        support.feed(
            db,
            cfg,
            [("2026-07-15T17:00:30Z", "2026-07-15T17:00:30Z", [support.channel("NET", 3, 9, 0)])],
        )
        self.assertEqual(db.conn.execute("SELECT COUNT(*) AS n FROM samples").fetchone()["n"], 3)
        db.close()
        lock = os.path.join(tmp.name, "collector.lock")
        held = InstanceLock(lock)
        with self.assertRaises(BackupError):
            restore_backup(backup, path, lock)
        held.close()
        restore_backup(backup, path, lock)
        restored = Database(path)
        self.addCleanup(restored.close)
        self.assertEqual(restored.conn.execute("SELECT COUNT(*) AS n FROM samples").fetchone()["n"], 2)
        spans_before = restored.conn.execute("SELECT COUNT(*) AS n FROM energy_spans").fetchone()["n"]
        apply_retention(restored, parse_instant("2026-07-20T00:00:00Z"), 1, 1)
        raw = restored.conn.execute("SELECT raw_json, e_imp_ws FROM samples s JOIN sample_channels sc ON sc.sample_id=s.id").fetchall()
        self.assertTrue(all(row["raw_json"] is None for row in raw))
        self.assertEqual(raw[0]["e_imp_ws"], 1)
        spans_after = restored.conn.execute("SELECT COUNT(*) AS n FROM energy_spans").fetchone()["n"]
        self.assertEqual(spans_before, spans_after)
        safety = path + ".pre-restore"
        self.assertTrue(os.path.exists(safety))
        with self.assertRaises(BackupError):
            restore_backup(backup, path, lock)


class ApiAndPollerTests(unittest.TestCase):
    def test_http_api_pagination_redaction_and_csv(self):
        db = support.open_db(self)
        cfg = support.make_config(data_dir=db.path.rsplit("/", 1)[0], bind_port=0, ephemeral_port=True)
        for index in range(5):
            stamp = f"2026-07-15T17:00:{index:02d}Z"
            ingest_body(
                db,
                support.sample_bytes(
                    SYNTHETIC_SENSOR_ID,
                    stamp,
                    [support.channel("NET", 3, index, 0), support.channel("CONSUMPTION", 5, index, 0)],
                    cts=[{"ct": 1, "p_W": 5, "q_VAR": 1, "v_V": 120}],
                ),
                parse_instant("2026-07-15T17:00:00Z") + timedelta(seconds=index),
                cfg,
            )
        db.record_poll(
            "2026-07-15T17:00:00.000Z",
            "2026-07-15T17:00:00.000Z",
            "success",
            200,
            None,
            1,
            15,
        )
        server = make_server(cfg, db, None)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        self.addCleanup(server.shutdown)
        self.addCleanup(server.server_close)
        port = server.server_address[1]
        base = f"http://127.0.0.1:{port}"

        health = json.loads(urllib.request.urlopen(base + "/healthz", timeout=2).read())
        self.assertTrue(health["process_ok"])
        ready = json.loads(urllib.request.urlopen(base + "/readyz", timeout=2).read())
        self.assertTrue(ready["ready"])
        self.assertFalse(ready["device_fresh"])
        status = json.loads(urllib.request.urlopen(base + "/api/v1/status", timeout=2).read())
        self.assertFalse(status["ha_poll_modified"])
        self.assertFalse(status["rateplantool_contract"])
        self.assertIn("19:31:06", status["observations"]["became_unavailable_chicago"])
        latest = json.loads(urllib.request.urlopen(base + "/api/v1/latest", timeout=2).read())
        encoded = json.dumps(latest)
        self.assertNotIn(SYNTHETIC_SENSOR_ID, encoded)
        self.assertEqual(latest["last_applied_sample"]["channels"][0]["e_imp_ws"], 4)
        page = json.loads(
            urllib.request.urlopen(
                base + "/api/v1/samples?start=2026-07-15T00:00:00Z&end=2026-07-16T00:00:00Z&limit=2",
                timeout=2,
            ).read()
        )
        self.assertEqual(len(page["samples"]), 2)
        self.assertIsNotNone(page["next_cursor"])
        nxt = json.loads(
            urllib.request.urlopen(
                base
                + "/api/v1/samples?start=2026-07-15T00:00:00Z&end=2026-07-16T00:00:00Z&limit=2&cursor="
                + page["next_cursor"],
                timeout=2,
            ).read()
        )
        ids = [row["id"] for row in page["samples"] + nxt["samples"]]
        self.assertEqual(len(ids), len(set(ids)))
        request = urllib.request.Request(base + "/api/v1/latest", method="POST")
        with self.assertRaises(urllib.error.HTTPError) as caught:
            urllib.request.urlopen(request, timeout=2)
        self.assertEqual(caught.exception.code, 405)
        csv_body = urllib.request.urlopen(
            base + "/api/v1/export/general-hourly.csv?date=2026-07-15&timezone=America/Chicago",
            timeout=2,
        ).read().decode()
        self.assertIn("rateplantool_contract", csv_body)
        self.assertIn("false", csv_body)
        self.assertNotIn("grid_import_derived_kwh", csv_body)
        self.assertIn("2026-07-15T01:00:00-05:00", csv_body)
        household = urllib.request.urlopen(
            base + "/api/v1/export/general-household-hourly.csv?date=2026-07-15&timezone=America/Chicago",
            timeout=2,
        ).read().decode()
        self.assertIn("dedicated_channel_eImp_minus_eExp", household)
        self.assertNotIn(SYNTHETIC_SENSOR_ID, household)

    def test_ready_stays_up_when_device_is_stale(self):
        db = support.open_db(self)
        cfg = support.make_config(data_dir=db.path.rsplit("/", 1)[0])
        status, body, _ = dispatch("/readyz", cfg, db, None, now=parse_instant("2026-07-15T17:00:00Z"))
        self.assertEqual(status, 200)
        self.assertTrue(body["ready"])
        self.assertFalse(body["device_fresh"])

    def test_poller_single_flight_backoff_and_recovery(self):
        db = support.open_db(self)
        cfg = support.make_config(data_dir=db.path.rsplit("/", 1)[0], poll_interval_seconds=15)
        calls = {"n": 0, "inflight": 0, "max": 0}
        gate = threading.Event()

        def fetcher(url, timeout):
            calls["inflight"] += 1
            calls["max"] = max(calls["max"], calls["inflight"])
            calls["n"] += 1
            self.assertTrue(gate.wait(2))
            calls["inflight"] -= 1
            from neurio_collector.fetch import FetchResult

            if calls["n"] == 1:
                return FetchResult("timeout", None, b"", "timeout")
            body = support.sample_bytes(
                SYNTHETIC_SENSOR_ID,
                "2026-07-15T17:00:15Z",
                [support.channel("NET", 3, 10 + calls["n"], 0)],
            )
            return FetchResult("http_ok", 200, body, None)

        poller = Poller(cfg, db, fetcher=fetcher)
        poller.start()
        self.addCleanup(poller.stop)
        for _ in range(50):
            if calls["n"] >= 1:
                break
            threading.Event().wait(0.05)
        self.assertEqual(calls["n"], 1)
        threading.Event().wait(0.3)
        self.assertEqual(calls["n"], 1)
        self.assertEqual(calls["max"], 1)
        gate.set()
        poller.stop(timeout=3)
        self.assertGreaterEqual(
            db.conn.execute("SELECT COUNT(*) AS n FROM poll_attempts").fetchone()["n"],
            1,
        )

    def test_poller_backoff_schedule(self):
        db = support.open_db(self)
        cfg = support.make_config(
            data_dir=db.path.rsplit("/", 1)[0],
            poll_interval_seconds=15,
            poll_jitter_ratio=0,
            poll_max_backoff_seconds=300,
        )

        def fetcher(url, timeout):
            from neurio_collector.fetch import FetchResult

            return FetchResult("timeout", None, b"", "timeout")

        poller = Poller(cfg, db, fetcher=fetcher, clock=lambda: parse_instant("2026-07-15T17:00:00Z"))
        delays = [poller.run_once() for _ in range(7)]
        self.assertEqual(delays, [15, 30, 60, 120, 240, 300, 300])
        poller.fetcher = lambda url, timeout: __import__(
            "neurio_collector.fetch", fromlist=["FetchResult"]
        ).FetchResult(
            "http_ok",
            200,
            support.sample_bytes(
                SYNTHETIC_SENSOR_ID,
                "2026-07-15T17:00:00Z",
                [support.channel("NET", 3, 5, 0), support.channel("CONSUMPTION", 5, 5, 0)],
            ),
            None,
        )
        success_delay = poller.run_once()
        self.assertEqual(success_delay, 15)
        self.assertEqual(poller.run_once() and poller._failures, 0)
        again = Poller(cfg, db, fetcher=fetcher, clock=lambda: parse_instant("2026-07-15T18:00:00Z"))
        # A new poller starts its own failure count. The previous poller reset.
        self.assertEqual(poller._failures, 0)
        self.assertEqual(again.run_once(), 15)

    def test_fetch_timeout_auth_and_redirect(self):
        class Handler(BaseHTTPRequestHandler):
            def do_GET(self):
                if self.path == "/slow":
                    threading.Event().wait(1)
                    self.send_response(200)
                    self.end_headers()
                    return
                if self.path == "/auth":
                    self.send_response(401)
                    self.end_headers()
                    return
                if self.path == "/redirect":
                    self.send_response(302)
                    self.send_header("Location", "https://api.neur.io/v1/private")
                    self.end_headers()
                    return
                body = support.sample_bytes(
                    SYNTHETIC_SENSOR_ID,
                    "2026-07-15T17:00:00Z",
                    [support.channel("NET", 3, 1, 0)],
                )
                self.send_response(200)
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def log_message(self, fmt, *args):
                return

        server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        self.addCleanup(server.shutdown)
        self.addCleanup(server.server_close)
        port = server.server_address[1]
        ok = fetch_current_sample(f"http://127.0.0.1:{port}/current-sample", 2, 100000)
        self.assertEqual(ok.outcome, "http_ok")
        slow = fetch_current_sample(f"http://127.0.0.1:{port}/slow", 0.2, 100000)
        self.assertEqual(slow.outcome, "timeout")
        denied = fetch_current_sample(f"http://127.0.0.1:{port}/auth", 2, 100000)
        self.assertEqual(denied.outcome, "http_error")
        self.assertIn("no_credential_retry", denied.error)
        redirected = fetch_current_sample(f"http://127.0.0.1:{port}/redirect", 2, 100000)
        self.assertEqual(redirected.outcome, "http_error")
        self.assertNotIn(b"api.neur.io", redirected.body)


if __name__ == "__main__":
    unittest.main()
