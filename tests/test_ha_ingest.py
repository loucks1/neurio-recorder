"""Synthetic Home Assistant state responses. No token is a real credential. No Neurio call."""

import json
import ssl
import threading
import unittest
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import support
from neurio_collector.config import Config, ConfigError, Secret
from neurio_collector.ha_fetch import fetch_ha_state, ha_opener, tls_context
from neurio_collector.ha_ingest import ingest_ha_state
from neurio_collector.poller import Poller
from neurio_collector.timeutil import parse_instant

TOKEN = "synthetic-ha-token-not-a-secret"
ENTITY = "sensor.neurio_raw"


def ha_config(**overrides):
    data = dict(
        source_mode="homeassistant",
        neurio_url="",
        ha_url="http://127.0.0.1",
        ha_entity=ENTITY,
        ha_source_id="ha-relay:sensor.neurio_raw",
        ha_token=Secret(TOKEN),
    )
    data.update(overrides)
    return support.make_config(**data)


def ha_body(state, updated, reported, channels, changed=None):
    return json.dumps(
        {
            "entity_id": ENTITY,
            "state": state,
            "attributes": {"friendly_name": "neurio raw", "channels": channels},
            "last_changed": changed or updated,
            "last_updated": updated,
            "last_reported": reported,
        }
    ).encode()


def five(imp):
    return [
        support.channel("PHASE_A_CONSUMPTION", 1, imp, 0, 586),
        support.channel("PHASE_B_CONSUMPTION", 2, imp, 0, 153),
        support.channel("NET", 3, imp, 0, 738),
        support.channel("GENERATION", 4, 0, 0, 0),
        support.channel("CONSUMPTION", 5, imp, 0, 739),
    ]


class HaConfigTests(unittest.TestCase):
    def test_mode_is_explicit_and_token_is_redacted(self):
        with self.assertRaises(ConfigError) as caught:
            Config.from_env({})
        self.assertIn("SOURCE_MODE", str(caught.exception))
        env = {
            "SOURCE_MODE": "homeassistant",
            "BIND_ADDRESS": "127.0.0.1",
            "BIND_PORT": "8095",
            "DATA_DIR": "/tmp/neurio-ha-test",
            "HA_URL": "https://ha.example.test",
            "HA_ENTITY": ENTITY,
            "HA_TOKEN": TOKEN,
            "NEURIO_URL": "http://192.168.1.50/current-sample",
        }
        with self.assertRaises(ConfigError) as caught:
            Config.from_env(env)
        self.assertNotIn(TOKEN, str(caught.exception))
        self.assertIn("no fallback", str(caught.exception))
        env.pop("NEURIO_URL")
        cfg = Config.from_env(env)
        self.assertEqual(cfg.source_mode, "homeassistant")
        self.assertEqual(cfg.ha_source_id, "ha-relay:sensor.neurio_raw")
        self.assertNotIn(TOKEN, repr(cfg))
        self.assertEqual(str(cfg.ha_token), "<redacted>")
        with self.assertRaises(ConfigError):
            Config.from_env({**env, "HA_URL": "http://192.168.1.10"})
        with self.assertRaises(ConfigError):
            Config.from_env({**env, "HA_TOKEN": "", "HA_TOKEN_FILE": ""})

    def test_rfc1918_http_requires_the_exact_opt_in(self):
        base = {
            "SOURCE_MODE": "homeassistant",
            "BIND_ADDRESS": "127.0.0.1",
            "BIND_PORT": "8095",
            "DATA_DIR": "/tmp/neurio-ha-test",
            "HA_ENTITY": ENTITY,
            "HA_TOKEN": TOKEN,
            "NEURIO_RAW_RETENTION_DAYS": "0",
            "HA_URL": "http://192.168.1.10:8123",
        }
        with self.assertRaises(ConfigError) as caught:
            Config.from_env(base)
        self.assertNotIn(TOKEN, str(caught.exception))
        self.assertIn("accept-unencrypted-bearer", str(caught.exception))
        with self.assertRaises(ConfigError):
            Config.from_env({**base, "HA_ALLOW_RFC1918_HTTP": "true"})
        cfg = Config.from_env({**base, "HA_ALLOW_RFC1918_HTTP": "accept-unencrypted-bearer"})
        self.assertEqual(cfg.ha_url, "http://192.168.1.10:8123")
        self.assertEqual(cfg.source_mode, "homeassistant")
        self.assertEqual(cfg.raw_retention_days, 0)
        self.assertEqual(cfg.neurio_url, "")
        self.assertNotIn(TOKEN, repr(cfg))
        for host in (
            "http://8.8.8.8:8123",
            "http://169.254.169.254:8123",
            "http://169.254.1.1:8123",
            "http://100.64.0.1:8123",
            "http://172.15.0.1:8123",
            "http://172.32.0.1:8123",
            "http://ha.example.com:8123",
            "http://localhost:8123",
            "http://192.168.001.010:8123",
            "http://192.168.1.10:8123/api",
            "http://192.168.1.10:8123?token=1",
            "http://192.168.1.10:8123#frag",
            "http://user:tok@192.168.1.10:8123",
            "http://[fc00::1]:8123",
        ):
            with self.assertRaises(ConfigError) as caught:
                Config.from_env({**base, "HA_URL": host, "HA_ALLOW_RFC1918_HTTP": "accept-unencrypted-bearer"})
            self.assertNotIn(TOKEN, str(caught.exception))
            self.assertNotIn("tok", str(caught.exception))
        for host in ("http://10.1.2.3:8123", "http://172.16.0.1:8123", "http://172.31.255.255:8123"):
            allowed = Config.from_env(
                {**base, "HA_URL": host, "HA_ALLOW_RFC1918_HTTP": "accept-unencrypted-bearer"}
            )
            self.assertEqual(allowed.ha_url, host)
        https = Config.from_env({**base, "HA_URL": "https://ha.example.test"})
        self.assertTrue(https.ha_url.startswith("https://"))

    def test_tls_verification_stays_on(self):
        context = tls_context()
        self.assertEqual(context.verify_mode, ssl.CERT_REQUIRED)
        self.assertTrue(context.check_hostname)


class HaIngestTests(unittest.TestCase):
    def test_channels_counters_and_unknown_device_time(self):
        observed = json.loads(
            (support.OBSERVED / "ha-recorder-neurio-raw-channels-2026-11-06.json").read_text()
        )
        db = support.open_db(self)
        cfg = ha_config(data_dir=db.path.rsplit("/", 1)[0])
        body = ha_body(
            "OK",
            "2026-11-07T02:00:00+00:00",
            "2026-11-07T02:00:10+00:00",
            observed["channels"],
        )
        body = body.replace(b"neurio raw", TOKEN.encode())
        result = ingest_ha_state(db, body, parse_instant("2026-11-07T02:00:12Z"), cfg)
        self.assertTrue(result.applied)
        row = db.conn.execute(
            "SELECT device_timestamp_utc, device_timestamp_raw, ha_state, ha_last_updated_utc, "
            "ha_last_reported_utc, source_kind, raw_json, e_imp_ws FROM samples s "
            "JOIN sample_channels sc ON sc.sample_id=s.id "
            "JOIN channels c ON c.id=sc.channel_pk WHERE c.type='CONSUMPTION'"
        ).fetchone()
        self.assertIsNone(row["device_timestamp_utc"])
        self.assertIsNone(row["device_timestamp_raw"])
        self.assertEqual(row["source_kind"], "homeassistant_relay")
        self.assertEqual(row["ha_last_updated_utc"], "2026-11-07T02:00:00.000Z")
        self.assertEqual(row["ha_last_reported_utc"], "2026-11-07T02:00:10.000Z")
        self.assertEqual(row["e_imp_ws"], 626227269168)
        self.assertNotIn(TOKEN, row["raw_json"])
        self.assertEqual(db.conn.execute("SELECT sensor_id FROM sensors").fetchone()["sensor_id"], cfg.ha_source_id)

    def test_duplicate_unchanged_power_unavailable_reset_and_gap(self):
        db = support.open_db(self)
        cfg = ha_config(data_dir=db.path.rsplit("/", 1)[0])
        first = ha_body("OK", "2026-11-07T02:00:00Z", "2026-11-07T02:00:05Z", five(1000))
        again = ingest_ha_state(db, first, parse_instant("2026-11-07T02:00:06Z"), cfg)
        replay = ingest_ha_state(db, first, parse_instant("2026-11-07T02:00:20Z"), cfg)
        self.assertEqual(replay.outcome, "duplicate")
        steady = ha_body("OK", "2026-11-07T02:00:15Z", "2026-11-07T02:00:16Z", five(1000))
        steady_result = ingest_ha_state(db, steady, parse_instant("2026-11-07T02:00:18Z"), cfg)
        self.assertEqual(steady_result.sequence_status, "accepted")
        zero = db.conn.execute(
            "SELECT delta_ws, time_basis, allocation_status, start_device_utc FROM energy_spans "
            "WHERE direction='import' ORDER BY id DESC LIMIT 1"
        ).fetchone()
        self.assertEqual(zero["delta_ws"], 0)
        self.assertEqual(zero["time_basis"], "ha_last_updated")
        self.assertEqual(zero["allocation_status"], "allocatable")
        self.assertIsNone(zero["start_device_utc"])
        down = ha_body("unavailable", "2026-11-07T03:00:00Z", "2026-11-07T03:00:01Z", [])
        down_result = ingest_ha_state(db, down, parse_instant("2026-11-07T03:00:02Z"), cfg)
        self.assertEqual(down_result.sequence_status, "unavailable")
        self.assertFalse(down_result.applied)
        jumped = ha_body("OK", "2026-11-07T05:00:00Z", "2026-11-07T05:00:02Z", five(1000 + 3_600_000))
        back = ingest_ha_state(db, jumped, parse_instant("2026-11-07T05:00:03Z"), cfg)
        self.assertTrue(back.applied)
        gap = db.conn.execute(
            "SELECT delta_ws, allocation_status FROM energy_spans WHERE direction='import' "
            "AND delta_ws > 0 ORDER BY id DESC LIMIT 1"
        ).fetchone()
        self.assertEqual(gap["delta_ws"], 3_600_000)
        self.assertEqual(gap["allocation_status"], "unresolved_gap")
        reset = ha_body("OK", "2026-11-07T05:00:15Z", "2026-11-07T05:00:16Z", five(10))
        ingest_ha_state(db, reset, parse_instant("2026-11-07T05:00:17Z"), cfg)
        negative = db.conn.execute("SELECT delta_ws FROM energy_spans WHERE delta_ws < 0").fetchone()
        self.assertIsNone(negative)
        self.assertEqual(again.applied, True)

    def test_restart_continues_the_ha_relay_identity(self):
        import os
        import tempfile

        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        path = os.path.join(tmp.name, "neurio.sqlite")
        from neurio_collector.db import Database

        cfg = ha_config(data_dir=tmp.name)
        db = Database(path)
        ingest_ha_state(
            db,
            ha_body("OK", "2026-11-07T02:00:00Z", "2026-11-07T02:00:01Z", five(50)),
            parse_instant("2026-11-07T02:00:02Z"),
            cfg,
        )
        ingest_ha_state(
            db,
            ha_body("OK", "2026-11-07T02:00:15Z", "2026-11-07T02:00:16Z", five(80)),
            parse_instant("2026-11-07T02:00:17Z"),
            cfg,
        )
        db.close()
        reopened = Database(path)
        self.addCleanup(reopened.close)
        ingest_ha_state(
            reopened,
            ha_body("OK", "2026-11-07T02:00:30Z", "2026-11-07T02:00:31Z", five(95)),
            parse_instant("2026-11-07T02:00:32Z"),
            cfg,
        )
        deltas = [
            row["delta_ws"]
            for row in reopened.conn.execute(
                "SELECT delta_ws FROM energy_spans WHERE direction='import' AND channel_pk IN "
                "(SELECT id FROM channels WHERE type='NET') ORDER BY id"
            )
        ]
        self.assertEqual(deltas, [30, 15])

    def test_poller_does_not_call_the_device_and_hides_auth_errors(self):
        db = support.open_db(self)
        cfg = ha_config(data_dir=db.path.rsplit("/", 1)[0])
        called = {"device": 0}

        def device_fetcher(url, timeout):
            called["device"] += 1
            raise AssertionError("device poll")

        def ha_fetcher(url, timeout):
            from neurio_collector.fetch import FetchResult

            return FetchResult(
                "http_ok",
                200,
                ha_body("OK", "2026-11-07T02:10:00Z", "2026-11-07T02:10:01Z", five(5)),
                None,
            )

        poller = Poller(
            cfg,
            db,
            fetcher=device_fetcher,
            ha_fetcher=ha_fetcher,
            clock=lambda: parse_instant("2026-11-07T02:10:02Z"),
        )
        delay = poller.run_once()
        self.assertEqual(called["device"], 0)
        self.assertEqual(delay, 15)
        self.assertEqual(db.conn.execute("SELECT COUNT(*) AS n FROM samples").fetchone()["n"], 1)

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self):
                self.send_response(401)
                self.end_headers()
                self.wfile.write(b"unauthorized")

            def do_POST(self):
                self.send_response(500)
                self.end_headers()

            def log_message(self, fmt, *args):
                return

        server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        self.addCleanup(server.shutdown)
        self.addCleanup(server.server_close)
        import os
        previous_proxy = os.environ.get("HTTP_PROXY"), os.environ.get("http_proxy")
        os.environ["HTTP_PROXY"] = "http://127.0.0.1:9"
        os.environ["http_proxy"] = "http://127.0.0.1:9"
        try:
            result = fetch_ha_state(
                f"http://127.0.0.1:{server.server_address[1]}",
                ENTITY,
                Secret(TOKEN),
                2,
                100000,
            )
        finally:
            for key, value in zip(("HTTP_PROXY", "http_proxy"), previous_proxy):
                if value is None:
                    os.environ.pop(key, None)
                else:
                    os.environ[key] = value
        self.assertEqual(result.outcome, "auth_error")
        self.assertNotIn(TOKEN, result.error or "")
        self.assertNotIn(TOKEN, result.body.decode())

        hits = {"n": 0}

        class RedirectHandler(BaseHTTPRequestHandler):
            def do_GET(self):
                hits["n"] += 1
                self.send_response(302)
                self.send_header("Location", "http://169.254.169.254/latest")
                self.end_headers()

            def log_message(self, fmt, *args):
                return

        redirected = ThreadingHTTPServer(("127.0.0.1", 0), RedirectHandler)
        thread = threading.Thread(target=redirected.serve_forever, daemon=True)
        thread.start()
        self.addCleanup(redirected.shutdown)
        self.addCleanup(redirected.server_close)
        moved = fetch_ha_state(
            f"http://127.0.0.1:{redirected.server_address[1]}",
            ENTITY,
            Secret(TOKEN),
            2,
            100000,
        )
        self.assertEqual(moved.outcome, "http_error")
        self.assertEqual(moved.error, "http_302")
        self.assertEqual(hits["n"], 1)
        self.assertNotIn(TOKEN, moved.error or "")
        self.assertFalse(any(isinstance(handler, urllib.request.ProxyHandler) for handler in ha_opener().handlers))


if __name__ == "__main__":
    unittest.main()
