"""Cloud ingest: fake cloud transports and one loopback HTTP server.

No real Neurio cloud, no credentials. The dataset values are watt-seconds
chosen so the expected milli-kWh results are known exactly.
"""

import json
import threading
import unittest
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

import support
from neurio_collector.api import dispatch
from neurio_collector.cloud_client import CloudError, NeurioCloudClient
from neurio_collector.cloud_ingest import (
    parse_stats_item,
    read_sync_state,
    store_cloud_hours,
    ws_to_millikwh,
)
from neurio_collector.cloud_sync import CloudSyncer
from neurio_collector.config import Config, ConfigError, Secret
from neurio_collector.mqtt_ingest import store_hourly_record
from neurio_collector.timeutil import format_utc, parse_instant

UTC = timezone.utc
NOW = datetime(2026, 11, 11, 12, 0, 0, tzinfo=UTC)

DEVICE_ENV = {
    "SOURCE_MODE": "mqtt",
    "BIND_ADDRESS": "127.0.0.1",
    "BIND_PORT": "8095",
    "DATA_DIR": "/tmp/neurio-test",
    "MQTT_BROKER_HOST": "192.168.1.10",
}


def iso(epoch: int) -> str:
    return format_utc(datetime.fromtimestamp(epoch, UTC))


def stats_item(end_epoch: int, consumption_ws, **fields):
    item = {
        "start": iso(end_epoch - 3600),
        "end": iso(end_epoch),
        "consumptionEnergy": consumption_ws,
    }
    item.update(fields)
    return item


def cloud_config(db, **overrides):
    data = dict(
        data_dir=db.path.rsplit("/", 1)[0],
        cloud_client_id="test-client",
        cloud_client_secret=Secret("test-secret"),
        cloud_harvest_from="2026-10-17T00:00:00Z",
    )
    data.update(overrides)
    return support.make_config(**data)


class FakeCloud:
    """Transport fake for the Neurio cloud API.

    `hours` maps hour-end epoch -> stats fields (watt-seconds). Window
    matching is inclusive on the item start, like the real API's inclusive
    bounds: an item starting exactly at a window's end is returned by both
    adjacent windows, which is what makes the dedup tests meaningful.
    """

    def __init__(self, hours):
        self.hours = dict(hours)
        self.token_posts = 0
        self.stats_windows = []
        self.fail_stats = False
        self.fail_once_at = None
        self.sensor_id = "sensor-123"

    def __call__(self, method, url, headers, body, timeout):
        parsed = urlparse(url)
        if parsed.path.endswith("/oauth2/token"):
            self.token_posts += 1
            form = parse_qs(body.decode())
            assert form["grant_type"] == ["client_credentials"]
            assert form["client_id"] == ["test-client"]
            return 200, json.dumps(
                {"access_token": f"tok-{self.token_posts}", "expires_in": 3600}
            ).encode()
        if parsed.path.endswith("/users/current"):
            return 200, json.dumps(
                {"locations": [{"sensors": [{"sensorId": self.sensor_id}]}]}
            ).encode()
        if parsed.path.endswith("/samples/stats"):
            query = parse_qs(parsed.query)
            assert query["granularity"] == ["hours"]
            assert query["frequency"] == ["1"]
            assert query["sensorId"] == [self.sensor_id]
            start = int(parse_instant(query["start"][0]).timestamp())
            end = int(parse_instant(query["end"][0]).timestamp())
            self.stats_windows.append((start, end))
            if self.fail_stats:
                raise CloudError("connection_error")
            if self.fail_once_at is not None and start == self.fail_once_at:
                self.fail_once_at = None
                raise CloudError("boom")
            items = []
            for end_epoch in sorted(self.hours):
                item_start = end_epoch - 3600
                if start <= item_start <= end:
                    fields = dict(self.hours[end_epoch])
                    consumption = fields.pop("consumptionEnergy")
                    items.append(stats_item(end_epoch, consumption, **fields))
            return 200, json.dumps(items).encode()
        return 404, b"{}"


def hourly_dataset(start_end_epoch: int, count: int, base_ws=3_600_000):
    """`count` consecutive hourly items ending at consecutive hour ends."""
    return {
        start_end_epoch + 3600 * i: {
            "consumptionEnergy": base_ws + 3600 * i,
            "generationEnergy": 1_800_000,
            "importedEnergy": base_ws + 3600 * i,
            "exportedEnergy": 900_000,
        }
        for i in range(count)
    }


class CloudConfigTests(unittest.TestCase):
    def test_unconfigured_by_default(self):
        cfg = Config.from_env(DEVICE_ENV)
        self.assertFalse(cfg.cloud_configured)
        self.assertFalse(support.make_config().cloud_configured)

    def test_credentials_must_come_in_pairs(self):
        with self.assertRaises(ConfigError):
            Config.from_env({**DEVICE_ENV, "NEURIO_CLIENT_ID": "abc"})
        with self.assertRaises(ConfigError):
            Config.from_env({**DEVICE_ENV, "NEURIO_CLIENT_SECRET": "abc"})
        with self.assertRaises(ConfigError):
            support.make_config(cloud_client_id="abc")
        with self.assertRaises(ConfigError):
            support.make_config(cloud_client_secret=Secret("abc"))

    def test_configured_when_both_set(self):
        cfg = Config.from_env(
            {**DEVICE_ENV, "NEURIO_CLIENT_ID": "abc", "NEURIO_CLIENT_SECRET": "def"}
        )
        self.assertTrue(cfg.cloud_configured)
        self.assertEqual(cfg.cloud_api_base, "https://api.neur.io/v1")
        self.assertEqual(cfg.cloud_tail_hours, 48)
        self.assertEqual(cfg.cloud_sync_interval_seconds, 3600.0)

    def test_api_base_http_only_for_loopback(self):
        with self.assertRaises(ConfigError):
            support.make_config(
                cloud_client_id="a",
                cloud_client_secret=Secret("b"),
                cloud_api_base="http://192.168.1.5/v1",
            )
        cfg = support.make_config(
            cloud_client_id="a",
            cloud_client_secret=Secret("b"),
            cloud_api_base="http://127.0.0.1:8765/v1",
        )
        self.assertTrue(cfg.cloud_configured)

    def test_harvest_from_must_parse(self):
        with self.assertRaises(ConfigError):
            support.make_config(
                cloud_client_id="a",
                cloud_client_secret=Secret("b"),
                cloud_harvest_from="not-a-date",
            )


class ConversionTests(unittest.TestCase):
    def test_ws_to_millikwh_rounds_half_up(self):
        self.assertEqual(ws_to_millikwh(1_467_234), 408)  # 407.565 -> 408
        self.assertEqual(ws_to_millikwh("2486673"), 691)  # numeric string
        self.assertEqual(ws_to_millikwh(11_061_025), 3073)  # verified hour
        self.assertEqual(ws_to_millikwh(0), 0)
        self.assertEqual(ws_to_millikwh(1800), 1)  # exactly half -> up
        self.assertIsNone(ws_to_millikwh(None))
        self.assertIsNone(ws_to_millikwh(True))
        self.assertIsNone(ws_to_millikwh("junk"))
        self.assertIsNone(ws_to_millikwh(float("nan")))

    def test_parse_item_keys_by_end_and_keeps_values_as_is(self):
        first = parse_stats_item(stats_item(1_700_000_000, 3_600_000,
                                            generationEnergy=1_800_000))
        second = parse_stats_item(stats_item(1_700_003_600, 7_200_000))
        self.assertEqual(first.hour_end_epoch, 1_700_000_000)
        self.assertEqual(first.consumption_millikwh, 1000)
        self.assertEqual(first.generation_millikwh, 500)
        # Not differenced: the second hour keeps its own full value.
        self.assertEqual(second.consumption_millikwh, 2000)
        self.assertIsNone(second.generation_millikwh)

    def test_parse_item_skips_missing_consumption_and_bad_times(self):
        self.assertIsNone(parse_stats_item({"start": iso(0), "end": iso(3600)}))
        self.assertIsNone(
            parse_stats_item({"start": "junk", "end": iso(3600),
                              "consumptionEnergy": 5})
        )
        self.assertIsNone(parse_stats_item("not a dict"))


class ClientTests(unittest.TestCase):
    def _client(self, transport):
        cfg = support.make_config(
            cloud_client_id="test-client", cloud_client_secret=Secret("test-secret")
        )
        return NeurioCloudClient(cfg, transport=transport)

    def test_token_cached_and_sensor_id_found(self):
        cloud = FakeCloud({})
        client = self._client(cloud)
        self.assertEqual(client.get_sensor_id(), "sensor-123")
        self.assertEqual(client.get_sensor_id(), "sensor-123")
        self.assertEqual(cloud.token_posts, 1)

    def test_sensor_id_capital_variant(self):
        def transport(method, url, headers, body, timeout):
            if url.endswith("/oauth2/token"):
                return 200, b'{"access_token": "t", "expires_in": 3600}'
            return 200, b'{"locations": [{"sensors": [{"SensorID": "sensor-9"}]}]}'

        self.assertEqual(self._client(transport).get_sensor_id(), "sensor-9")

    def test_401_forces_one_token_refresh_and_retry(self):
        calls = {"token": 0, "get": 0}

        def transport(method, url, headers, body, timeout):
            if url.endswith("/oauth2/token"):
                calls["token"] += 1
                return 200, json.dumps(
                    {"access_token": f"t{calls['token']}", "expires_in": 3600}
                ).encode()
            calls["get"] += 1
            if headers["Authorization"] == "Bearer t1":
                return 401, b"expired"
            return 200, b'{"locations": [{"sensors": [{"sensorId": "s1"}]}]}'

        self.assertEqual(self._client(transport).get_sensor_id(), "s1")
        self.assertEqual(calls, {"token": 2, "get": 2})

    def test_token_failure_raises_cloud_error(self):
        def transport(method, url, headers, body, timeout):
            return 403, b"forbidden"

        with self.assertRaises(CloudError) as caught:
            self._client(transport).get_sensor_id()
        self.assertEqual(caught.exception.status, 403)

    def test_stats_response_must_be_a_list(self):
        def transport(method, url, headers, body, timeout):
            if url.endswith("/oauth2/token"):
                return 200, b'{"access_token": "t", "expires_in": 3600}'
            return 200, b'{"unexpected": true}'

        with self.assertRaises(CloudError):
            self._client(transport).get_hourly_stats("s", NOW, NOW)


class SyncTests(unittest.TestCase):
    def test_cloud_tables_are_created_lazily_on_first_sync(self):
        db = support.open_db(self)
        names = {
            row[0]
            for row in db.conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            )
        }
        self.assertNotIn("cloud_hourly_records", names)
        cfg = cloud_config(db)
        cloud = FakeCloud({})
        self._syncer(db, cfg, cloud, [NOW]).run_cycle()
        names = {
            row[0]
            for row in db.conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            )
        }
        self.assertIn("cloud_hourly_records", names)
        self.assertIn("cloud_sync_state", names)

    def _syncer(self, db, cfg, cloud, clock_holder):
        client = NeurioCloudClient(cfg, transport=cloud)
        return CloudSyncer(cfg, db, client=client, clock=lambda: clock_holder[0])

    def test_harvest_and_tail_store_hours(self):
        db = support.open_db(self)
        cfg = cloud_config(db)
        first_end = int(parse_instant("2026-10-17T01:00:00Z").timestamp())
        cloud = FakeCloud(hourly_dataset(first_end, 24 * 25))
        clock = [NOW]
        self._syncer(db, cfg, cloud, clock).run_cycle()
        state = read_sync_state(db)
        self.assertEqual(state["harvest_complete"], 1)
        self.assertEqual(state["sensor_id"], "sensor-123")
        self.assertIsNotNone(state["last_success_at_utc"])
        self.assertIsNone(state["last_error"])
        count = db.conn.execute(
            "SELECT COUNT(*) AS n FROM cloud_hourly_records"
        ).fetchone()["n"]
        self.assertEqual(count, 24 * 25)
        row = db.conn.execute(
            "SELECT * FROM cloud_hourly_records WHERE hour_end_epoch=?", (first_end,)
        ).fetchone()
        self.assertEqual(row["consumption_millikwh"], 1000)
        self.assertEqual(row["generation_millikwh"], 500)
        self.assertEqual(row["hour_end_utc"], iso(first_end))
        self.assertEqual(row["fetched_at_utc"], format_utc(NOW))

    def test_boundary_hours_deduplicate_across_chunks(self):
        db = support.open_db(self)
        cfg = cloud_config(db)  # harvest floor 2026-10-17, now 2026-11-11: 3 chunks
        first_end = int(parse_instant("2026-10-17T01:00:00Z").timestamp())
        dataset = hourly_dataset(first_end, 24 * 25)
        cloud = FakeCloud(dataset)
        self._syncer(db, cfg, cloud, [NOW]).run_cycle()
        count = db.conn.execute(
            "SELECT COUNT(*) AS n FROM cloud_hourly_records"
        ).fetchone()["n"]
        # The fake returns boundary items twice (inclusive bounds); the
        # hour-end upsert must leave exactly one row per distinct hour.
        self.assertEqual(count, len(dataset))
        dupes = db.conn.execute(
            "SELECT hour_end_epoch, COUNT(*) AS c FROM cloud_hourly_records "
            "GROUP BY hour_end_epoch HAVING c > 1"
        ).fetchall()
        self.assertEqual(dupes, [])

    def test_harvest_resumes_after_a_failed_chunk(self):
        db = support.open_db(self)
        cfg = cloud_config(db)
        first_end = int(parse_instant("2026-10-17T01:00:00Z").timestamp())
        cloud = FakeCloud(hourly_dataset(first_end, 24 * 26))
        floor = int(parse_instant("2026-10-17T00:00:00Z").timestamp())
        second_chunk_start = floor + 10 * 86400
        cloud.fail_once_at = second_chunk_start
        syncer = self._syncer(db, cfg, cloud, [NOW])
        syncer.run_cycle()
        state = read_sync_state(db)
        self.assertEqual(state["harvest_complete"], 0)
        self.assertEqual(
            int(parse_instant(state["harvest_through_utc"]).timestamp()),
            second_chunk_start,
        )
        self.assertIn("CloudError", state["last_error"])
        windows_before = len(cloud.stats_windows)
        syncer.run_cycle()
        state = read_sync_state(db)
        self.assertEqual(state["harvest_complete"], 1)
        self.assertIsNone(state["last_error"])
        new_windows = cloud.stats_windows[windows_before:]
        # The resumed harvest never re-asks for the completed first chunk.
        self.assertTrue(all(start >= second_chunk_start for start, _ in new_windows))

    def test_tail_sync_replaces_a_changed_value(self):
        db = support.open_db(self)
        cfg = cloud_config(db)
        target_end = int(NOW.timestamp()) - 3600  # inside the 48h tail window
        cloud = FakeCloud(hourly_dataset(target_end - 3600 * 5, 6))
        clock = [NOW]
        syncer = self._syncer(db, cfg, cloud, clock)
        syncer.run_cycle()
        before = db.conn.execute(
            "SELECT * FROM cloud_hourly_records WHERE hour_end_epoch=?",
            (target_end,),
        ).fetchone()
        self.assertEqual(before["consumption_millikwh"], 1005)
        cloud.hours[target_end] = {"consumptionEnergy": 7_200_000}
        clock[0] = NOW + timedelta(hours=1)
        syncer.run_cycle()
        after = db.conn.execute(
            "SELECT * FROM cloud_hourly_records WHERE hour_end_epoch=?",
            (target_end,),
        ).fetchone()
        self.assertEqual(after["consumption_millikwh"], 2000)
        self.assertEqual(after["fetched_at_utc"], before["fetched_at_utc"])
        self.assertGreater(after["last_fetched_at_utc"], before["last_fetched_at_utc"])

    def test_cloud_and_local_values_for_one_hour_both_survive(self):
        db = support.open_db(self)
        cfg = cloud_config(db)
        hour_end = int(parse_instant("2026-11-11T11:00:00Z").timestamp())
        body = json.dumps(
            {
                "id": hour_end,
                "timestamp": "2026-11-11T11:00:00Z",
                "net": {"import_kwh": 10.0, "export_kwh": 1.0, "delta_net_kwh": 0.5},
                "consumption": {"delta_import_kwh": 0.8},
                "generation": {"delta_export_kwh": 0.3},
            }
        ).encode()
        result = store_hourly_record(db, body, NOW, cfg)
        self.assertEqual(result.outcome, "stored")
        cloud = FakeCloud({hour_end: {"consumptionEnergy": 3_600_000}})
        self._syncer(db, cfg, cloud, [NOW]).run_cycle()
        local = db.conn.execute(
            "SELECT * FROM hourly_records WHERE record_id=?", (hour_end,)
        ).fetchone()
        self.assertEqual(local["net_delta_net_millikwh"], 500)
        cloud_row = db.conn.execute(
            "SELECT * FROM cloud_hourly_records WHERE hour_end_epoch=?", (hour_end,)
        ).fetchone()
        self.assertEqual(cloud_row["consumption_millikwh"], 1000)

    def test_failure_is_recorded_and_local_paths_unaffected(self):
        db = support.open_db(self)
        cfg = cloud_config(db)
        cloud = FakeCloud({})
        cloud.fail_stats = True
        syncer = self._syncer(db, cfg, cloud, [NOW])
        syncer.run_cycle()  # must not raise
        state = read_sync_state(db)
        self.assertEqual(state["harvest_complete"], 0)
        self.assertIn("CloudError", state["last_error"])
        self.assertIsNone(state["last_success_at_utc"])
        self.assertIsNotNone(state["last_attempt_at_utc"])
        status, body, _ = dispatch("/api/v1/status", cfg, db, None)
        self.assertEqual(status, 200)
        self.assertTrue(body["cloud_ingest"]["configured"])
        self.assertIn("CloudError", body["cloud_ingest"]["last_error"])
        status, body, _ = dispatch("/api/v1/hourly", cfg, db, None)
        self.assertEqual(status, 200)

    def test_offset_timestamps_store_by_utc_end(self):
        db = support.open_db(self)
        item = {
            "start": "2026-03-08T01:00:00-06:00",
            "end": "2026-03-08T03:00:00-05:00",  # spring-forward: a 1h wall jump
            "consumptionEnergy": 3_600_000,
        }
        stored, skipped = store_cloud_hours(db, [item], NOW)
        self.assertEqual((stored, skipped), (1, 0))
        row = db.conn.execute("SELECT * FROM cloud_hourly_records").fetchone()
        expected_end = int(parse_instant("2026-03-08T03:00:00-05:00").timestamp())
        self.assertEqual(row["hour_end_epoch"], expected_end)
        self.assertEqual(row["hour_end_utc"], "2026-03-08T08:00:00.000Z")


class StatusTests(unittest.TestCase):
    def test_status_cloud_section_unconfigured(self):
        db = support.open_db(self)
        cfg = support.make_config(data_dir=db.path.rsplit("/", 1)[0])
        status, body, _ = dispatch("/api/v1/status", cfg, db, None)
        self.assertEqual(status, 200)
        section = body["cloud_ingest"]
        self.assertFalse(section["configured"])
        self.assertEqual(section["hours_stored"], 0)
        self.assertFalse(section["harvest_complete"])
        self.assertIsNone(section["newest_hour_end_utc"])

    def test_status_cloud_section_after_sync(self):
        db = support.open_db(self)
        cfg = cloud_config(db)
        first_end = int(parse_instant("2026-10-17T01:00:00Z").timestamp())
        cloud = FakeCloud(hourly_dataset(first_end, 24 * 25))
        client = NeurioCloudClient(cfg, transport=cloud)
        CloudSyncer(cfg, db, client=client, clock=lambda: NOW).run_cycle()
        status, body, _ = dispatch("/api/v1/status", cfg, db, None)
        self.assertEqual(status, 200)
        section = body["cloud_ingest"]
        self.assertTrue(section["configured"])
        self.assertEqual(section["sensor_id"], "sensor-123")
        self.assertTrue(section["harvest_complete"])
        self.assertEqual(section["hours_stored"], 24 * 25)
        self.assertEqual(
            section["newest_hour_end_utc"], iso(first_end + 3600 * (24 * 25 - 1))
        )
        self.assertIsNotNone(section["last_success_at_utc"])


class LoopbackServerTests(unittest.TestCase):
    def test_end_to_end_over_real_http(self):
        dataset_end = int(parse_instant("2026-11-11T11:00:00Z").timestamp())
        hours = hourly_dataset(dataset_end - 3600 * 3, 3)

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass

            def _send(self, payload):
                raw = json.dumps(payload).encode()
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(raw)))
                self.end_headers()
                self.wfile.write(raw)

            def do_POST(self):
                self.rfile.read(int(self.headers["Content-Length"]))
                self._send({"access_token": "wire-tok", "expires_in": 3600})

            def do_GET(self):
                parsed = urlparse(self.path)
                if parsed.path.endswith("/users/current"):
                    self._send({"locations": [{"sensors": [{"sensorId": "wire-1"}]}]})
                    return
                query = parse_qs(parsed.query)
                start = int(parse_instant(query["start"][0]).timestamp())
                end = int(parse_instant(query["end"][0]).timestamp())
                items = []
                for end_epoch, fields in sorted(hours.items()):
                    if start <= end_epoch - 3600 <= end:
                        fields = dict(fields)
                        consumption = fields.pop("consumptionEnergy")
                        items.append(stats_item(end_epoch, consumption, **fields))
                self._send(items)

        server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            port = server.server_address[1]
            db = support.open_db(self)
            cfg = cloud_config(
                db,
                cloud_api_base=f"http://127.0.0.1:{port}/v1",
                cloud_harvest_from="2026-11-11T00:00:00Z",
                cloud_sensor_id="wire-1",
            )
            CloudSyncer(cfg, db, clock=lambda: NOW).run_cycle()
        finally:
            server.shutdown()
            server.server_close()
        count = db.conn.execute(
            "SELECT COUNT(*) AS n FROM cloud_hourly_records"
        ).fetchone()["n"]
        self.assertEqual(count, 3)
        state = read_sync_state(db)
        self.assertEqual(state["harvest_complete"], 1)
        self.assertIsNone(state["last_error"])


if __name__ == "__main__":
    unittest.main()
