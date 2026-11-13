"""Cloud ingest: fake cloud transports and one loopback HTTP server.

No real Neurio cloud, no credentials. The dataset values are watt-seconds
chosen so the expected milli-kWh results are known exactly.
"""

import json
import threading
import unittest
import unittest.mock
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

import support
import neurio_collector.cloud_sync as cloud_sync_module
from neurio_collector.api import build_status, dispatch
from neurio_collector.cloud_client import CloudError, NeurioCloudClient
from neurio_collector.cloud_ingest import (
    parse_stats_item,
    read_sync_state,
    store_cloud_hours,
    update_sync_state,
    ws_to_millikwh,
)
from neurio_collector.cloud_sync import CloudSyncer, _backoff_seconds
from neurio_collector.config import Config, ConfigError, Secret
from neurio_collector.mqtt_ingest import mark_hourly_acked, store_hourly_record
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
        self.rate_limit_once_at = None
        self.rate_limit_body = b""
        self.rate_limit_headers = {}
        self.http_error = None
        self.stop_after_stats = None
        self.on_stop = None
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
            if self.stop_after_stats is not None and len(self.stats_windows) >= self.stop_after_stats:
                if self.on_stop is not None:
                    self.on_stop()
            if self.http_error is not None:
                status, body, headers = self.http_error
                self.http_error = None
                if headers is None:
                    return status, body
                return status, body, headers
            if self.fail_stats:
                raise CloudError("connection_error")
            if self.fail_once_at is not None and start == self.fail_once_at:
                self.fail_once_at = None
                raise CloudError("boom")
            if self.rate_limit_once_at is not None and start == self.rate_limit_once_at:
                self.rate_limit_once_at = None
                return 429, self.rate_limit_body, self.rate_limit_headers
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
        self.assertEqual(cfg.cloud_max_requests_per_hour, 72)
        self.assertEqual(cfg.cloud_request_interval_seconds, 50.0)

    def test_request_budget_stays_inside_the_published_limit(self):
        with self.assertRaises(ConfigError):
            Config.from_env(
                {
                    **DEVICE_ENV,
                    "NEURIO_CLIENT_ID": "abc",
                    "NEURIO_CLIENT_SECRET": "def",
                    "NEURIO_CLOUD_MAX_REQUESTS_PER_HOUR": "0",
                }
            )
        with self.assertRaises(ConfigError):
            Config.from_env(
                {
                    **DEVICE_ENV,
                    "NEURIO_CLIENT_ID": "abc",
                    "NEURIO_CLIENT_SECRET": "def",
                    "NEURIO_CLOUD_MAX_REQUESTS_PER_HOUR": "145",
                }
            )
        cfg = Config.from_env(
            {
                **DEVICE_ENV,
                "NEURIO_CLIENT_ID": "abc",
                "NEURIO_CLIENT_SECRET": "def",
                "NEURIO_CLOUD_MAX_REQUESTS_PER_HOUR": "100",
            }
        )
        self.assertEqual(cfg.cloud_request_interval_seconds, 36.0)

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

    def test_429_parses_reset_and_drops_the_body(self):
        secret = "super-secret-value"

        def transport(method, url, headers, body, timeout):
            if url.endswith("/oauth2/token"):
                return 200, b'{"access_token": "t", "expires_in": 3600}'
            raw = (
                '{"code":"rate_limit_exceeding","message":"The user exceeds 144 '
                "rate limit. The remaining time to reset the limit is 15000 "
                f'milliseconds","access_token":"{secret}"}}'
            ).encode()
            return 429, raw, {"RateLimit-Reset": "1"}

        with self.assertRaises(CloudError) as caught:
            self._client(transport).get_hourly_stats("s", NOW, NOW)
        self.assertEqual(caught.exception.status, 429)
        self.assertEqual(caught.exception.retry_after_ms, 15000)
        self.assertIn("rate_limit_exceeding", str(caught.exception))
        self.assertNotIn(secret, str(caught.exception))
        self.assertNotIn("exceeds 144", str(caught.exception))

    def test_429_header_reset_when_the_body_has_no_sentence(self):
        def transport(method, url, headers, body, timeout):
            if url.endswith("/oauth2/token"):
                return 200, b'{"access_token": "t", "expires_in": 3600}'
            return 429, b'{"code":"rate_limit_exceeding"}', {"RateLimit-Reset": "2500"}

        with self.assertRaises(CloudError) as caught:
            self._client(transport).get_hourly_stats("s", NOW, NOW)
        self.assertEqual(caught.exception.retry_after_ms, 2500)

    def test_429_plain_sentence_and_missing_reset(self):
        def sentence(method, url, headers, body, timeout):
            if url.endswith("/oauth2/token"):
                return 200, b'{"access_token": "t", "expires_in": 3600}'
            return (
                429,
                b"The user exceeds 144 rate limit. The remaining time to reset the limit is 42 milliseconds",
            )

        with self.assertRaises(CloudError) as caught:
            self._client(sentence).get_hourly_stats("s", NOW, NOW)
        self.assertEqual(caught.exception.retry_after_ms, 42)
        self.assertEqual(str(caught.exception), "rate_limit_exceeding; retry after 42 ms")

        def bare(method, url, headers, body, timeout):
            if url.endswith("/oauth2/token"):
                return 200, b'{"access_token": "t", "expires_in": 3600}'
            return 429, b'{"code":"rate_limit_exceeding"}'

        with self.assertRaises(CloudError) as caught:
            self._client(bare).get_hourly_stats("s", NOW, NOW)
        self.assertIsNone(caught.exception.retry_after_ms)
        self.assertEqual(_backoff_seconds(None), 3600.0)
        self.assertEqual(_backoff_seconds(10**12), 3600.0)
        self.assertEqual(_backoff_seconds(0), 0.0)

    def test_other_http_errors_keep_only_a_short_code(self):
        def transport(method, url, headers, body, timeout):
            if url.endswith("/oauth2/token"):
                return 200, b'{"access_token": "t", "expires_in": 3600}'
            return 500, b'{"code":"server_error","token":"super-secret-value","message":"raw body"}'

        with self.assertRaises(CloudError) as caught:
            self._client(transport).get_hourly_stats("s", NOW, NOW)
        self.assertEqual(str(caught.exception), "request failed (500): server_error")
        self.assertNotIn("super-secret-value", str(caught.exception))
        self.assertNotIn("raw body", str(caught.exception))


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

    def _seed_tail_state(self, db, last_success=None):
        """A state row as a long-running install would have: harvest done,
        with (or without) a recorded last fully successful sync."""
        update_sync_state(db, harvest_complete=1, sensor_id="sensor-123")
        if last_success is not None:
            update_sync_state(db, last_success_at_utc=format_utc(last_success))

    def test_tail_window_is_48h_when_last_success_is_recent(self):
        db = support.open_db(self)
        cfg = cloud_config(db)
        self._seed_tail_state(db, last_success=NOW - timedelta(hours=1))
        cloud = FakeCloud({})
        self._syncer(db, cfg, cloud, [NOW]).run_cycle()
        self.assertTrue(cloud.stats_windows)
        self.assertEqual(
            cloud.stats_windows[0][0], int(NOW.timestamp()) - 48 * 3600
        )
        self.assertEqual(cloud.stats_windows[-1][1], int(NOW.timestamp()))

    def test_tail_backfills_from_last_success_after_a_long_outage(self):
        db = support.open_db(self)
        cfg = cloud_config(db)
        outage_start = NOW - timedelta(days=5)
        self._seed_tail_state(db, last_success=outage_start)
        first_end = int(outage_start.timestamp()) + 3600
        cloud = FakeCloud(hourly_dataset(first_end, 24 * 5))
        self._syncer(db, cfg, cloud, [NOW]).run_cycle()
        self.assertEqual(
            cloud.stats_windows[0][0], int(outage_start.timestamp())
        )
        self.assertEqual(cloud.stats_windows[-1][1], int(NOW.timestamp()))
        # A mid-outage hour actually landed in the store.
        mid = first_end + 3600 * 60
        row = db.conn.execute(
            "SELECT COUNT(*) AS n FROM cloud_hourly_records "
            "WHERE hour_end_epoch=?",
            (mid,),
        ).fetchone()
        self.assertEqual(row["n"], 1)
        state = read_sync_state(db)
        self.assertEqual(state["last_success_at_utc"], format_utc(NOW))

    def test_failed_pass_keeps_the_watermark_for_the_next_pass(self):
        db = support.open_db(self)
        cfg = cloud_config(db)
        outage_start = NOW - timedelta(days=5)
        self._seed_tail_state(db, last_success=outage_start)
        cloud = FakeCloud({})
        cloud.fail_stats = True
        self._syncer(db, cfg, cloud, [NOW]).run_cycle()
        state = read_sync_state(db)
        self.assertEqual(
            state["last_success_at_utc"], format_utc(outage_start)
        )
        self.assertIsNotNone(state["last_error"])
        # The next pass still starts at the old watermark, not at -48h.
        cloud.fail_stats = False
        cloud.stats_windows.clear()
        self._syncer(db, cfg, cloud, [NOW]).run_cycle()
        self.assertEqual(
            cloud.stats_windows[0][0], int(outage_start.timestamp())
        )
        state = read_sync_state(db)
        self.assertEqual(state["last_success_at_utc"], format_utc(NOW))

    def test_tail_first_run_without_a_recorded_success_uses_48h(self):
        db = support.open_db(self)
        cfg = cloud_config(db)
        self._seed_tail_state(db, last_success=None)
        cloud = FakeCloud({})
        self._syncer(db, cfg, cloud, [NOW]).run_cycle()
        self.assertEqual(
            cloud.stats_windows[0][0], int(NOW.timestamp()) - 48 * 3600
        )
        state = read_sync_state(db)
        self.assertEqual(state["last_success_at_utc"], format_utc(NOW))

    def test_tail_requests_are_hour_aligned(self):
        db = support.open_db(self)
        cfg = cloud_config(db)
        now37 = datetime(2026, 11, 11, 12, 37, 24, tzinfo=UTC)
        self._seed_tail_state(db, last_success=now37 - timedelta(hours=1))
        cloud = FakeCloud({})
        self._syncer(db, cfg, cloud, [now37]).run_cycle()
        self.assertTrue(cloud.stats_windows)
        for start_epoch, end_epoch in cloud.stats_windows:
            self.assertEqual(start_epoch % 3600, 0)
            self.assertEqual(end_epoch % 3600, 0)
            self.assertLessEqual(end_epoch, int(now37.timestamp()) - 37 * 60 - 24)
        # The window closes at the floored now — the in-progress hour
        # (12:00-13:00, partial) is never requested.
        self.assertEqual(
            cloud.stats_windows[-1][1],
            int(parse_instant("2026-11-11T12:00:00Z").timestamp()),
        )
        self.assertEqual(
            cloud.stats_windows[0][0],
            int(parse_instant("2026-11-09T12:00:00Z").timestamp()),
        )

    def test_harvest_resume_floors_an_unaligned_watermark(self):
        db = support.open_db(self)
        cfg = cloud_config(db)
        # A state row as the buggy build left it: mid-hour watermark.
        update_sync_state(
            db,
            harvest_from_utc="2026-10-17T00:00:00.000Z",
            harvest_through_utc="2026-10-20T05:37:00.000Z",
        )
        cloud = FakeCloud({})
        self._syncer(db, cfg, cloud, [NOW]).run_cycle()
        self.assertEqual(
            cloud.stats_windows[0][0],
            int(parse_instant("2026-10-20T05:00:00Z").timestamp()),
        )
        for start_epoch, end_epoch in cloud.stats_windows:
            self.assertEqual(start_epoch % 3600, 0)
            self.assertEqual(end_epoch % 3600, 0)
        state = read_sync_state(db)
        self.assertEqual(state["harvest_complete"], 1)
        self.assertEqual(
            int(parse_instant(state["harvest_through_utc"]).timestamp()) % 3600, 0
        )

    def test_failed_success_write_is_recorded_and_sync_continues(self):
        db = support.open_db(self)
        cfg = cloud_config(db)
        self._seed_tail_state(db, last_success=NOW - timedelta(hours=1))
        cloud = FakeCloud({})
        syncer = self._syncer(db, cfg, cloud, [NOW])
        real_update = cloud_sync_module.update_sync_state

        def flaky(db_, **fields):
            if "last_success_at_utc" in fields:
                raise RuntimeError("database is locked")
            return real_update(db_, **fields)

        with unittest.mock.patch.object(
            cloud_sync_module, "update_sync_state", flaky
        ):
            syncer.run_cycle()  # must not raise
        self.assertGreaterEqual(syncer.state_write_failures, 1)
        state = read_sync_state(db)
        self.assertIn("state_write_failed", state["last_error"])
        # The success stamp never landed, so it stays at the seeded value.
        self.assertEqual(
            state["last_success_at_utc"], format_utc(NOW - timedelta(hours=1))
        )
        # The next cycle is unaffected: sync is alive, not dead until restart.
        syncer.run_cycle()
        state = read_sync_state(db)
        self.assertEqual(state["last_success_at_utc"], format_utc(NOW))
        self.assertIsNone(state["last_error"])

    def test_failed_attempt_write_does_not_abort_the_cycle(self):
        db = support.open_db(self)
        cfg = cloud_config(db)
        self._seed_tail_state(db, last_success=NOW - timedelta(hours=1))
        cloud = FakeCloud({})
        syncer = self._syncer(db, cfg, cloud, [NOW])
        real_update = cloud_sync_module.update_sync_state

        def flaky(db_, **fields):
            if "last_attempt_at_utc" in fields:
                raise RuntimeError("database is locked")
            return real_update(db_, **fields)

        with unittest.mock.patch.object(
            cloud_sync_module, "update_sync_state", flaky
        ):
            syncer.run_cycle()  # must not raise
        self.assertEqual(syncer.state_write_failures, 1)
        state = read_sync_state(db)
        self.assertEqual(state["last_success_at_utc"], format_utc(NOW))
        self.assertIsNone(state["last_error"])

    def _syncer(self, db, cfg, cloud, clock_holder, sleeper=None):
        client = NeurioCloudClient(cfg, transport=cloud)
        return CloudSyncer(
            cfg,
            db,
            client=client,
            clock=lambda: clock_holder[0],
            sleeper=sleeper or (lambda _seconds: False),
        )

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

    def test_unaligned_buckets_are_skipped_counted_and_surfaced(self):
        db = support.open_db(self)
        aligned = {
            "start": "2026-11-11T10:00:00Z",
            "end": "2026-11-11T11:00:00Z",
            "consumptionEnergy": 3_600_000,
        }
        unaligned = {
            "start": "2026-11-11T10:05:00Z",
            "end": "2026-11-11T11:05:00Z",
            "consumptionEnergy": 3_600_000,
        }
        stored, skipped = store_cloud_hours(db, [aligned, unaligned], NOW)
        self.assertEqual((stored, skipped), (1, 1))
        epochs = [
            row[0]
            for row in db.conn.execute(
                "SELECT hour_end_epoch FROM cloud_hourly_records"
            )
        ]
        self.assertEqual(
            epochs, [int(parse_instant("2026-11-11T11:00:00Z").timestamp())]
        )
        self.assertEqual(read_sync_state(db)["unaligned_skipped_total"], 1)
        # The counter accumulates across calls and reaches the status API.
        store_cloud_hours(db, [unaligned], NOW)
        self.assertEqual(read_sync_state(db)["unaligned_skipped_total"], 2)
        cfg = cloud_config(db)
        section = build_status(cfg, db, None, NOW)["cloud_ingest"]
        self.assertEqual(section["unaligned_skipped_total"], 2)

    def test_partial_refetch_never_nulls_stored_quantities(self):
        db = support.open_db(self)
        full = {
            "start": "2026-11-11T10:00:00Z",
            "end": "2026-11-11T11:00:00Z",
            "consumptionEnergy": 3_600_000,
            "generationEnergy": 1_800_000,
            "importedEnergy": 3_600_000,
            "exportedEnergy": 1_800_000,
        }
        store_cloud_hours(db, [full], NOW)
        # A later partial response for the same hour: generation and
        # exported are omitted (they parse to NULL) and consumption is
        # corrected. The NULLs must not erase the stored values.
        partial = {
            "start": "2026-11-11T10:00:00Z",
            "end": "2026-11-11T11:00:00Z",
            "consumptionEnergy": 7_200_000,
            "importedEnergy": 7_200_000,
        }
        store_cloud_hours(db, [partial], NOW)
        row = db.conn.execute("SELECT * FROM cloud_hourly_records").fetchone()
        self.assertEqual(row["consumption_millikwh"], 2000)
        self.assertEqual(row["generation_millikwh"], 500)
        self.assertEqual(row["imported_millikwh"], 2000)
        self.assertEqual(row["exported_millikwh"], 500)
        # A genuine zero is a real value and still replaces.
        zeroed = {**full, "generationEnergy": 0}
        store_cloud_hours(db, [zeroed], NOW)
        row = db.conn.execute("SELECT * FROM cloud_hourly_records").fetchone()
        self.assertEqual(row["generation_millikwh"], 0)

    def test_harvest_paces_between_chunks(self):
        db = support.open_db(self)
        cfg = cloud_config(db, cloud_sensor_id="sensor-123")
        first_end = int(parse_instant("2026-10-17T01:00:00Z").timestamp())
        cloud = FakeCloud(hourly_dataset(first_end, 24 * 25))
        waits = []

        def sleeper(seconds):
            waits.append(seconds)
            return False

        self._syncer(db, cfg, cloud, [NOW], sleeper=sleeper).run_cycle()
        # 3 harvest chunks plus one tail chunk: a gap before every call but the first.
        self.assertEqual(waits, [50.0, 50.0, 50.0])
        self.assertEqual(len(cloud.stats_windows), 4)
        self.assertEqual(read_sync_state(db)["harvest_complete"], 1)

    def test_429_backs_off_and_resumes_without_the_body(self):
        db = support.open_db(self)
        cfg = cloud_config(db, cloud_sensor_id="sensor-123")
        first_end = int(parse_instant("2026-10-17T01:00:00Z").timestamp())
        cloud = FakeCloud(hourly_dataset(first_end, 24 * 25))
        floor = int(parse_instant("2026-10-17T00:00:00Z").timestamp())
        cloud.rate_limit_once_at = floor + 10 * 86400
        cloud.rate_limit_body = (
            b'{"code":"rate_limit_exceeding","message":"The user exceeds 144 '
            b'rate limit. The remaining time to reset the limit is 15000 milliseconds",'
            b'"token":"super-secret-value"}'
        )
        cloud.rate_limit_headers = {"RateLimit-Reset": "15000"}
        waits = []

        def sleeper(seconds):
            waits.append(seconds)
            return False

        self._syncer(db, cfg, cloud, [NOW], sleeper=sleeper).run_cycle()
        self.assertIn(15.0, waits)
        state = read_sync_state(db)
        self.assertEqual(state["harvest_complete"], 1)
        self.assertIsNone(state["last_error"])
        self.assertIsNone(state["backoff_until_utc"])
        status, body, _ = dispatch("/api/v1/status", cfg, db, None, NOW)
        self.assertEqual(status, 200)
        encoded = json.dumps(body)
        self.assertNotIn("super-secret-value", encoded)
        self.assertNotIn("exceeds 144", encoded)
        self.assertFalse(body["cloud_ingest"]["backing_off"])
        self.assertEqual(body["cloud_ingest"]["max_requests_per_hour"], 72)
        self.assertEqual(body["cloud_ingest"]["min_request_interval_seconds"], 50.0)
        self.assertEqual(body["cloud_ingest"]["published_limit_per_hour"], 144)

    def test_429_stop_during_backoff_is_sanitized_and_visible(self):
        db = support.open_db(self)
        cfg = cloud_config(db, cloud_sensor_id="sensor-123")
        first_end = int(parse_instant("2026-10-17T01:00:00Z").timestamp())
        cloud = FakeCloud(hourly_dataset(first_end, 24))
        floor = int(parse_instant("2026-10-17T00:00:00Z").timestamp())
        cloud.rate_limit_once_at = floor
        cloud.rate_limit_body = (
            b'{"code":"rate_limit_exceeding","message":"The user exceeds 144 '
            b'rate limit. The remaining time to reset the limit is 15000 milliseconds",'
            b'"token":"super-secret-value"}'
        )
        waits = []

        def sleeper(seconds):
            waits.append(seconds)
            return seconds == 15.0

        syncer = self._syncer(db, cfg, cloud, [NOW], sleeper=sleeper)
        syncer.run_cycle()
        self.assertEqual(waits, [15.0])
        state = read_sync_state(db)
        self.assertEqual(state["harvest_complete"], 0)
        self.assertEqual(state["last_error"], "CloudError: rate_limit_exceeding")
        self.assertNotIn("super-secret", state["last_error"])
        status, body, _ = dispatch("/api/v1/status", cfg, db, None, NOW)
        self.assertEqual(status, 200)
        self.assertTrue(body["db_ok"])
        section = body["cloud_ingest"]
        self.assertTrue(section["backing_off"])
        self.assertEqual(section["backoff_until_utc"], format_utc(NOW + timedelta(seconds=15)))
        self.assertEqual(section["last_error"], "CloudError: rate_limit_exceeding")
        self.assertNotIn("super-secret-value", json.dumps(body))
        self.assertNotIn("exceeds 144", json.dumps(body))

    def test_zero_reset_waits_one_pace_interval_instead_of_spinning(self):
        db = support.open_db(self)
        cfg = cloud_config(
            db,
            cloud_sensor_id="sensor-123",
            cloud_harvest_from="2026-11-11T00:00:00Z",
        )
        cloud = FakeCloud({})
        cloud.rate_limit_once_at = int(parse_instant("2026-11-11T00:00:00Z").timestamp())
        cloud.rate_limit_body = (
            b'{"code":"rate_limit_exceeding","message":"The user exceeds 144 '
            b'rate limit. The remaining time to reset the limit is 0 milliseconds"}'
        )
        waits = []

        def sleeper(seconds):
            waits.append(seconds)
            return seconds >= 50

        self._syncer(db, cfg, cloud, [NOW], sleeper=sleeper).run_cycle()
        self.assertEqual(waits, [50.0])
        self.assertEqual(
            read_sync_state(db)["last_error"], "CloudError: rate_limit_exceeding"
        )

    def test_stop_between_chunks_does_not_continue(self):
        db = support.open_db(self)
        cfg = cloud_config(db, cloud_sensor_id="sensor-123")
        first_end = int(parse_instant("2026-10-17T01:00:00Z").timestamp())
        cloud = FakeCloud(hourly_dataset(first_end, 24 * 25))
        syncer = self._syncer(db, cfg, cloud, [NOW])
        cloud.stop_after_stats = 1
        cloud.on_stop = syncer._stop.set
        syncer.run_cycle()
        self.assertEqual(len(cloud.stats_windows), 1)
        state = read_sync_state(db)
        self.assertEqual(state["harvest_complete"], 0)
        floor = int(parse_instant("2026-10-17T00:00:00Z").timestamp())
        self.assertEqual(
            int(parse_instant(state["harvest_through_utc"]).timestamp()),
            floor + 10 * 86400,
        )

    def test_persisted_backoff_is_honored_before_the_next_request(self):
        db = support.open_db(self)
        cfg = cloud_config(db, cloud_sensor_id="sensor-123")
        from neurio_collector.cloud_ingest import update_sync_state

        update_sync_state(
            db, backoff_until_utc=format_utc(NOW + timedelta(seconds=30))
        )
        first_end = int(parse_instant("2026-10-17T01:00:00Z").timestamp())
        cloud = FakeCloud(hourly_dataset(first_end, 4))
        waits = []

        def sleeper(seconds):
            waits.append(seconds)
            return False

        self._syncer(db, cfg, cloud, [NOW], sleeper=sleeper).run_cycle()
        self.assertAlmostEqual(waits[0], 30.0, places=3)
        self.assertGreater(len(cloud.stats_windows), 0)

    def test_http_error_body_is_not_stored(self):
        db = support.open_db(self)
        cfg = cloud_config(db, cloud_sensor_id="sensor-123")
        cloud = FakeCloud({})
        cloud.http_error = (
            500,
            b'{"code":"server_error","token":"super-secret-value","message":"raw body"}',
            None,
        )
        self._syncer(db, cfg, cloud, [NOW]).run_cycle()
        state = read_sync_state(db)
        self.assertEqual(state["last_error"], "CloudError: request failed (500): server_error")
        status, body, _ = dispatch("/api/v1/status", cfg, db, None)
        self.assertNotIn("super-secret-value", json.dumps(body))
        self.assertNotIn("raw body", json.dumps(body))
        self.assertEqual(
            body["cloud_ingest"]["last_error"],
            "CloudError: request failed (500): server_error",
        )


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
        self.assertEqual(section["published_limit_per_hour"], 144)
        self.assertEqual(section["max_requests_per_hour"], 72)
        self.assertEqual(section["min_request_interval_seconds"], 50.0)
        self.assertFalse(section["backing_off"])
        self.assertIsNone(section["backoff_until_utc"])
        self.assertIsNone(section["tail_window_start_utc"])

    def test_status_ack_classification_splits_the_unacked(self):
        db = support.open_db(self)
        cfg = support.make_config(data_dir=db.path.rsplit("/", 1)[0])
        db.conn.execute(
            "CREATE TABLE hourly_backfill_provenance("
            "record_id INTEGER PRIMARY KEY, backfill_source TEXT NOT NULL, "
            "detail TEXT NOT NULL)"
        )

        def store(record_id, received, **changes):
            body = {
                "id": record_id,
                "timestamp": "2026-11-11T10:00:00Z",
                "net": {"delta_net_kwh": 1.0},
            }
            body.update(changes)
            result = store_hourly_record(
                db, json.dumps(body).encode(), received, cfg
            )
            self.assertEqual(result.outcome, "stored")

        # (a) Backfill row: provenance entry, never ACKed, days old.
        store(101, NOW - timedelta(days=3))
        db.conn.execute(
            "INSERT INTO hourly_backfill_provenance VALUES"
            "(101, 'neurio_cloud_stats', 'backfill-20261110')"
        )
        # (c) Native, ACKed.
        store(102, NOW - timedelta(hours=2))
        mark_hourly_acked(db, 102, NOW - timedelta(hours=2))
        # (b) Pending, fresh and old.
        store(103, NOW - timedelta(minutes=5))
        store(104, NOW - timedelta(hours=2))
        # (a) Bridge says no ACK required; old, but not pending or stuck.
        store(105, NOW - timedelta(hours=3), ack_required=False)
        # (b) Explicitly required, and a non-boolean treated as absent.
        store(106, NOW - timedelta(minutes=10), ack_required=True)
        store(107, NOW - timedelta(minutes=10), ack_required="no")

        hourly = build_status(cfg, db, None, NOW)["hourly_records"]
        self.assertEqual(hourly["stored"], 7)
        self.assertEqual(hourly["acked"], 1)
        self.assertEqual(hourly["ack_not_required"], 2)
        self.assertEqual(hourly["ack_pending"], 4)
        self.assertEqual(hourly["ack_pending_stuck"], 1)
        self.assertEqual(
            hourly["ack_pending_oldest_received_at_utc"],
            format_utc(NOW - timedelta(hours=2)),
        )
        # The three classes partition the table.
        self.assertEqual(
            hourly["acked"] + hourly["ack_not_required"] + hourly["ack_pending"],
            hourly["stored"],
        )

    def test_status_tail_window_start_reflects_the_watermark(self):
        db = support.open_db(self)
        cfg = cloud_config(db)
        update_sync_state(
            db,
            harvest_complete=1,
            last_success_at_utc=format_utc(NOW - timedelta(days=5)),
        )
        section = build_status(cfg, db, None, NOW)["cloud_ingest"]
        self.assertEqual(
            section["tail_window_start_utc"],
            format_utc(NOW - timedelta(days=5)),
        )
        # A recent success leaves the plain trailing window.
        update_sync_state(db, last_success_at_utc=format_utc(NOW - timedelta(hours=1)))
        section = build_status(cfg, db, None, NOW)["cloud_ingest"]
        self.assertEqual(
            section["tail_window_start_utc"],
            format_utc(NOW - timedelta(hours=48)),
        )

    def test_status_cloud_section_after_sync(self):
        db = support.open_db(self)
        cfg = cloud_config(db)
        first_end = int(parse_instant("2026-10-17T01:00:00Z").timestamp())
        cloud = FakeCloud(hourly_dataset(first_end, 24 * 25))
        client = NeurioCloudClient(cfg, transport=cloud)
        CloudSyncer(
            cfg, db, client=client, clock=lambda: NOW, sleeper=lambda _seconds: False
        ).run_cycle()
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
        self.assertEqual(section["max_requests_per_hour"], 72)
        self.assertFalse(section["backing_off"])
        self.assertIsNone(body["merge"]["error"])

    def test_status_tolerates_cloud_state_from_before_pacing(self):
        db = support.open_db(self)
        cfg = support.make_config(data_dir=db.path.rsplit("/", 1)[0])
        db.conn.execute(
            "CREATE TABLE cloud_sync_state ("
            "id INTEGER PRIMARY KEY CHECK (id = 1), "
            "sensor_id TEXT, harvest_from_utc TEXT, harvest_through_utc TEXT, "
            "harvest_complete INTEGER NOT NULL DEFAULT 0, "
            "last_attempt_at_utc TEXT, last_success_at_utc TEXT, "
            "last_error TEXT, last_error_at_utc TEXT)"
        )
        db.conn.execute(
            "INSERT INTO cloud_sync_state(id, harvest_complete, last_error) "
            "VALUES(1, 0, 'CloudError: connection_error')"
        )
        status, body, _ = dispatch("/api/v1/status", cfg, db, None)
        self.assertEqual(status, 200)
        self.assertTrue(body["db_ok"])
        section = body["cloud_ingest"]
        self.assertEqual(section["last_error"], "CloudError: connection_error")
        self.assertFalse(section["backing_off"])
        self.assertIsNone(section["backoff_until_utc"])
        self.assertEqual(section["published_limit_per_hour"], 144)


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
            CloudSyncer(cfg, db, clock=lambda: NOW, sleeper=lambda _seconds: False).run_cycle()
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
