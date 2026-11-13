"""Hourly MQTT ingest, acknowledgements, and the read API. No real broker."""

import json
import time
import unittest
import unittest.mock
from datetime import timedelta

import support
import neurio_collector.poller as poller_module
from neurio_collector.api import dispatch
from neurio_collector.config import Config, ConfigError
from neurio_collector.mqtt_client import MqttClient, MqttError, MqttMessage
from neurio_collector.mqtt_ingest import mark_hourly_acked, store_hourly_record
from neurio_collector.poller import Poller
from neurio_collector.timeutil import format_utc, parse_instant

RECEIVED = parse_instant("2026-11-06T18:00:05Z")
HOURLY_ID = int(parse_instant("2026-11-06T17:00:00Z").timestamp())
HOURLY_RECORD_KEYS = (
    "record_id",
    "device_timestamp_raw",
    "device_timestamp_utc",
    "retries",
    "received_at_utc",
    "acked_at_utc",
    "net_import_kwh",
    "net_export_kwh",
    "net_delta_net_kwh",
    "generation_delta_export_kwh",
    "consumption_delta_import_kwh",
    "phase_a_delta_import_kwh",
    "phase_b_delta_import_kwh",
)

BASE_ENV = {
    "SOURCE_MODE": "mqtt",
    "BIND_ADDRESS": "127.0.0.1",
    "BIND_PORT": "8095",
    "DATA_DIR": "/tmp/neurio-test",
    "MQTT_BROKER_HOST": "192.168.1.10",
}


def hourly_body(**changes):
    body = {
        "id": HOURLY_ID,
        "timestamp": "2026-11-06T17:00:00Z",
        "retries": 1,
        "net": {
            "import_kwh": 1234.567,
            "export_kwh": 89.1,
            "delta_import_kwh": 0.75,
            "delta_export_kwh": 0.25,
            "delta_net_kwh": 0.5,
        },
        "generation": {"export_kwh": 456.0, "delta_export_kwh": 0.3},
        "consumption": {"import_kwh": 1600.0, "delta_import_kwh": 0.8},
        "phase_a": {"delta_import_kwh": 0.45, "voltage_v": 121.3},
        "phase_b": {"delta_import_kwh": 0.35, "voltage_v": 119.8},
    }
    body.update(changes)
    return json.dumps(body).encode()


class ConfigTests(unittest.TestCase):
    def test_from_env_happy_path(self):
        cfg = Config.from_env(BASE_ENV)
        self.assertEqual(cfg.source_mode, "mqtt")
        self.assertEqual(cfg.mqtt_broker_host, "192.168.1.10")
        self.assertEqual(cfg.mqtt_broker_port, 1883)
        self.assertEqual(cfg.mqtt_hourly_topic, "neurio/hourly/energy")
        self.assertEqual(cfg.mqtt_ack_topic, "neurio/hourly/ack")
        self.assertIsNone(cfg.mqtt_username)
        self.assertIsNone(cfg.mqtt_password)

    def test_from_env_auth_and_tls(self):
        cfg = Config.from_env(
            {
                **BASE_ENV,
                "MQTT_USERNAME": "bridge",
                "MQTT_PASSWORD": "synthetic-password",
                "MQTT_TLS": "true",
                "MQTT_BROKER_PORT": "8883",
            }
        )
        self.assertEqual(cfg.mqtt_username, "bridge")
        self.assertEqual(str(cfg.mqtt_password), "<redacted>")
        self.assertTrue(cfg.mqtt_tls)
        self.assertEqual(cfg.mqtt_broker_port, 8883)

    def test_required_bind_and_rejects_unspecified(self):
        with self.assertRaises(ConfigError):
            Config.from_env({key: value for key, value in BASE_ENV.items() if key != "BIND_ADDRESS"})
        with self.assertRaises(ConfigError):
            Config.from_env({**BASE_ENV, "BIND_ADDRESS": "0.0.0.0"})
        cfg = Config.from_env(
            {**BASE_ENV, "BIND_ADDRESS": "0.0.0.0", "ALLOW_CONTAINER_WILDCARD_BIND": "host-publish-pinned"}
        )
        self.assertEqual(cfg.bind_address, "0.0.0.0")

    def test_removed_modes_and_raw_topic_are_rejected(self):
        with self.assertRaises(ConfigError):
            Config.from_env({**BASE_ENV, "SOURCE_MODE": "device"})
        with self.assertRaises(ConfigError):
            Config.from_env({**BASE_ENV, "SOURCE_MODE": "homeassistant"})
        with self.assertRaises(ConfigError) as caught:
            Config.from_env({**BASE_ENV, "NEURIO_URL": "http://192.168.1.50/current-sample"})
        self.assertIn("NEURIO_URL", str(caught.exception))
        with self.assertRaises(ConfigError):
            Config.from_env({**BASE_ENV, "HA_TOKEN": "synthetic"})
        with self.assertRaises(ConfigError):
            Config.from_env({**BASE_ENV, "MQTT_RAW_TOPIC": "neurio/raw"})
        with self.assertRaises(ConfigError):
            Config.from_env({**BASE_ENV, "MQTT_RAW_TOPIC": "neurio/#"})
        cfg = Config.from_env({**BASE_ENV, "MQTT_RAW_TOPIC": ""})
        self.assertEqual(cfg.mqtt_hourly_topic, "neurio/hourly/energy")
        cfg = Config.from_env({**BASE_ENV, "MQTT_RAW_TOPIC": "   "})
        self.assertEqual(cfg.source_mode, "mqtt")

    def test_wildcard_hourly_topics_rejected(self):
        with self.assertRaises(ConfigError):
            Config.from_env({**BASE_ENV, "MQTT_HOURLY_TOPIC": "neurio/+/energy"})
        with self.assertRaises(ConfigError):
            support.make_config(mqtt_ack_topic="neurio/hourly/+")

    def test_hourly_stale_threshold(self):
        cfg = Config.from_env(BASE_ENV)
        self.assertEqual(cfg.hourly_stale_after_seconds, 3900)
        overridden = Config.from_env({**BASE_ENV, "NEURIO_HOURLY_STALE_AFTER_SECONDS": "7200"})
        self.assertEqual(overridden.hourly_stale_after_seconds, 7200)
        for raw in ("0", "-5", "nope"):
            with self.assertRaises(ConfigError):
                Config.from_env({**BASE_ENV, "NEURIO_HOURLY_STALE_AFTER_SECONDS": raw})

    def test_username_without_password_rejected(self):
        with self.assertRaises(ConfigError):
            Config.from_env({**BASE_ENV, "MQTT_USERNAME": "bridge"})
        with self.assertRaises(ConfigError):
            Config.from_env({**BASE_ENV, "MQTT_PASSWORD": "synthetic-password"})

    def test_broker_host_must_not_be_a_url(self):
        with self.assertRaises(ConfigError):
            Config.from_env({**BASE_ENV, "MQTT_BROKER_HOST": "mqtt://192.168.1.10"})

    def test_missing_broker_host_rejected(self):
        env = {key: value for key, value in BASE_ENV.items() if key != "MQTT_BROKER_HOST"}
        with self.assertRaises(ConfigError):
            Config.from_env(env)


class FakeReason:
    def __init__(self, code=0):
        self._code = code

    @property
    def is_failure(self):
        return self._code != 0

    def __int__(self):
        return self._code

    def __str__(self):
        return f"FakeReason({self._code})"


class FakePublishInfo:
    def __init__(self, rc):
        self.rc = rc


class FakePahoMessage:
    def __init__(self, topic, payload, qos=0, retain=False):
        self.topic = topic
        self.payload = payload
        self.qos = qos
        self.retain = retain


class FakePahoClient:
    connect_reason = FakeReason(0)
    connect_exception = None
    subscribe_codes = [0]
    publish_rc = 0

    def __init__(self, **kwargs):
        self.kwargs = kwargs
        self.auth = None
        self.tls_called = False
        self.connect_args = None
        self.subscribed = None
        self.published = []
        self.loop_started = False
        self.disconnected = False

    @classmethod
    def reset(cls):
        cls.connect_reason = FakeReason(0)
        cls.connect_exception = None
        cls.subscribe_codes = [0]
        cls.publish_rc = 0

    def username_pw_set(self, username, password=None):
        self.auth = (username, password)

    def tls_set(self, **kwargs):
        self.tls_called = True

    def connect(self, host, port, keepalive=60):
        if type(self).connect_exception is not None:
            raise type(self).connect_exception
        self.connect_args = (host, port, keepalive)
        self.on_connect(self, None, {}, type(self).connect_reason, None)
        return 0

    def loop_start(self):
        self.loop_started = True

    def loop_stop(self):
        self.loop_started = False

    def disconnect(self):
        self.disconnected = True

    def subscribe(self, topics):
        self.subscribed = topics
        mid = 7
        self.on_subscribe(self, None, mid, list(type(self).subscribe_codes), None)
        return 0, mid

    def publish(self, topic, payload=None, qos=0):
        self.published.append((topic, payload, qos))
        return FakePublishInfo(type(self).publish_rc)


class FakePahoModule:
    Client = FakePahoClient
    MQTTv311 = 4
    MQTT_ERR_SUCCESS = 0

    class CallbackAPIVersion:
        VERSION2 = "VERSION2"


def adapter_client(**overrides):
    data = dict(
        host="192.168.1.10",
        port=1883,
        client_id="test-client",
        timeout_seconds=1,
        paho_module=FakePahoModule(),
    )
    data.update(overrides)
    return MqttClient(**data)


class MqttAdapterTests(unittest.TestCase):
    def setUp(self):
        FakePahoClient.reset()

    def test_connect_uses_a_persistent_session(self):
        client = adapter_client(username="user", password="pass", tls=True)
        client.connect()
        fake = client._client
        self.assertEqual(fake.kwargs["client_id"], "test-client")
        self.assertEqual(fake.kwargs["clean_session"], False)
        self.assertEqual(fake.kwargs["reconnect_on_failure"], False)
        self.assertEqual(fake.auth, ("user", "pass"))
        self.assertTrue(fake.tls_called)
        self.assertEqual(fake.connect_args, ("192.168.1.10", 1883, 60))
        self.assertTrue(fake.loop_started)
        client.close()
        self.assertTrue(fake.disconnected)

    def test_connect_refused_raises(self):
        FakePahoClient.connect_reason = FakeReason(5)
        with self.assertRaises(MqttError):
            adapter_client().connect()

    def test_connect_socket_error_raises(self):
        FakePahoClient.connect_exception = ConnectionRefusedError("refused")
        with self.assertRaises(MqttError):
            adapter_client().connect()

    def test_message_flows_to_poll(self):
        client = adapter_client()
        client.connect()
        fake = client._client
        fake.on_message(fake, None, FakePahoMessage("neurio/hourly/energy", b"{}", qos=1))
        message = client.poll(0.5)
        self.assertEqual(message.topic, "neurio/hourly/energy")
        self.assertEqual(message.payload, b"{}")
        self.assertIsNone(client.poll(0.05))
        client.close()

    def test_oversize_payload_is_dropped(self):
        client = adapter_client(max_message_bytes=4)
        client.connect()
        fake = client._client
        fake.on_message(fake, None, FakePahoMessage("neurio/hourly/energy", b"12345"))
        self.assertIsNone(client.poll(0.05))
        self.assertEqual(client.dropped_oversize, 1)
        client.close()

    def test_disconnect_makes_poll_raise(self):
        client = adapter_client()
        client.connect()
        fake = client._client
        fake.on_disconnect(fake, None, None, FakeReason(1), None)
        with self.assertRaises(MqttError):
            client.poll(0.05)
        client.close()

    def test_subscribe_and_refusal(self):
        client = adapter_client()
        client.connect()
        client.subscribe([("neurio/hourly/energy", 1)])
        self.assertEqual(client._client.subscribed, [("neurio/hourly/energy", 1)])
        client.close()
        FakePahoClient.subscribe_codes = [128]
        refused = adapter_client()
        refused.connect()
        with self.assertRaises(MqttError):
            refused.subscribe([("neurio/hourly/energy", 1)])
        refused.close()

    def test_publish_delegates_and_failure_raises(self):
        client = adapter_client()
        client.connect()
        client.publish("neurio/hourly/ack", b"1730970000")
        self.assertEqual(client._client.published, [("neurio/hourly/ack", b"1730970000", 0)])
        client.close()
        FakePahoClient.publish_rc = 4
        failing = adapter_client()
        failing.connect()
        with self.assertRaises(MqttError):
            failing.publish("neurio/hourly/ack", b"1")
        failing.close()


class HourlyIngestTests(unittest.TestCase):
    def test_valid_record_stores_millikwh(self):
        db = support.open_db(self)
        result = store_hourly_record(db, hourly_body(), RECEIVED, support.make_config())
        self.assertEqual(result.outcome, "stored")
        self.assertEqual(result.record_id, HOURLY_ID)
        row = db.conn.execute(
            "SELECT * FROM hourly_records WHERE record_id=?", (HOURLY_ID,)
        ).fetchone()
        self.assertEqual(row["net_import_millikwh"], 1234567)
        self.assertEqual(row["net_export_millikwh"], 89100)
        self.assertEqual(row["net_delta_net_millikwh"], 500)
        self.assertEqual(row["generation_delta_export_millikwh"], 300)
        self.assertEqual(row["consumption_delta_import_millikwh"], 800)
        self.assertEqual(row["phase_a_delta_import_millikwh"], 450)
        self.assertEqual(row["phase_b_delta_import_millikwh"], 350)
        self.assertEqual(row["retries"], 1)
        self.assertEqual(row["device_timestamp_raw"], "2026-11-06T17:00:00Z")
        self.assertEqual(row["device_timestamp_utc"], "2026-11-06T17:00:00.000Z")
        self.assertIsNone(row["acked_at_utc"])

    def test_redelivery_is_duplicate_and_ack_marks_once(self):
        db = support.open_db(self)
        cfg = support.make_config()
        first = store_hourly_record(db, hourly_body(), RECEIVED, cfg)
        second = store_hourly_record(db, hourly_body(), RECEIVED, cfg)
        self.assertEqual(first.outcome, "stored")
        self.assertEqual(second.outcome, "duplicate")
        self.assertEqual(second.record_id, HOURLY_ID)
        count = db.conn.execute("SELECT COUNT(*) AS n FROM hourly_records").fetchone()["n"]
        self.assertEqual(count, 1)
        mark_hourly_acked(db, HOURLY_ID, RECEIVED)
        row = db.conn.execute(
            "SELECT acked_at_utc FROM hourly_records WHERE record_id=?", (HOURLY_ID,)
        ).fetchone()
        self.assertEqual(row["acked_at_utc"], "2026-11-06T18:00:05.000Z")
        mark_hourly_acked(db, HOURLY_ID, parse_instant("2026-11-06T19:00:05Z"))
        row = db.conn.execute(
            "SELECT acked_at_utc FROM hourly_records WHERE record_id=?", (HOURLY_ID,)
        ).fetchone()
        self.assertEqual(row["acked_at_utc"], "2026-11-06T18:00:05.000Z")

    def test_parse_errors_have_no_record_id(self):
        db = support.open_db(self)
        cfg = support.make_config()
        for body in (
            b"not json",
            json.dumps({"timestamp": "2026-11-06T17:00:00Z"}).encode(),
            json.dumps({"id": True, "net": {"delta_net_kwh": 0.5}}).encode(),
            json.dumps({"id": HOURLY_ID, "net": {"delta_net_kwh": "lots"}}).encode(),
            json.dumps({"id": 4294967296, "net": {"delta_net_kwh": 0.5}}).encode(),
        ):
            result = store_hourly_record(db, body, RECEIVED, cfg)
            self.assertEqual(result.outcome, "parse_error", body)
            self.assertIsNone(result.record_id)
        count = db.conn.execute("SELECT COUNT(*) AS n FROM hourly_records").fetchone()["n"]
        self.assertEqual(count, 0)

    def test_optional_fields_and_lenient_timestamp(self):
        db = support.open_db(self)
        body = hourly_body(
            timestamp="2026-11-06 17:00:00",
            net={
                "import_kwh": "lots",
                "export_kwh": 89.1,
                "delta_import_kwh": 0.75,
                "delta_export_kwh": 0.25,
                "delta_net_kwh": -0.5,
            },
        )
        result = store_hourly_record(db, body, RECEIVED, support.make_config())
        self.assertEqual(result.outcome, "stored")
        row = db.conn.execute(
            "SELECT * FROM hourly_records WHERE record_id=?", (HOURLY_ID,)
        ).fetchone()
        self.assertIsNone(row["net_import_millikwh"])
        self.assertEqual(row["net_delta_net_millikwh"], -500)
        self.assertEqual(row["device_timestamp_raw"], "2026-11-06 17:00:00")
        self.assertIsNone(row["device_timestamp_utc"])


class FakeMqttClient:
    def __init__(self, messages):
        self._messages = list(messages)
        self.published = []
        self.subscriptions = None
        self.connected = False
        self.closed = False

    def connect(self):
        self.connected = True

    def subscribe(self, topics):
        self.subscriptions = topics

    def poll(self, timeout):
        if self._messages:
            return self._messages.pop(0)
        time.sleep(min(timeout, 0.05))
        return None

    def publish(self, topic, payload):
        self.published.append((topic, payload))

    def close(self):
        self.closed = True


def wait_for(testcase, predicate):
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.02)
    testcase.fail("timed out")


class MqttPollerTests(unittest.TestCase):
    def test_poller_stores_hourly_then_acks(self):
        db = support.open_db(self)
        cfg = support.make_config()
        fake = FakeMqttClient(
            [MqttMessage(topic="neurio/hourly/energy", payload=hourly_body(), qos=1)]
        )
        poller = Poller(cfg, db, mqtt_client_factory=lambda: fake)
        poller.start()
        try:
            wait_for(
                self,
                lambda: db.conn.execute(
                    "SELECT acked_at_utc FROM hourly_records "
                    "WHERE record_id=? AND acked_at_utc IS NOT NULL",
                    (HOURLY_ID,),
                ).fetchone()
                is not None,
            )
        finally:
            poller.stop()
        self.assertEqual(fake.subscriptions, [("neurio/hourly/energy", 1)])
        self.assertEqual(fake.published, [("neurio/hourly/ack", str(HOURLY_ID).encode())])
        self.assertTrue(fake.closed)
        snap = poller.snapshot()
        self.assertEqual(snap["mqtt_hourly_messages"], 1)
        self.assertIsNotNone(snap["last_hourly_message_at"])
        self.assertNotIn("mqtt_raw_messages", snap)
        names = {
            row[0]
            for row in db.conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
        }
        self.assertNotIn("poll_attempts", names)
        self.assertNotIn("samples", names)

    def test_ack_mark_failure_keeps_connection_and_is_visible(self):
        db = support.open_db(self)
        cfg = support.make_config()
        fake = FakeMqttClient(
            [MqttMessage(topic="neurio/hourly/energy", payload=hourly_body(), qos=1)]
        )
        factory_calls = []

        def factory():
            factory_calls.append(1)
            return fake

        def broken_mark(db_, record_id, when):
            raise RuntimeError("database is locked")

        poller = Poller(cfg, db, mqtt_client_factory=factory)
        with unittest.mock.patch.object(
            poller_module, "mark_hourly_acked", broken_mark
        ):
            poller.start()
            try:
                wait_for(self, lambda: poller.snapshot()["ack_mark_failures"] == 1)
            finally:
                poller.stop()
        # The ACK was still published — the bridge dropped the record.
        self.assertEqual(
            fake.published, [("neurio/hourly/ack", str(HOURLY_ID).encode())]
        )
        # No reconnect loop: the factory ran once for the whole session.
        self.assertEqual(len(factory_calls), 1)
        # The record is intact, stored but unmarked.
        row = db.conn.execute(
            "SELECT acked_at_utc FROM hourly_records WHERE record_id=?",
            (HOURLY_ID,),
        ).fetchone()
        self.assertIsNotNone(row)
        self.assertIsNone(row["acked_at_utc"])
        snap = poller.snapshot()
        self.assertEqual(snap["last_ack_mark_error"], "RuntimeError")
        self.assertIsNotNone(snap["last_ack_mark_error_at"])
        self.assertEqual(snap["last_outcome"], "success")

    def test_poller_never_acks_a_bad_hourly_payload(self):
        db = support.open_db(self)
        cfg = support.make_config()
        fake = FakeMqttClient(
            [MqttMessage(topic="neurio/hourly/energy", payload=b"junk", qos=1)]
        )
        poller = Poller(cfg, db, mqtt_client_factory=lambda: fake)
        poller.start()
        try:
            wait_for(self, lambda: poller.snapshot().get("last_outcome") == "parse_error")
        finally:
            poller.stop()
        self.assertEqual(fake.published, [])
        count = db.conn.execute("SELECT COUNT(*) AS n FROM hourly_records").fetchone()["n"]
        self.assertEqual(count, 0)

    def test_backoff_caps_at_the_configured_maximum(self):
        db = support.open_db(self)
        cfg = support.make_config(poll_jitter_ratio=0, poll_interval_seconds=15)
        poller = Poller(cfg, db, mqtt_client_factory=lambda: FakeMqttClient([]))
        self.assertEqual(poller._backoff(1), 15)
        self.assertEqual(poller._backoff(2), 30)
        self.assertEqual(poller._backoff(20), 300)


class HourlyApiTests(unittest.TestCase):
    def _seed(self, db):
        cfg = support.make_config()
        first = store_hourly_record(db, hourly_body(), RECEIVED, cfg)
        second_id = HOURLY_ID + 3600
        body = json.loads(hourly_body())
        body["id"] = second_id
        body["net"]["delta_net_kwh"] = -0.25
        second = store_hourly_record(db, json.dumps(body).encode(), RECEIVED, cfg)
        self.assertEqual(first.outcome, "stored")
        self.assertEqual(second.outcome, "stored")
        mark_hourly_acked(db, HOURLY_ID, RECEIVED)
        return second_id

    def test_status_summarizes_hourly_records_without_raw_counts(self):
        db = support.open_db(self)
        second_id = self._seed(db)
        status, body, _ = dispatch("/api/v1/status", support.make_config(), db, None)
        self.assertEqual(status, 200)
        self.assertEqual(body["counts"], {"hourly_records": 2})
        self.assertNotIn("samples", body["counts"])
        self.assertNotIn("spans", body["counts"])
        self.assertNotIn("channels", body["counts"])
        summary = body["hourly_records"]
        self.assertEqual(summary["stored"], 2)
        self.assertEqual(summary["acked"], 1)
        self.assertEqual(summary["last_record_id"], second_id)
        relay = body["mqtt_relay"]
        self.assertEqual(relay["hourly_topic"], "neurio/hourly/energy")
        self.assertEqual(relay["ack_topic"], "neurio/hourly/ack")
        self.assertNotIn("raw_topic", relay)
        self.assertNotIn("raw_messages", relay)
        self.assertNotIn("ha_relay", body)
        self.assertNotIn("last_poll", body)
        for gone in (
            "/api/v1/energy",
            "/api/v1/samples",
            "/api/v1/spans",
            "/api/v1/latest",
            "/api/v1/export/samples.csv",
        ):
            code, _, _ = dispatch(gone, support.make_config(), db, None)
            self.assertEqual(code, 404, gone)

    def test_hourly_endpoint_keeps_its_record_shape(self):
        db = support.open_db(self)
        second_id = self._seed(db)
        status, body, _ = dispatch("/api/v1/hourly", support.make_config(), db, None)
        self.assertEqual(status, 200)
        records = body["hourly_records"]
        self.assertEqual([r["record_id"] for r in records], [HOURLY_ID, second_id])
        self.assertEqual(tuple(records[0]), HOURLY_RECORD_KEYS)
        self.assertEqual(records[0]["net_delta_net_kwh"], "0.5")
        self.assertEqual(records[0]["net_import_kwh"], "1234.567")
        self.assertEqual(records[1]["net_delta_net_kwh"], "-0.25")
        self.assertIsNotNone(records[0]["acked_at_utc"])
        self.assertIsNone(records[1]["acked_at_utc"])
        self.assertIsNone(body["next_cursor"])

    def test_hourly_endpoint_paginates_with_an_exclusive_cursor(self):
        db = support.open_db(self)
        cfg = support.make_config()
        second_id = self._seed(db)
        third_id = HOURLY_ID + 7200
        body = json.loads(hourly_body())
        body["id"] = third_id
        store_hourly_record(db, json.dumps(body).encode(), RECEIVED, cfg)
        status, page, _ = dispatch("/api/v1/hourly?limit=168", support.make_config(), db, None)
        self.assertEqual(status, 200)
        self.assertEqual(len(page["hourly_records"]), 3)
        self.assertIsNone(page["next_cursor"])
        status, page1, _ = dispatch("/api/v1/hourly?limit=1", support.make_config(), db, None)
        self.assertEqual(page1["hourly_records"][0]["record_id"], HOURLY_ID)
        self.assertEqual(page1["next_cursor"], str(HOURLY_ID))
        status, page2, _ = dispatch(
            f"/api/v1/hourly?limit=168&cursor={HOURLY_ID}", support.make_config(), db, None
        )
        self.assertEqual(
            [r["record_id"] for r in page2["hourly_records"]], [second_id, third_id]
        )
        self.assertIsNone(page2["next_cursor"])
        code, rejected, _ = dispatch("/api/v1/hourly?limit=1001", support.make_config(), db, None)
        self.assertEqual(code, 400)
        self.assertIn("limit", rejected["error"])
        code, ok, _ = dispatch("/api/v1/hourly?limit=1000", support.make_config(), db, None)
        self.assertEqual(code, 200)
        self.assertEqual(len(ok["hourly_records"]), 3)

    def test_channels_returns_the_sensor_id_in_the_historical_object(self):
        db = support.open_db(self)
        db.conn.execute(
            "INSERT INTO sensors(sensor_id, sensor_token, first_seen_utc, last_seen_utc) "
            "VALUES(?, ?, ?, ?)",
            (
                "ha-relay:sensor.neurio_raw",
                "abc123token",
                "2026-11-01T00:00:00.000Z",
                "2026-11-10T00:00:00.000Z",
            ),
        )
        status, body, _ = dispatch("/api/v1/channels", support.make_config(), db, None)
        self.assertEqual(status, 200)
        self.assertEqual(list(body), ["channels"])
        self.assertEqual(len(body["channels"]), 1)
        item = body["channels"][0]
        self.assertEqual(
            list(item),
            [
                "identity_key",
                "type",
                "ch",
                "first_seen_utc",
                "last_seen_utc",
                "reading_notice",
                "label",
                "sensor_id",
            ],
        )
        self.assertEqual(item["sensor_id"], "ha-relay:sensor.neurio_raw")
        self.assertNotIn("sensor_token", item)
        self.assertIsNone(item["type"])
        self.assertIsNone(item["ch"])
        self.assertIsNone(item["identity_key"])
        self.assertEqual(item["reading_notice"], "device_collector")
        self.assertEqual(item["first_seen_utc"], "2026-11-01T00:00:00.000Z")

    def test_channels_response_contains_no_token(self):
        db = support.open_db(self)
        token = "secret-sensor-token-value"
        db.conn.execute(
            "INSERT INTO sensors(sensor_id, sensor_token, first_seen_utc, last_seen_utc) "
            "VALUES(?, ?, ?, ?)",
            (
                "ha-relay:sensor.neurio_raw",
                token,
                "2026-11-01T00:00:00.000Z",
                "2026-11-10T00:00:00.000Z",
            ),
        )
        status, body, _ = dispatch("/api/v1/channels", support.make_config(), db, None)
        self.assertEqual(status, 200)
        encoded = json.dumps(body)
        self.assertNotIn(token, encoded)
        self.assertNotIn("sensor_token", encoded)
        self.assertEqual(body["channels"][0]["sensor_id"], "ha-relay:sensor.neurio_raw")
        for item in body["channels"]:
            for key, value in item.items():
                self.assertNotIn("token", key.lower())
                if isinstance(value, str):
                    self.assertNotIn(token, value)

    def test_hourly_empty_database(self):
        db = support.open_db(self)
        status, body, _ = dispatch("/api/v1/hourly", support.make_config(), db, None)
        self.assertEqual(status, 200)
        self.assertEqual(body["hourly_records"], [])
        status, body, _ = dispatch("/api/v1/status", support.make_config(), db, None)
        self.assertEqual(body["counts"]["hourly_records"], 0)
        self.assertEqual(body["hourly_records"]["stored"], 0)
        self.assertEqual(body["hourly_records"]["acked"], 0)
        self.assertIsNone(body["hourly_records"]["last_record_id"])
        status, channels, _ = dispatch("/api/v1/channels", support.make_config(), db, None)
        self.assertEqual(channels, {"channels": []})


FRESH_NOW = parse_instant("2026-11-06T18:00:00Z")


class _HourlyClock:
    def __init__(self, last_hourly_message_at):
        self._at = last_hourly_message_at

    def snapshot(self):
        return {"last_hourly_message_at": self._at, "storage_writable": True}


def _seconds_ago(seconds: float):
    return FRESH_NOW - timedelta(seconds=seconds)


def _stamp(seconds: float) -> str:
    return format_utc(_seconds_ago(seconds))


def _store_hourly(db, seconds_ago: float, record_id: int = HOURLY_ID) -> None:
    result = store_hourly_record(
        db, hourly_body(id=record_id), _seconds_ago(seconds_ago), support.make_config()
    )
    if result.outcome != "stored":
        raise AssertionError(result.outcome)


class ComposeFileTests(unittest.TestCase):
    def test_mqtt_compose_keeps_the_production_env_keys(self):
        text = (support.ROOT / "compose.mqtt.yml").read_text(encoding="utf-8")
        self.assertIn('SOURCE_MODE: "mqtt"', text)
        self.assertIn('MQTT_BROKER_HOST: "${MQTT_BROKER_HOST:-192.168.86.7}"', text)
        self.assertIn('MQTT_USERNAME: "${MQTT_USERNAME:?set MQTT_USERNAME}"', text)
        self.assertIn('MQTT_PASSWORD_FILE: "/run/secrets/mqtt_password"', text)
        self.assertIn("${PUBLISH_ADDRESS:-192.168.86.20}:8095:8095", text)
        self.assertIn("${DATA_HOST_PATH:-/srv/neurio-recorder/data}:/data", text)
        self.assertIn("${BACKUP_HOST_PATH:-/srv/neurio-recorder/backups}:/backups", text)
        self.assertIn('MQTT_HOURLY_TOPIC: "neurio/hourly/energy"', text)
        self.assertIn('MQTT_ACK_TOPIC: "neurio/hourly/ack"', text)
        self.assertIn("MQTT_PASSWORD_HOST_FILE", text)
        self.assertNotIn("MQTT_RAW_TOPIC", text)
        self.assertNotIn("ha_token", text)
        self.assertNotIn("HA_", text)
        self.assertNotIn("NEURIO_URL", text)
        self.assertNotIn("NEURIO_RAW_RETENTION", text)
        self.assertFalse((support.ROOT / "compose.yml").exists())
        for line in text.splitlines():
            self.assertFalse(line.strip().startswith("MQTT_PASSWORD:"))


class HourlyFreshnessTests(unittest.TestCase):
    def test_stays_fresh_past_an_hour_until_the_hourly_threshold(self):
        db = support.open_db(self)
        _store_hourly(db, 120)
        status, body, _ = dispatch(
            "/api/v1/status", support.make_config(), db, None, now=FRESH_NOW
        )
        self.assertEqual(status, 200)
        self.assertTrue(body["device_fresh"])
        self.assertAlmostEqual(body["device_age_seconds"], 120, places=3)
        self.assertEqual(body["staleness_threshold_seconds"], 3900)

        db = support.open_db(self)
        _store_hourly(db, 3900)
        _, body, _ = dispatch("/api/v1/status", support.make_config(), db, None, now=FRESH_NOW)
        self.assertTrue(body["device_fresh"])

        db = support.open_db(self)
        _store_hourly(db, 3901)
        _, body, _ = dispatch("/api/v1/status", support.make_config(), db, None, now=FRESH_NOW)
        self.assertFalse(body["device_fresh"])
        self.assertAlmostEqual(body["device_age_seconds"], 3901, places=3)
        code, ready, _ = dispatch("/readyz", support.make_config(), db, None, now=FRESH_NOW)
        self.assertEqual(code, 200)
        self.assertTrue(ready["ready"])
        self.assertFalse(ready["device_fresh"])

    def test_without_an_hourly_message_is_not_fresh(self):
        db = support.open_db(self)
        _, body, _ = dispatch("/api/v1/status", support.make_config(), db, None, now=FRESH_NOW)
        self.assertFalse(body["device_fresh"])
        self.assertIsNone(body["device_age_seconds"])

    def test_poller_time_wins_and_restart_falls_back_to_the_row(self):
        db = support.open_db(self)
        _store_hourly(db, 4000)
        _, body, _ = dispatch(
            "/api/v1/status", support.make_config(), db, _HourlyClock(_stamp(30)), now=FRESH_NOW
        )
        self.assertTrue(body["device_fresh"])
        self.assertAlmostEqual(body["device_age_seconds"], 30, places=3)

        _, body, _ = dispatch(
            "/api/v1/status", support.make_config(), db, _HourlyClock(None), now=FRESH_NOW
        )
        self.assertFalse(body["device_fresh"])
        self.assertAlmostEqual(body["device_age_seconds"], 4000, places=3)

        db = support.open_db(self)
        _store_hourly(db, 200)
        _, body, _ = dispatch(
            "/api/v1/status", support.make_config(), db, _HourlyClock(None), now=FRESH_NOW
        )
        self.assertTrue(body["device_fresh"])
        self.assertAlmostEqual(body["device_age_seconds"], 200, places=3)

    def test_unwritable_storage_fails_readiness(self):
        db = support.open_db(self)

        class Full:
            def snapshot(self):
                return {"storage_writable": False, "storage_error": "disk_full"}

        code, ready, _ = dispatch("/readyz", support.make_config(), db, Full(), now=FRESH_NOW)
        self.assertEqual(code, 503)
        self.assertFalse(ready["ready"])
        self.assertFalse(ready["storage_writable"])


if __name__ == "__main__":
    unittest.main()
