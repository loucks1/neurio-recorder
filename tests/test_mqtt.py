"""Synthetic MQTT bridge payloads and a fake broker client. No real broker, no Neurio device."""

import json
import threading
import time
import unittest

import support
from neurio_collector.config import Config, ConfigError
from neurio_collector.ingest import ingest_body
from neurio_collector.mqtt_client import MqttClient, MqttError, MqttMessage
from neurio_collector.mqtt_ingest import mark_hourly_acked, store_hourly_record
from neurio_collector.poller import Poller
from neurio_collector.timeutil import parse_instant

RECEIVED = parse_instant("2026-11-06T18:00:05Z")
HOURLY_ID = int(parse_instant("2026-11-06T17:00:00Z").timestamp())

BASE_ENV = {
    "SOURCE_MODE": "mqtt",
    "BIND_ADDRESS": "127.0.0.1",
    "BIND_PORT": "8095",
    "DATA_DIR": "/tmp/neurio-test",
    "MQTT_BROKER_HOST": "192.168.1.10",
}


def mqtt_config(**overrides):
    data = dict(
        source_mode="mqtt",
        neurio_url="",
        mqtt_broker_host="192.168.1.10",
    )
    data.update(overrides)
    return support.make_config(**data)


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


class MqttConfigTests(unittest.TestCase):
    def test_from_env_happy_path(self):
        cfg = Config.from_env(BASE_ENV)
        self.assertEqual(cfg.source_mode, "mqtt")
        self.assertEqual(cfg.mqtt_broker_host, "192.168.1.10")
        self.assertEqual(cfg.mqtt_broker_port, 1883)
        self.assertEqual(cfg.mqtt_raw_topic, "neurio/raw")
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

    def test_missing_broker_host_rejected(self):
        env = {key: value for key, value in BASE_ENV.items() if key != "MQTT_BROKER_HOST"}
        with self.assertRaises(ConfigError):
            Config.from_env(env)
        with self.assertRaises(ConfigError):
            mqtt_config(mqtt_broker_host=None)

    def test_neurio_url_rejected_in_mqtt_mode(self):
        with self.assertRaises(ConfigError) as caught:
            Config.from_env({**BASE_ENV, "NEURIO_URL": "http://192.168.1.50/current-sample"})
        self.assertIn("NEURIO_URL", str(caught.exception))

    def test_wildcard_topics_rejected(self):
        with self.assertRaises(ConfigError):
            Config.from_env({**BASE_ENV, "MQTT_RAW_TOPIC": "neurio/#"})
        with self.assertRaises(ConfigError):
            Config.from_env({**BASE_ENV, "MQTT_HOURLY_TOPIC": "neurio/+/energy"})
        with self.assertRaises(ConfigError):
            mqtt_config(mqtt_ack_topic="neurio/hourly/+")

    def test_username_without_password_rejected(self):
        with self.assertRaises(ConfigError):
            Config.from_env({**BASE_ENV, "MQTT_USERNAME": "bridge"})
        with self.assertRaises(ConfigError):
            Config.from_env({**BASE_ENV, "MQTT_PASSWORD": "synthetic-password"})

    def test_ha_token_rejected_in_mqtt_mode(self):
        with self.assertRaises(ConfigError):
            Config.from_env({**BASE_ENV, "HA_TOKEN": "synthetic"})

    def test_broker_host_must_not_be_a_url(self):
        with self.assertRaises(ConfigError):
            Config.from_env({**BASE_ENV, "MQTT_BROKER_HOST": "mqtt://192.168.1.10"})


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
    """Stands in for paho's Client. Callbacks fire synchronously."""

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

    def test_connect_configures_paho_client(self):
        client = adapter_client(username="user", password="pass", tls=True)
        client.connect()
        fake = client._client
        self.assertEqual(fake.kwargs["client_id"], "test-client")
        self.assertEqual(fake.kwargs["clean_session"], False)
        self.assertEqual(fake.kwargs["reconnect_on_failure"], False)
        self.assertEqual(fake.kwargs["callback_api_version"], "VERSION2")
        self.assertEqual(fake.auth, ("user", "pass"))
        self.assertTrue(fake.tls_called)
        self.assertEqual(fake.connect_args, ("192.168.1.10", 1883, 60))
        self.assertTrue(fake.loop_started)
        client.close()
        self.assertTrue(fake.disconnected)

    def test_connect_refused_raises(self):
        FakePahoClient.connect_reason = FakeReason(5)
        client = adapter_client()
        with self.assertRaises(MqttError):
            client.connect()

    def test_connect_socket_error_raises(self):
        FakePahoClient.connect_exception = ConnectionRefusedError("refused")
        client = adapter_client()
        with self.assertRaises(MqttError):
            client.connect()

    def test_message_flows_to_poll(self):
        client = adapter_client()
        client.connect()
        fake = client._client
        fake.on_message(fake, None, FakePahoMessage("neurio/raw", b'{"channels":[]}', qos=0))
        message = client.poll(0.5)
        self.assertEqual(message.topic, "neurio/raw")
        self.assertEqual(message.payload, b'{"channels":[]}')
        self.assertIsNone(client.poll(0.05))
        client.close()

    def test_oversize_payload_is_dropped(self):
        client = adapter_client(max_message_bytes=4)
        client.connect()
        fake = client._client
        fake.on_message(fake, None, FakePahoMessage("neurio/raw", b"12345"))
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
        client.subscribe([("neurio/raw", 0), ("neurio/hourly/energy", 1)])
        self.assertEqual(
            client._client.subscribed, [("neurio/raw", 0), ("neurio/hourly/energy", 1)]
        )
        client.close()
        FakePahoClient.subscribe_codes = [128]
        refused = adapter_client()
        refused.connect()
        with self.assertRaises(MqttError):
            refused.subscribe([("neurio/raw", 0)])
        refused.close()

    def test_publish_delegates_and_failure_raises(self):
        client = adapter_client()
        client.connect()
        client.publish("neurio/hourly/ack", b"1730970000")
        self.assertEqual(
            client._client.published, [("neurio/hourly/ack", b"1730970000", 0)]
        )
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
        result = store_hourly_record(db, hourly_body(), RECEIVED, mqtt_config())
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
        cfg = mqtt_config()
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
        cfg = mqtt_config()
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
        result = store_hourly_record(db, body, RECEIVED, mqtt_config())
        self.assertEqual(result.outcome, "stored")
        row = db.conn.execute(
            "SELECT * FROM hourly_records WHERE record_id=?", (HOURLY_ID,)
        ).fetchone()
        self.assertIsNone(row["net_import_millikwh"])
        self.assertEqual(row["net_delta_net_millikwh"], -500)
        self.assertEqual(row["device_timestamp_raw"], "2026-11-06 17:00:00")
        self.assertIsNone(row["device_timestamp_utc"])


class RawIngestTests(unittest.TestCase):
    def test_mqtt_bridge_source_kind(self):
        db = support.open_db(self)
        cfg = mqtt_config()
        body = support.sample_bytes(
            support.SYNTHETIC_SENSOR_ID,
            "2026-11-06T17:59:00Z",
            [support.channel("NET", 3, 1000, 0, 10)],
        )
        result = ingest_body(db, body, RECEIVED, cfg, source_kind="mqtt_bridge")
        self.assertEqual(result.outcome, "stored")
        row = db.conn.execute(
            "SELECT source_kind FROM samples WHERE id=?", (result.sample_id,)
        ).fetchone()
        self.assertEqual(row["source_kind"], "mqtt_bridge")

    def test_default_source_kind_stays_device(self):
        db = support.open_db(self)
        cfg = support.make_config()
        body = support.sample_bytes(
            support.SYNTHETIC_SENSOR_ID,
            "2026-11-06T17:59:00Z",
            [support.channel("NET", 3, 1000, 0, 10)],
        )
        result = ingest_body(db, body, RECEIVED, cfg)
        self.assertEqual(result.outcome, "stored")
        row = db.conn.execute(
            "SELECT source_kind FROM samples WHERE id=?", (result.sample_id,)
        ).fetchone()
        self.assertEqual(row["source_kind"], "device")


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


def wait_for(testcase, db, sql, params=()):
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        row = db.conn.execute(sql, params).fetchone()
        if row is not None:
            return row
        time.sleep(0.02)
    testcase.fail(f"timed out waiting for: {sql}")


class MqttPollerTests(unittest.TestCase):
    def test_poller_stores_raw_and_hourly_then_acks(self):
        db = support.open_db(self)
        cfg = mqtt_config()
        raw = support.sample_bytes(
            support.SYNTHETIC_SENSOR_ID,
            "2026-11-06T17:59:00Z",
            [support.channel("NET", 3, 1000, 0, 10)],
        )
        fake = FakeMqttClient(
            [
                MqttMessage(topic="neurio/raw", payload=raw, qos=0),
                MqttMessage(topic="neurio/hourly/energy", payload=hourly_body(), qos=1),
            ]
        )
        poller = Poller(cfg, db, mqtt_client_factory=lambda: fake)
        poller.start()
        try:
            row = wait_for(
                self,
                db,
                "SELECT acked_at_utc FROM hourly_records "
                "WHERE record_id=? AND acked_at_utc IS NOT NULL",
                (HOURLY_ID,),
            )
            self.assertIsNotNone(row["acked_at_utc"])
        finally:
            poller.stop()
        self.assertEqual(
            fake.subscriptions, [("neurio/raw", 0), ("neurio/hourly/energy", 1)]
        )
        self.assertEqual(
            fake.published, [("neurio/hourly/ack", str(HOURLY_ID).encode())]
        )
        sample = db.conn.execute("SELECT source_kind FROM samples").fetchone()
        self.assertEqual(sample["source_kind"], "mqtt_bridge")
        self.assertTrue(fake.closed)

    def test_poller_never_acks_a_bad_hourly_payload(self):
        db = support.open_db(self)
        cfg = mqtt_config()
        fake = FakeMqttClient(
            [MqttMessage(topic="neurio/hourly/energy", payload=b"junk", qos=1)]
        )
        poller = Poller(cfg, db, mqtt_client_factory=lambda: fake)
        poller.start()
        try:
            wait_for(
                self,
                db,
                "SELECT id FROM poll_attempts WHERE outcome='parse_error'",
            )
        finally:
            poller.stop()
        self.assertEqual(fake.published, [])
        count = db.conn.execute("SELECT COUNT(*) AS n FROM hourly_records").fetchone()["n"]
        self.assertEqual(count, 0)


if __name__ == "__main__":
    unittest.main()
