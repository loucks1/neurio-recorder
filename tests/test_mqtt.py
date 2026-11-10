"""Synthetic MQTT bridge payloads and a fake broker client. No real broker, no Neurio device."""

import json
import threading
import time
import unittest
from datetime import timedelta

import support
from neurio_collector.config import Config, ConfigError, Secret
from neurio_collector.ha_ingest import ingest_ha_state
from neurio_collector.ingest import ingest_body
from neurio_collector.api import dispatch
from neurio_collector.mqtt_client import MqttClient, MqttError, MqttMessage
from neurio_collector.mqtt_ingest import mark_hourly_acked, store_hourly_record
from neurio_collector.poller import Poller
from neurio_collector.timeutil import format_utc, parse_instant

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

    def test_raw_topic_explicitly_empty_disables_it(self):
        cfg = Config.from_env({**BASE_ENV, "MQTT_RAW_TOPIC": ""})
        self.assertEqual(cfg.mqtt_raw_topic, "")
        cfg = Config.from_env({**BASE_ENV, "MQTT_RAW_TOPIC": "   "})
        self.assertEqual(cfg.mqtt_raw_topic, "")
        cfg = mqtt_config(mqtt_raw_topic="")
        self.assertEqual(cfg.mqtt_raw_topic, "")

    def test_hourly_stale_threshold_defaults_and_rejects_non_positive(self):
        cfg = Config.from_env(BASE_ENV)
        self.assertEqual(cfg.hourly_stale_after_seconds, 3900)
        self.assertEqual(cfg.stale_after_seconds, 60)
        overridden = Config.from_env(
            {**BASE_ENV, "NEURIO_HOURLY_STALE_AFTER_SECONDS": "7200"}
        )
        self.assertEqual(overridden.hourly_stale_after_seconds, 7200)
        self.assertEqual(overridden.stale_after_seconds, 60)
        for raw in ("0", "-5", "nope"):
            with self.assertRaises(ConfigError):
                Config.from_env({**BASE_ENV, "NEURIO_HOURLY_STALE_AFTER_SECONDS": raw})

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
        snap = poller.snapshot()
        self.assertEqual(snap["mqtt_raw_messages"], 1)
        self.assertEqual(snap["mqtt_hourly_messages"], 1)
        self.assertIsNotNone(snap["last_raw_message_at"])
        self.assertIsNotNone(snap["last_hourly_message_at"])

    def test_poller_hourly_only_when_raw_disabled(self):
        db = support.open_db(self)
        cfg = mqtt_config(mqtt_raw_topic="")
        fake = FakeMqttClient(
            [MqttMessage(topic="neurio/hourly/energy", payload=hourly_body(), qos=1)]
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
        self.assertEqual(fake.subscriptions, [("neurio/hourly/energy", 1)])
        self.assertEqual(
            fake.published, [("neurio/hourly/ack", str(HOURLY_ID).encode())]
        )
        snap = poller.snapshot()
        self.assertEqual(snap["mqtt_hourly_messages"], 1)
        self.assertEqual(snap["mqtt_raw_messages"], 0)
        self.assertIsNotNone(snap["last_hourly_message_at"])
        self.assertIsNone(snap["last_raw_message_at"])
        count = db.conn.execute("SELECT COUNT(*) AS n FROM samples").fetchone()["n"]
        self.assertEqual(count, 0)

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


class HourlyApiTests(unittest.TestCase):
    def _cfg_for(self, db):
        return mqtt_config(data_dir=db.path.rsplit("/", 1)[0])

    def _seed(self, db, cfg):
        first = store_hourly_record(db, hourly_body(), RECEIVED, cfg)
        second_id = HOURLY_ID + 3600
        body = json.loads(hourly_body())
        body["id"] = second_id
        body["net"]["delta_net_kwh"] = -0.25
        second = store_hourly_record(
            db, json.dumps(body).encode(), RECEIVED, cfg
        )
        self.assertEqual(first.outcome, "stored")
        self.assertEqual(second.outcome, "stored")
        mark_hourly_acked(db, HOURLY_ID, RECEIVED)
        return second_id

    def test_status_counts_and_summarizes_hourly_records(self):
        db = support.open_db(self)
        cfg = self._cfg_for(db)
        second_id = self._seed(db, cfg)
        status, body, _ = dispatch("/api/v1/status", cfg, db, None)
        self.assertEqual(status, 200)
        self.assertEqual(body["counts"]["hourly_records"], 2)
        summary = body["hourly_records"]
        self.assertEqual(summary["stored"], 2)
        self.assertEqual(summary["acked"], 1)
        self.assertEqual(summary["last_record_id"], second_id)
        self.assertIsNotNone(summary["last_received_at_utc"])
        self.assertIsNotNone(summary["last_acked_at_utc"])

    def test_hourly_endpoint_lists_records_with_kwh_strings(self):
        db = support.open_db(self)
        cfg = self._cfg_for(db)
        second_id = self._seed(db, cfg)
        status, body, _ = dispatch("/api/v1/hourly", cfg, db, None)
        self.assertEqual(status, 200)
        records = body["hourly_records"]
        self.assertEqual([r["record_id"] for r in records], [HOURLY_ID, second_id])
        self.assertEqual(records[0]["net_delta_net_kwh"], "0.5")
        self.assertEqual(records[0]["net_import_kwh"], "1234.567")
        self.assertEqual(records[1]["net_delta_net_kwh"], "-0.25")
        self.assertIsNotNone(records[0]["acked_at_utc"])
        self.assertIsNone(records[1]["acked_at_utc"])
        self.assertIsNone(body["next_cursor"])

    def test_hourly_endpoint_paginates_by_record_id(self):
        db = support.open_db(self)
        cfg = self._cfg_for(db)
        second_id = self._seed(db, cfg)
        status, page1, _ = dispatch("/api/v1/hourly?limit=1", cfg, db, None)
        self.assertEqual(status, 200)
        self.assertEqual(len(page1["hourly_records"]), 1)
        self.assertEqual(page1["next_cursor"], str(HOURLY_ID))
        status, page2, _ = dispatch(
            f"/api/v1/hourly?limit=1&cursor={page1['next_cursor']}", cfg, db, None
        )
        self.assertEqual(status, 200)
        self.assertEqual(
            [r["record_id"] for r in page2["hourly_records"]], [second_id]
        )
        self.assertIsNone(page2["next_cursor"])

    def test_status_mqtt_relay_reports_raw_disabled(self):
        db = support.open_db(self)
        cfg = mqtt_config(data_dir=db.path.rsplit("/", 1)[0], mqtt_raw_topic="")
        status, body, _ = dispatch("/api/v1/status", cfg, db, None)
        self.assertEqual(status, 200)
        relay = body["mqtt_relay"]
        self.assertEqual(relay["raw_topic"], "")
        self.assertFalse(relay["raw_topic_enabled"])

    def test_status_mqtt_relay_reports_raw_enabled_by_default(self):
        db = support.open_db(self)
        cfg = self._cfg_for(db)
        status, body, _ = dispatch("/api/v1/status", cfg, db, None)
        self.assertEqual(status, 200)
        relay = body["mqtt_relay"]
        self.assertEqual(relay["raw_topic"], "neurio/raw")
        self.assertTrue(relay["raw_topic_enabled"])

    def test_hourly_empty_database(self):
        db = support.open_db(self)
        cfg = self._cfg_for(db)
        status, body, _ = dispatch("/api/v1/hourly", cfg, db, None)
        self.assertEqual(status, 200)
        self.assertEqual(body["hourly_records"], [])
        status, body, _ = dispatch("/api/v1/status", cfg, db, None)
        self.assertEqual(body["counts"]["hourly_records"], 0)
        self.assertEqual(body["hourly_records"]["stored"], 0)
        self.assertEqual(body["hourly_records"]["acked"], 0)
        self.assertIsNone(body["hourly_records"]["last_record_id"])


FRESH_NOW = parse_instant("2026-11-06T18:00:00Z")


class _HourlyClock:
    """Poller stand-in that only exposes the hourly message time."""

    def __init__(self, last_hourly_message_at):
        self._at = last_hourly_message_at

    def snapshot(self):
        return {"last_hourly_message_at": self._at, "storage_writable": True}


def _seconds_ago(seconds: float):
    return FRESH_NOW - timedelta(seconds=seconds)


def _stamp(seconds: float) -> str:
    return format_utc(_seconds_ago(seconds))


def _record_poll(db, seconds_ago: float, outcome: str = "success") -> None:
    stamp = _stamp(seconds_ago)
    db.record_poll(stamp, stamp, outcome, None, None, None, 0.0)


def _store_hourly(db, cfg, seconds_ago: float, record_id: int = HOURLY_ID) -> None:
    result = store_hourly_record(
        db, hourly_body(id=record_id), _seconds_ago(seconds_ago), cfg
    )
    if result.outcome != "stored":
        raise AssertionError(result.outcome)


def _hourly_cfg(db, **overrides):
    return mqtt_config(
        data_dir=db.path.rsplit("/", 1)[0], mqtt_raw_topic="", **overrides
    )


def _device_cfg(db, **overrides):
    return support.make_config(data_dir=db.path.rsplit("/", 1)[0], **overrides)


def _ha_cfg(db, **overrides):
    data = dict(
        data_dir=db.path.rsplit("/", 1)[0],
        source_mode="homeassistant",
        neurio_url="",
        ha_url="http://127.0.0.1",
        ha_entity="sensor.neurio_raw",
        ha_source_id="ha-relay:sensor.neurio_raw",
        ha_token=Secret("synthetic-ha-token-not-a-secret"),
    )
    data.update(overrides)
    return support.make_config(**data)


def _ingest_ha(db, cfg, seconds_ago: float) -> None:
    stamp = _stamp(seconds_ago)
    body = json.dumps(
        {
            "entity_id": "sensor.neurio_raw",
            "state": "OK",
            "attributes": {"channels": [support.channel("NET", 3, 10, 0)]},
            "last_changed": stamp,
            "last_updated": stamp,
            "last_reported": stamp,
        }
    ).encode()
    result = ingest_ha_state(db, body, _seconds_ago(seconds_ago), cfg)
    if result.outcome == "parse_error":
        raise AssertionError(result.error)


def _views(cfg, db, poller=None):
    status_code, status, _ = dispatch("/api/v1/status", cfg, db, poller, now=FRESH_NOW)
    latest_code, latest, _ = dispatch("/api/v1/latest", cfg, db, poller, now=FRESH_NOW)
    if status_code != 200 or latest_code != 200:
        raise AssertionError((status_code, latest_code))
    return status, latest


def _assert_fresh(test, status, latest, fresh, age, threshold):
    for body in (status, latest):
        test.assertEqual(body["device_fresh"], fresh)
        if age is None:
            test.assertIsNone(body["device_age_seconds"])
        else:
            test.assertAlmostEqual(body["device_age_seconds"], age, places=3)
    test.assertEqual(status["staleness_threshold_seconds"], threshold)


class ComposeMqttFileTests(unittest.TestCase):
    def test_mqtt_compose_is_hourly_only_and_keeps_the_password_out_of_the_file(self):
        root = support.ROOT
        text = (root / "compose.mqtt.yml").read_text(encoding="utf-8")
        self.assertIn('SOURCE_MODE: "mqtt"', text)
        self.assertIn('MQTT_BROKER_HOST: "${MQTT_BROKER_HOST:-192.168.86.7}"', text)
        self.assertIn('MQTT_BROKER_PORT: "${MQTT_BROKER_PORT:-1883}"', text)
        self.assertIn('MQTT_USERNAME: "${MQTT_USERNAME:?set MQTT_USERNAME}"', text)
        self.assertIn('MQTT_PASSWORD_FILE: "/run/secrets/mqtt_password"', text)
        self.assertIn('MQTT_RAW_TOPIC: ""', text)
        self.assertIn('MQTT_HOURLY_TOPIC: "neurio/hourly/energy"', text)
        self.assertIn('MQTT_ACK_TOPIC: "neurio/hourly/ack"', text)
        self.assertIn("mqtt_password:", text)
        self.assertNotIn("ha_token", text)
        self.assertNotIn("HA_TOKEN", text)
        self.assertNotIn("HA_URL", text)
        self.assertNotIn("NEURIO_URL", text)
        for line in text.splitlines():
            self.assertFalse(line.strip().startswith("MQTT_PASSWORD:"))
        ha = (root / "compose.yml").read_text(encoding="utf-8")
        self.assertIn('SOURCE_MODE: "homeassistant"', ha)
        self.assertIn("ha_token", ha)
        self.assertNotIn("MQTT_", ha)


class HourlyFreshnessTests(unittest.TestCase):
    def test_hourly_only_stays_fresh_past_the_raw_threshold(self):
        db = support.open_db(self)
        cfg = _hourly_cfg(db)
        # A successful poll 120s ago is stale on the 60s raw clock.
        _record_poll(db, 120)
        _store_hourly(db, cfg, 120)
        status, latest = _views(cfg, db)
        _assert_fresh(self, status, latest, True, 120, 3900)

    def test_hourly_only_threshold_is_inclusive_and_then_stale(self):
        db = support.open_db(self)
        cfg = _hourly_cfg(db)
        _store_hourly(db, cfg, 3900)
        status, latest = _views(cfg, db)
        _assert_fresh(self, status, latest, True, 3900, 3900)

        db = support.open_db(self)
        cfg = _hourly_cfg(db)
        _record_poll(db, 3901)
        _store_hourly(db, cfg, 3901)
        status, latest = _views(cfg, db)
        _assert_fresh(self, status, latest, False, 3901, 3900)
        code, ready, _ = dispatch("/readyz", cfg, db, None, now=FRESH_NOW)
        self.assertEqual(code, 200)
        self.assertTrue(ready["ready"])
        self.assertFalse(ready["device_fresh"])

    def test_hourly_only_without_an_hourly_message_is_not_fresh(self):
        db = support.open_db(self)
        cfg = _hourly_cfg(db)
        _record_poll(db, 10)
        status, latest = _views(cfg, db)
        _assert_fresh(self, status, latest, False, None, 3900)

    def test_hourly_only_uses_poller_time_ahead_of_an_older_row(self):
        db = support.open_db(self)
        cfg = _hourly_cfg(db)
        _store_hourly(db, cfg, 4000)
        poller = _HourlyClock(_stamp(30))
        status, latest = _views(cfg, db, poller)
        _assert_fresh(self, status, latest, True, 30, 3900)

    def test_hourly_only_falls_back_to_the_stored_row_after_restart(self):
        db = support.open_db(self)
        cfg = _hourly_cfg(db)
        _store_hourly(db, cfg, 200)
        poller = _HourlyClock(None)
        status, latest = _views(cfg, db, poller)
        _assert_fresh(self, status, latest, True, 200, 3900)

    def test_hourly_only_custom_threshold(self):
        db = support.open_db(self)
        cfg = _hourly_cfg(db, hourly_stale_after_seconds=1000)
        _store_hourly(db, cfg, 1000)
        status, latest = _views(cfg, db)
        _assert_fresh(self, status, latest, True, 1000, 1000)

        db = support.open_db(self)
        cfg = _hourly_cfg(db, hourly_stale_after_seconds=1000)
        _store_hourly(db, cfg, 1001)
        status, latest = _views(cfg, db)
        _assert_fresh(self, status, latest, False, 1001, 1000)

    def test_whitespace_raw_topic_is_hourly_only(self):
        db = support.open_db(self)
        cfg = mqtt_config(
            data_dir=db.path.rsplit("/", 1)[0], mqtt_raw_topic="   "
        )
        _record_poll(db, 120)
        _store_hourly(db, cfg, 120)
        status, latest = _views(cfg, db)
        _assert_fresh(self, status, latest, True, 120, 3900)

    def test_failed_poll_does_not_hide_a_recent_hourly_message(self):
        db = support.open_db(self)
        cfg = _hourly_cfg(db)
        _record_poll(db, 5, outcome="mqtt_error")
        _store_hourly(db, cfg, 100)
        status, latest = _views(cfg, db)
        _assert_fresh(self, status, latest, True, 100, 3900)

    def test_raw_mqtt_keeps_the_short_poll_clock(self):
        db = support.open_db(self)
        cfg = mqtt_config(
            data_dir=db.path.rsplit("/", 1)[0], hourly_stale_after_seconds=1
        )
        _record_poll(db, 30)
        _store_hourly(db, cfg, 4000)
        status, latest = _views(cfg, db, _HourlyClock(_stamp(5)))
        _assert_fresh(self, status, latest, True, 30, 60)

        db = support.open_db(self)
        cfg = mqtt_config(data_dir=db.path.rsplit("/", 1)[0])
        _record_poll(db, 90)
        _store_hourly(db, cfg, 10)
        status, latest = _views(cfg, db, _HourlyClock(_stamp(10)))
        _assert_fresh(self, status, latest, False, 90, 60)

        db = support.open_db(self)
        cfg = mqtt_config(data_dir=db.path.rsplit("/", 1)[0])
        _record_poll(db, 5, outcome="mqtt_error")
        _store_hourly(db, cfg, 5)
        status, latest = _views(cfg, db, _HourlyClock(_stamp(5)))
        _assert_fresh(self, status, latest, False, None, 60)

    def test_device_mode_ignores_hourly_records(self):
        db = support.open_db(self)
        cfg = _device_cfg(db, hourly_stale_after_seconds=1)
        _record_poll(db, 30)
        _store_hourly(db, cfg, 4000)
        status, latest = _views(cfg, db, _HourlyClock(_stamp(5)))
        _assert_fresh(self, status, latest, True, 30, 60)

        db = support.open_db(self)
        cfg = _device_cfg(db)
        _record_poll(db, 90)
        _store_hourly(db, cfg, 10)
        status, latest = _views(cfg, db, _HourlyClock(_stamp(10)))
        _assert_fresh(self, status, latest, False, 90, 60)

    def test_home_assistant_mode_ignores_the_hourly_clock(self):
        db = support.open_db(self)
        cfg = _ha_cfg(db, hourly_stale_after_seconds=1)
        _ingest_ha(db, cfg, 30)
        _store_hourly(db, cfg, 4000)
        status, latest = _views(cfg, db, _HourlyClock(_stamp(5)))
        _assert_fresh(self, status, latest, True, 30, 60)

        db = support.open_db(self)
        cfg = _ha_cfg(db)
        _ingest_ha(db, cfg, 90)
        _store_hourly(db, cfg, 10)
        status, latest = _views(cfg, db, _HourlyClock(_stamp(10)))
        _assert_fresh(self, status, latest, False, 90, 60)


if __name__ == "__main__":
    unittest.main()
