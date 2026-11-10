import json
import unittest

import support
from neurio_collector.config import ConfigError
from neurio_collector.fetch import fetch_current_sample
from neurio_collector.observed import (
    CANDIDATE_DEVICE_URL,
    CANDIDATE_DEVICE_URL_EXPLICIT_PORT,
    COLLECTOR_DEFAULT_POLL_INTERVAL_SECONDS,
    HA_POLL_INTERVAL_SECONDS,
)
from neurio_collector.parse import parse_payload
from neurio_collector.power import estimate_interval_ws
from neurio_collector.units import kwh_str_to_ws, ws_to_kwh_str, ws_to_wh_str


class ConfigTests(unittest.TestCase):
    def test_required_bind_and_rejects_unspecified(self):
        with self.assertRaises(ConfigError):
            support.make_config(bind_address="")
        with self.assertRaises(ConfigError):
            support.make_config(bind_address="0.0.0.0")
        with self.assertRaises(ConfigError):
            support.make_config(bind_address="::")
        allowed = support.make_config(
            bind_address="0.0.0.0",
            allow_container_wildcard_bind="host-publish-pinned",
        )
        self.assertEqual(allowed.bind_address, "0.0.0.0")

    def test_missing_env_has_no_default_bind(self):
        from neurio_collector.config import Config

        with self.assertRaises(ConfigError) as caught:
            Config.from_env({})
        self.assertIn("SOURCE_MODE", str(caught.exception))
        with self.assertRaises(ConfigError) as caught:
            Config.from_env({"SOURCE_MODE": "device"})
        self.assertIn("NEURIO_URL", str(caught.exception))
        with self.assertRaises(ConfigError) as caught:
            Config.from_env(
                {
                    "SOURCE_MODE": "device",
                    "NEURIO_URL": "http://192.168.1.50/current-sample",
                    "DATA_DIR": "/tmp/neurio-bind-test",
                    "BIND_PORT": "8095",
                }
            )
        self.assertIn("BIND_ADDRESS", str(caught.exception))

    def test_poll_interval_stays_slower_than_home_assistant(self):
        self.assertEqual(HA_POLL_INTERVAL_SECONDS, 5)
        self.assertEqual(COLLECTOR_DEFAULT_POLL_INTERVAL_SECONDS, 15)
        cfg = support.make_config()
        self.assertEqual(cfg.poll_interval_seconds, 15)
        with self.assertRaises(ConfigError) as caught:
            support.make_config(poll_interval_seconds=5)
        self.assertIn("5 seconds", str(caught.exception))

    def test_cloud_and_credentials_rejected(self):
        with self.assertRaises(ConfigError):
            support.make_config(neurio_url="https://api.neur.io/v1/samples")
        with self.assertRaises(ConfigError):
            support.make_config(neurio_url="http://user:secret@192.168.1.50/current-sample")
        cfg = support.make_config(neurio_url=CANDIDATE_DEVICE_URL_EXPLICIT_PORT)
        self.assertEqual(cfg.neurio_url, CANDIDATE_DEVICE_URL_EXPLICIT_PORT)
        result = fetch_current_sample("https://api.neur.io/v1/samples", 1, 1000)
        self.assertEqual(result.outcome, "rejected_url")
        self.assertEqual(result.body, b"")

    def test_derivation_requires_verified_topology(self):
        with self.assertRaises(ConfigError):
            support.make_config(consumption_source="derive_net_plus_generation")
        cfg = support.make_config(
            consumption_source="derive_net_plus_generation",
            topology_verified=True,
        )
        self.assertTrue(cfg.topology_verified)

    def test_candidate_url_is_explicit_port_80(self):
        self.assertEqual(CANDIDATE_DEVICE_URL, "http://192.168.1.50/current-sample")
        self.assertEqual(
            CANDIDATE_DEVICE_URL_EXPLICIT_PORT,
            "http://192.168.1.50:80/current-sample",
        )


class ParseAndUnitTests(unittest.TestCase):
    def test_observed_ha_recorder_fixture(self):
        path = support.OBSERVED / "ha-recorder-neurio-raw-channels-2026-11-06.json"
        raw = path.read_bytes()
        payload = json.loads(raw)
        self.assertTrue(payload["_provenance"]["not_a_live_current_sample"])
        self.assertTrue(payload["_provenance"]["not_a_complete_physical_payload"])
        parsed = parse_payload(raw)
        self.assertIsNone(parsed.sensor_id)
        self.assertEqual(parsed.clock_quality, "missing")
        self.assertEqual(
            [(item.type, item.ch) for item in parsed.channels],
            [
                ("PHASE_A_CONSUMPTION", 1),
                ("PHASE_B_CONSUMPTION", 2),
                ("NET", 3),
                ("GENERATION", 4),
                ("CONSUMPTION", 5),
            ],
        )
        by_type = {item.type: item for item in parsed.channels}
        self.assertEqual(by_type["NET"].e_imp_ws, 445392613666)
        self.assertEqual(by_type["NET"].e_exp_ws, 236251294077)
        self.assertEqual(by_type["NET"].p_w, 738)
        self.assertEqual(by_type["CONSUMPTION"].e_imp_ws, 626227269168)
        self.assertEqual(by_type["CONSUMPTION"].e_exp_ws, 0)
        self.assertEqual(by_type["CONSUMPTION"].p_w, 739)
        self.assertEqual(by_type["GENERATION"].e_imp_ws, 417287992434)
        self.assertEqual(by_type["GENERATION"].e_exp_ws, 145362402)
        self.assertEqual(by_type["GENERATION"].p_w, 0)
        self.assertEqual(by_type["PHASE_A_CONSUMPTION"].p_w, 586)
        self.assertEqual(by_type["PHASE_B_CONSUMPTION"].p_w, 153)
        phase_energy = (
            by_type["PHASE_A_CONSUMPTION"].e_imp_ws + by_type["PHASE_B_CONSUMPTION"].e_imp_ws
        )
        self.assertNotEqual(phase_energy, by_type["CONSUMPTION"].e_imp_ws)
        self.assertNotEqual(by_type["NET"].p_w, by_type["CONSUMPTION"].p_w)
        for item in parsed.channels:
            self.assertEqual(kwh_str_to_ws(ws_to_kwh_str(item.e_imp_ws)), item.e_imp_ws)
            self.assertEqual(kwh_str_to_ws(ws_to_kwh_str(item.e_exp_ws)), item.e_exp_ws)

    def test_alternate_keys_and_not_synchronized_are_synthetic(self):
        raw = (support.SYNTHETIC / "alternate-keys.json").read_bytes()
        parsed = parse_payload(raw)
        self.assertEqual(parsed.sensor_id, "SYNTHETIC-NOT-A-DEVICE")
        self.assertEqual(parsed.clock_quality, "not_synchronized")
        self.assertIsNone(parsed.device_timestamp_utc)
        channel = parsed.channels[0]
        self.assertEqual(channel.identity_key, "ch:3|type:NET")
        self.assertEqual(channel.e_imp_ws, 10)
        self.assertEqual(channel.e_exp_ws, 4)
        self.assertEqual(channel.q_var, 3)
        self.assertEqual(channel.v_v, 120.5)
        self.assertIn("voltage_key_V_V", channel.flags)
        self.assertEqual(parsed.cts[0].p_w, -300)

    def test_non_integral_and_overflow_stay_null(self):
        body = json.dumps(
            {
                "sensorId": "SYNTHETIC-NOT-A-DEVICE",
                "timestamp": "2026-07-15T12:00:00Z",
                "channels": [
                    {
                        "type": "NET",
                        "ch": 3,
                        "eImp_Ws": 1.5,
                        "eExp_Ws": 2**63,
                        "p_W": 1,
                        "q_VAR": 0,
                        "v_V": 1,
                    }
                ],
            }
        ).encode()
        parsed = parse_payload(body)
        self.assertIsNone(parsed.channels[0].e_imp_ws)
        self.assertIsNone(parsed.channels[0].e_exp_ws)

    def test_units_and_negative_net(self):
        self.assertEqual(ws_to_wh_str(3600), "1")
        self.assertEqual(ws_to_kwh_str(3_600_000), "1")
        self.assertEqual(ws_to_kwh_str(1_800_000), "0.5")
        self.assertEqual(ws_to_kwh_str(0), "0")
        self.assertEqual(ws_to_kwh_str(-4000), ws_to_kwh_str(-4000))
        self.assertTrue(ws_to_kwh_str(-4000).startswith("-"))
        self.assertNotEqual(max(-4000, 0), 1000)

    def test_power_estimate_refuses_long_gaps(self):
        short = estimate_interval_ws(100, 300, 15, 45)
        self.assertEqual(short["source_method"], "power_integration_estimated")
        self.assertEqual(short["import_ws_est"], 3000)
        self.assertFalse(short["persisted"])
        negative = estimate_interval_ws(-40, -60, 10, 45)
        self.assertEqual(negative["import_ws_est"], 0)
        self.assertEqual(negative["export_ws_est"], 500)
        self.assertIsNone(estimate_interval_ws(100, 100, 3600, 45))


if __name__ == "__main__":
    unittest.main()
