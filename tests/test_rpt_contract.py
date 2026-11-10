"""Show the measured RatePlanTool parser bugs without serving them."""

import hashlib
import json
import unittest
from datetime import date
from decimal import Decimal
from pathlib import Path

import support
from neurio_collector.api import dispatch
from neurio_collector.energy import build_energy_report
from neurio_collector.rpt_contract import (
    RPT_COMMIT,
    RPT_SAMPLES_SHA256,
    contract_flags,
    document_would_fail_deserialization,
    emit_like_raw_data,
    in_progress_cache_hour,
    refuse_hour_boundary_export,
    request_window,
)
from neurio_collector.timeutil import parse_instant

FIXTURE = Path(__file__).resolve().parent / "fixtures" / "rateplantool" / "rateplantool-synthetic-samples.json"
STUB = Path(__file__).resolve().parent / "fixtures" / "rateplantool" / "rateplantool-stub-contract.json"


class RptSemanticsTests(unittest.TestCase):
    def setUp(self):
        self.points = json.loads(FIXTURE.read_text())

    def test_fixture_hash_matches_the_probe(self):
        digest = hashlib.sha256(FIXTURE.read_bytes()).hexdigest()
        self.assertEqual(digest, RPT_SAMPLES_SHA256)
        stub = json.loads(STUB.read_text())
        self.assertEqual(stub["commit"], RPT_COMMIT)
        self.assertTrue(stub["synthetic"])
        self.assertEqual(stub["samples_body_sha256"], RPT_SAMPLES_SHA256)
        self.assertEqual(stub["samples_request_observed"]["query"]["granularity"], "hours")
        self.assertEqual(stub["samples_request_observed"]["query"]["perPage"], "500")
        self.assertNotIn("oauth2/token", json.dumps(contract_flags()))

    def test_probe_table_gap_and_reset(self):
        rows = emit_like_raw_data(self.points)
        self.assertEqual(
            [(row["emitted_utc"], str(row["consumption_kwh"]), str(row["generation_kwh"])) for row in rows],
            [
                ("2026-01-15T21:00:00.000Z", "1.5", "0.25"),
                ("2026-01-15T22:00:00.000Z", "2", "0"),
                ("2026-01-15T23:00:00.000Z", "0.5", "0.5"),
                ("2026-01-16T00:00:00.000Z", "0.5", "0"),
                ("2026-01-16T03:00:00.000Z", "3", "1"),
                ("2026-01-16T04:00:00.000Z", "0.1", "0"),
                ("2026-01-16T05:00:00.000Z", "0", "0.2"),
            ],
        )
        gap = rows[4]
        self.assertTrue(gap["gap_assigned_to_one_hour"])
        self.assertEqual(gap["elapsed_hours"], 3)
        self.assertEqual(gap["consumption_kwh"], Decimal(3))
        emitted = {row["emitted_utc"] for row in rows}
        self.assertNotIn("2026-01-16T01:00:00.000Z", emitted)
        self.assertNotIn("2026-01-16T02:00:00.000Z", emitted)
        self.assertTrue(rows[-1]["decrease_forced_to_zero"])
        self.assertEqual(rows[-1]["consumption_kwh"], Decimal(0))
        self.assertEqual(rows[-1]["generation_kwh"], Decimal("0.2"))

    def test_fixed_24h_windows_and_hour_23_cache(self):
        winter = request_window(date(2026, 1, 15))
        summer = request_window(date(2026, 7, 15))
        spring = request_window(date(2026, 3, 8))
        fall = request_window(date(2026, 11, 1))
        self.assertEqual(winter["start_utc"], "2026-01-15T06:00:00.000Z")
        self.assertEqual(winter["end_utc"], "2026-01-16T06:00:00.000Z")
        self.assertEqual(winter["gap_hours"], 0)
        self.assertEqual(winter["request_start_dotnet"], "2026-01-15T06:00:00.0000000Z")
        self.assertEqual(summer["start_utc"], "2026-07-15T05:00:00.000Z")
        self.assertEqual(summer["end_utc"], "2026-07-16T05:00:00.000Z")
        self.assertEqual(spring["boundary"], "overlap")
        self.assertEqual(spring["end_utc"], "2026-03-09T06:00:00.000Z")
        self.assertEqual(spring["next_start_utc"], "2026-03-09T05:00:00.000Z")
        self.assertEqual(spring["gap_hours"], -1)
        self.assertEqual(fall["boundary"], "gap")
        self.assertEqual(fall["end_utc"], "2026-11-02T05:00:00.000Z")
        self.assertEqual(fall["next_start_utc"], "2026-11-02T06:00:00.000Z")
        self.assertEqual(fall["gap_hours"], 1)
        cache = in_progress_cache_hour(parse_instant("2026-12-02T05:00:00Z"))
        self.assertEqual(cache["local_hour"], 23)
        self.assertEqual(cache["cache_hour"], 24)
        self.assertTrue(cache["throws_argument_out_of_range"])

    def test_fractional_energy_fails_the_whole_document(self):
        self.assertEqual(document_would_fail_deserialization(3600000.0), "fraction")
        self.assertEqual(document_would_fail_deserialization(-1), "negative")
        self.assertIsNone(document_would_fail_deserialization(3600000))
        with self.assertRaises(Exception):
            emit_like_raw_data(
                [
                    {"timestamp": "2026-01-15T21:00:00Z", "consumptionEnergy": 0, "generationEnergy": 0},
                    {"timestamp": "2026-01-15T22:00:00Z", "consumptionEnergy": 1.5, "generationEnergy": 0},
                ]
            )

    def test_collector_does_not_put_the_gap_on_the_last_hour(self):
        db = support.open_db(self)
        cfg = support.make_config(data_dir=db.path.rsplit("/", 1)[0])
        net = lambda imp: [
            support.channel("NET", 3, imp, 0),
            support.channel("CONSUMPTION", 5, imp, 0),
            support.channel("GENERATION", 4, 0, 0),
        ]
        support.feed(
            db,
            cfg,
            [
                ("2026-01-15T21:00:00Z", "2026-01-15T21:00:00Z", net(1_000_000_000)),
                ("2026-01-15T21:00:15Z", "2026-01-15T21:00:15Z", net(1_003_600_000)),
                ("2026-01-16T00:00:15Z", "2026-01-16T00:00:15Z", net(1_003_600_000 + 10_800_000)),
            ],
        )
        report = build_energy_report(
            db.conn,
            cfg,
            {"date": "2026-01-15", "interval": "hour", "timezone": "America/Chicago"},
        )
        self.assertFalse(report["rateplantool_contract"])
        allocated = [
            bucket["household"]["consumption_ws"]
            for bucket in report["buckets"]
            if bucket["household"]["consumption_ws"]
        ]
        self.assertEqual(allocated, [3_600_000])
        self.assertNotIn(10_800_000, allocated)
        rpt_rows = emit_like_raw_data(
            [
                {"timestamp": "2026-01-15T21:00:15.000Z", "consumptionEnergy": 1003600000, "generationEnergy": 0},
                {"timestamp": "2026-01-16T00:00:15.000Z", "consumptionEnergy": 1014400000, "generationEnergy": 0},
            ]
        )
        self.assertEqual(rpt_rows[0]["consumption_kwh"], Decimal(3))
        self.assertEqual(rpt_rows[0]["emitted_utc"], "2026-01-15T23:00:15.000Z")
        self.assertTrue(rpt_rows[0]["gap_assigned_to_one_hour"])

    def test_no_facade_and_general_csv_is_not_the_samples_array(self):
        refusal = refuse_hour_boundary_export()
        self.assertIsNone(refusal["export"])
        self.assertFalse(refusal["rateplantool_contract"])
        self.assertIn("hour_boundary_cumulatives_would_be_fabricated", refusal["reasons"])
        db = support.open_db(self)
        cfg = support.make_config(data_dir=db.path.rsplit("/", 1)[0])
        for path in ("/oauth2/token", "/users/current", "/samples", "/api/v1/export/general-hourly.csv"):
            if path.endswith(".csv"):
                continue
            status, body, _ = dispatch(path, cfg, db, None)
            self.assertEqual(status, 404, path)
            self.assertNotIn("access_token", json.dumps(body))
        status, body, kind = dispatch(
            "/api/v1/export/general-hourly.csv?date=2026-01-15&timezone=America/Chicago",
            cfg,
            db,
            None,
        )
        self.assertEqual(status, 200)
        self.assertEqual(kind, "csv")
        text = body.decode()
        self.assertIn("rateplantool_contract", text)
        self.assertNotIn("consumptionEnergy", text)
        self.assertNotIn("generationEnergy", text)
        flags = dispatch("/api/v1/status", cfg, db, None)[1]
        self.assertFalse(flags["rateplantool_contract"])
        self.assertEqual(flags["rateplantool_integration"], "not_wired")
        self.assertFalse(flags["safe_drop_in_adapter"])
        self.assertEqual(flags["rateplantool_source_sha"], RPT_COMMIT)


if __name__ == "__main__":
    unittest.main()
