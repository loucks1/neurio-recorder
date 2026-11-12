"""Step 3 contract tests: the read-time merged hourly view.

The merged view (?view=merged) unions hourly_records (ESP32 bridge) and
cloud_hourly_records (Neurio cloud), cloud-preferred per hour, with
origin, esp32 shadow values, and divergence flags. The default listing
must stay exactly what it was.
"""

import json
import unittest
from datetime import datetime, timedelta, timezone

import support
from neurio_collector.api import build_status, dispatch
from neurio_collector.cloud_ingest import store_cloud_hours
from neurio_collector.mqtt_ingest import store_hourly_record

NOW = datetime(2026, 11, 11, 12, 0, tzinfo=timezone.utc)
H0 = int(datetime(2026, 11, 6, 6, 0, tzinfo=timezone.utc).timestamp())

PROVENANCE_DDL = """
CREATE TABLE hourly_backfill_provenance (
  record_id INTEGER PRIMARY KEY,
  backfill_source TEXT NOT NULL,
  detail TEXT NOT NULL
);
"""


def iso(epoch):
    from neurio_collector.timeutil import format_utc

    return format_utc(datetime.fromtimestamp(epoch, tz=timezone.utc))


def ws(kwh):
    return int(round(kwh * 3_600_000))


def add_esp(db, cfg, hour_end, consumption_kwh, generation_kwh):
    body = {
        "id": hour_end,
        "timestamp": iso(hour_end),
        "retries": 0,
        "net": {
            "import_kwh": 10.0,
            "export_kwh": 1.0,
            "delta_net_kwh": consumption_kwh - generation_kwh,
        },
        "generation": {"delta_export_kwh": generation_kwh},
        "consumption": {"delta_import_kwh": consumption_kwh},
        "phase_a": {"delta_import_kwh": consumption_kwh / 2},
        "phase_b": {"delta_import_kwh": consumption_kwh / 2},
    }
    result = store_hourly_record(
        db, json.dumps(body).encode(), NOW, cfg
    )
    assert result.outcome == "stored", result


def add_cloud(db, hour_end, consumption_kwh, generation_kwh):
    item = {
        "start": iso(hour_end - 3600),
        "end": iso(hour_end),
        "consumptionEnergy": ws(consumption_kwh),
        "generationEnergy": ws(generation_kwh),
        "importedEnergy": ws(consumption_kwh),
        "exportedEnergy": ws(generation_kwh),
    }
    stored, skipped = store_cloud_hours(db, [item], NOW)
    assert stored == 1 and skipped == 0


def add_provenance(db, record_id):
    with db.write_lock:
        with db.transaction():
            db.conn.execute(PROVENANCE_DDL)
            db.conn.execute(
                "INSERT INTO hourly_backfill_provenance VALUES(?,?,?)",
                (record_id, "neurio_cloud_stats", "batch backfill-20261110"),
            )


def merged(db, cfg, query=""):
    status, payload, _ = dispatch(
        f"/api/v1/hourly?view=merged{query}", cfg, db, None
    )
    assert status == 200, payload
    return payload


def by_id(payload):
    return {r["record_id"]: r for r in payload["hourly_records"]}


class MergedViewTests(unittest.TestCase):
    def setUp(self):
        self.cfg = support.make_config()
        self.db = support.open_db(self)

    def test_default_listing_unchanged(self):
        add_esp(self.db, self.cfg, H0, 0.8, 0.3)
        status, payload, _ = dispatch("/api/v1/hourly", self.cfg, self.db, None)
        self.assertEqual(status, 200)
        (record,) = payload["hourly_records"]
        self.assertNotIn("origin", record)
        self.assertEqual(record["consumption_delta_import_kwh"], "0.8")
        self.assertEqual(record["generation_delta_export_kwh"], "0.3")
        self.assertIsNone(payload["next_cursor"])

    def test_cloud_preferred_and_esp32_shadow_carried(self):
        add_esp(self.db, self.cfg, H0, 0.98, 0.48)
        add_cloud(self.db, H0, 1.0, 0.5)
        record = by_id(merged(self.db, self.cfg))[H0]
        self.assertEqual(record["origin"], "neurio")
        self.assertEqual(record["consumption_delta_import_kwh"], "1")
        self.assertEqual(record["generation_delta_export_kwh"], "0.5")
        self.assertEqual(record["esp32_consumption_kwh"], "0.98")
        self.assertEqual(record["esp32_generation_kwh"], "0.48")
        self.assertFalse(record["divergent_consumption"])
        self.assertFalse(record["divergent_generation"])

    def test_cloud_only_hour_appears(self):
        add_cloud(self.db, H0, 1.25, 0.5)
        record = by_id(merged(self.db, self.cfg))[H0]
        self.assertEqual(record["origin"], "neurio")
        self.assertEqual(record["device_timestamp_utc"], iso(H0))
        self.assertEqual(record["consumption_delta_import_kwh"], "1.25")
        self.assertIsNone(record["acked_at_utc"])
        self.assertNotIn("esp32_consumption_kwh", record)

    def test_esp32_only_hour(self):
        add_esp(self.db, self.cfg, H0, 0.8, 0.3)
        record = by_id(merged(self.db, self.cfg))[H0]
        self.assertEqual(record["origin"], "esp32")
        self.assertEqual(record["consumption_delta_import_kwh"], "0.8")
        self.assertNotIn("esp32_consumption_kwh", record)
        self.assertNotIn("divergent_consumption", record)

    def test_provenance_row_is_cloud_origin_without_divergence(self):
        add_esp(self.db, self.cfg, H0, 0.9, 0.4)
        add_provenance(self.db, H0)
        record = by_id(merged(self.db, self.cfg))[H0]
        self.assertEqual(record["origin"], "neurio")
        self.assertEqual(record["consumption_delta_import_kwh"], "0.9")
        self.assertNotIn("esp32_consumption_kwh", record)
        # The cloud table now also holds the hour, with different values:
        # the table wins, still no divergence (same source, not two).
        add_cloud(self.db, H0, 1.7, 0.9)
        record = by_id(merged(self.db, self.cfg))[H0]
        self.assertEqual(record["consumption_delta_import_kwh"], "1.7")
        self.assertNotIn("esp32_consumption_kwh", record)
        self.assertNotIn("divergent_consumption", record)
        status = build_status(self.cfg, self.db, None, NOW)
        self.assertEqual(status["merge"]["counts"], {"neurio": 1, "esp32": 0})
        self.assertEqual(status["merge"]["divergence_count"], 0)

    def test_divergence_boundaries_per_quantity(self):
        cases = [
            # (hour offset, cloud kWh, esp32 kWh, expected flag)
            (0, 1.000, 1.100, False),  # exactly +10%: not flagged
            (1, 1.000, 1.101, True),   # just over +10%
            (2, 0.400, 0.450, False),  # exactly +0.05 kWh: not flagged
            (3, 0.400, 0.451, True),   # just over +0.05 kWh
        ]
        for offset, cloud_kwh, esp_kwh, _ in cases:
            hour = H0 + offset * 3600
            add_esp(self.db, self.cfg, hour, esp_kwh, 0.5)
            add_cloud(self.db, hour, cloud_kwh, 0.5)
        records = by_id(merged(self.db, self.cfg))
        for offset, _, _, expected in cases:
            record = records[H0 + offset * 3600]
            self.assertEqual(record["divergent_consumption"], expected, record)
            # Generation is identical on both sides: never flagged, so the
            # two quantities really are judged independently.
            self.assertFalse(record["divergent_generation"])

    def test_merged_pagination_walks_the_union(self):
        add_esp(self.db, self.cfg, H0, 0.8, 0.3)                    # esp only
        add_esp(self.db, self.cfg, H0 + 3600, 0.8, 0.3)             # both
        add_cloud(self.db, H0 + 3600, 0.9, 0.3)
        add_cloud(self.db, H0 + 2 * 3600, 1.0, 0.5)                 # cloud only
        add_esp(self.db, self.cfg, H0 + 3 * 3600, 0.7, 0.2)         # esp only
        add_cloud(self.db, H0 + 4 * 3600, 1.1, 0.6)                 # cloud only
        seen = []
        cursor = "0"
        while True:
            payload = merged(self.db, self.cfg, f"&limit=2&cursor={cursor}")
            seen.extend(r["record_id"] for r in payload["hourly_records"])
            if payload["next_cursor"] is None:
                break
            cursor = payload["next_cursor"]
        self.assertEqual(seen, [H0 + i * 3600 for i in range(5)])

    def test_merged_without_cloud_table_is_default_plus_origin(self):
        add_esp(self.db, self.cfg, H0, 0.8, 0.3)
        _, default_payload, _ = dispatch("/api/v1/hourly", self.cfg, self.db, None)
        merged_payload = merged(self.db, self.cfg)
        (default_record,) = default_payload["hourly_records"]
        (merged_record,) = merged_payload["hourly_records"]
        self.assertEqual(merged_record, {**default_record, "origin": "esp32"})
        status = build_status(self.cfg, self.db, None, NOW)
        self.assertEqual(status["merge"]["counts"], {"neurio": 0, "esp32": 1})
        self.assertEqual(status["merge"]["divergence_count"], 0)
        self.assertEqual(status["merge"]["worst_divergent"], [])

    def test_invalid_view_rejected(self):
        status, _, _ = dispatch(
            "/api/v1/hourly?view=bogus", self.cfg, self.db, None
        )
        self.assertEqual(status, 400)


class StatusMergeTests(unittest.TestCase):
    def setUp(self):
        self.cfg = support.make_config()
        self.db = support.open_db(self)

    def test_counts_and_worst_divergent(self):
        # Divergent consumption: cloud 1.0 vs esp32 1.3 (+30%).
        add_esp(self.db, self.cfg, H0, 1.3, 0.5)
        add_cloud(self.db, H0, 1.0, 0.5)
        # Divergent generation: cloud 0.5 vs esp32 0.7 (+40%) — worse.
        add_esp(self.db, self.cfg, H0 + 3600, 0.9, 0.7)
        add_cloud(self.db, H0 + 3600, 0.9, 0.5)
        # One esp32-only and one cloud-only hour for the counts.
        add_esp(self.db, self.cfg, H0 + 2 * 3600, 0.8, 0.3)
        add_cloud(self.db, H0 + 3 * 3600, 1.0, 0.5)
        merge_section = build_status(self.cfg, self.db, None, NOW)["merge"]
        self.assertEqual(merge_section["counts"], {"neurio": 3, "esp32": 1})
        self.assertEqual(merge_section["divergence_count"], 2)
        self.assertEqual(merge_section["divergent_hours"], 2)
        worst = merge_section["worst_divergent"]
        self.assertEqual(len(worst), 2)
        self.assertEqual(worst[0]["quantity"], "generation")
        self.assertEqual(worst[0]["pct_difference"], 40.0)
        self.assertEqual(worst[0]["cloud_kwh"], "0.5")
        self.assertEqual(worst[0]["esp32_kwh"], "0.7")
        self.assertEqual(worst[0]["hour_end_utc"], iso(H0 + 3600))
        self.assertEqual(worst[1]["quantity"], "consumption")
        self.assertEqual(worst[1]["pct_difference"], 30.0)


if __name__ == "__main__":
    unittest.main()
