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
from neurio_collector.hourly_merge import PRODUCTION_PROVENANCE_DDL, PROVENANCE_COLUMNS
from neurio_collector.mqtt_ingest import store_hourly_record
from neurio_collector.timeutil import format_utc

NOW = datetime(2026, 11, 11, 12, 0, tzinfo=timezone.utc)
H0 = int(datetime(2026, 11, 6, 6, 0, tzinfo=timezone.utc).timestamp())

# The production table, plus IF NOT EXISTS so a test can insert more than one row.
PROVENANCE_DDL = PRODUCTION_PROVENANCE_DDL.replace(
    "CREATE TABLE hourly_backfill_provenance",
    "CREATE TABLE IF NOT EXISTS hourly_backfill_provenance",
    1,
)


def iso(epoch):
    from neurio_collector.timeutil import format_utc

    return format_utc(datetime.fromtimestamp(epoch, tz=timezone.utc))


def ws(kwh):
    return int(round(kwh * 3_600_000))


def add_esp(
    db, cfg, hour_end, consumption_kwh, generation_kwh, delta_import=0.4, delta_export=0.1
):
    body = {
        "id": hour_end,
        "timestamp": iso(hour_end),
        "retries": 0,
        "net": {
            "import_kwh": 10.0,
            "export_kwh": 1.0,
            "delta_import_kwh": delta_import,
            "delta_export_kwh": delta_export,
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


def add_provenance(db, record_id, source="neurio_cloud_stats"):
    """Insert one live-schema provenance row.

    ``method`` records how the net columns should be read: interval energy
    for cloud stats, a counter snapshot for the HA import and the board.
    """
    method = "interval" if source == "neurio_cloud_stats" else "counter"
    with db.write_lock:
        with db.transaction():
            db.conn.execute(PROVENANCE_DDL)
            db.conn.execute(
                "INSERT INTO hourly_backfill_provenance("
                + ",".join(PROVENANCE_COLUMNS)
                + ") VALUES(?,?,?,?,?,?,?,?,?)",
                (
                    record_id,
                    "backfill-20261110",
                    "apply",
                    source,
                    method,
                    0,
                    None,
                    None,
                    "2026-11-10T00:00:00.000Z",
                ),
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
        expected = {
            "hourly_records": [
                {
                    "record_id": H0,
                    "device_timestamp_raw": iso(H0),
                    "device_timestamp_utc": iso(H0),
                    "retries": 0,
                    "received_at_utc": format_utc(NOW),
                    "acked_at_utc": None,
                    "net_import_kwh": "10",
                    "net_export_kwh": "1",
                    "net_delta_net_kwh": "0.5",
                    "generation_delta_export_kwh": "0.3",
                    "consumption_delta_import_kwh": "0.8",
                    "phase_a_delta_import_kwh": "0.4",
                    "phase_b_delta_import_kwh": "0.4",
                }
            ],
            "next_cursor": None,
        }
        # rate-plan-tool depends on this document byte-for-byte.
        self.assertEqual(
            json.dumps(payload, separators=(",", ":"), ensure_ascii=False),
            json.dumps(expected, separators=(",", ":"), ensure_ascii=False),
        )

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
        # Cloud per-hour import/export, not the board's lifetime counters.
        self.assertEqual(record["imported_kwh"], "1")
        self.assertEqual(record["exported_kwh"], "0.5")
        self.assertEqual(record["net_import_counter_kwh"], "10")
        self.assertEqual(record["net_export_counter_kwh"], "1")
        self.assertNotIn("net_import_kwh", record)
        self.assertNotIn("net_export_kwh", record)

    def test_cloud_only_hour_appears(self):
        add_cloud(self.db, H0, 1.25, 0.5)
        record = by_id(merged(self.db, self.cfg))[H0]
        self.assertEqual(record["origin"], "neurio")
        self.assertEqual(record["device_timestamp_utc"], iso(H0))
        self.assertEqual(record["consumption_delta_import_kwh"], "1.25")
        self.assertEqual(record["imported_kwh"], "1.25")
        self.assertEqual(record["exported_kwh"], "0.5")
        self.assertIsNone(record["net_import_counter_kwh"])
        self.assertIsNone(record["net_export_counter_kwh"])
        self.assertIsNone(record["acked_at_utc"])
        self.assertNotIn("esp32_consumption_kwh", record)
        self.assertNotIn("net_import_kwh", record)

    def test_esp32_only_hour(self):
        add_esp(self.db, self.cfg, H0, 0.8, 0.3)
        record = by_id(merged(self.db, self.cfg))[H0]
        self.assertEqual(record["origin"], "esp32")
        self.assertEqual(record["consumption_delta_import_kwh"], "0.8")
        self.assertEqual(record["imported_kwh"], "0.4")
        self.assertEqual(record["exported_kwh"], "0.1")
        self.assertEqual(record["net_import_counter_kwh"], "10")
        self.assertEqual(record["net_export_counter_kwh"], "1")
        self.assertNotIn("net_import_kwh", record)
        self.assertNotIn("esp32_consumption_kwh", record)
        self.assertNotIn("divergent_consumption", record)

    def test_provenance_row_is_cloud_origin_without_divergence(self):
        add_esp(self.db, self.cfg, H0, 0.9, 0.4)
        add_provenance(self.db, H0)
        record = by_id(merged(self.db, self.cfg))[H0]
        self.assertEqual(record["origin"], "neurio")
        self.assertEqual(record["consumption_delta_import_kwh"], "0.9")
        # Stats backfill stored per-hour import/export in the net columns.
        self.assertEqual(record["imported_kwh"], "10")
        self.assertEqual(record["exported_kwh"], "1")
        self.assertIsNone(record["net_import_counter_kwh"])
        self.assertIsNone(record["net_export_counter_kwh"])
        self.assertNotIn("esp32_consumption_kwh", record)
        # The cloud table now also holds the hour, with different values:
        # the table wins, still no divergence (same source, not two).
        add_cloud(self.db, H0, 1.7, 0.9)
        record = by_id(merged(self.db, self.cfg))[H0]
        self.assertEqual(record["consumption_delta_import_kwh"], "1.7")
        self.assertEqual(record["imported_kwh"], "1.7")
        self.assertIsNone(record["net_import_counter_kwh"])
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

    def test_merged_without_cloud_table_splits_counters_from_hourly_net(self):
        add_esp(self.db, self.cfg, H0, 0.8, 0.3)
        _, default_payload, _ = dispatch("/api/v1/hourly", self.cfg, self.db, None)
        merged_payload = merged(self.db, self.cfg)
        (default_record,) = default_payload["hourly_records"]
        (merged_record,) = merged_payload["hourly_records"]
        self.assertEqual(default_record["net_import_kwh"], "10")
        self.assertEqual(default_record["net_export_kwh"], "1")
        self.assertNotIn("imported_kwh", default_record)
        self.assertNotIn("net_import_counter_kwh", default_record)
        self.assertEqual(merged_record["origin"], "esp32")
        self.assertNotIn("net_import_kwh", merged_record)
        self.assertNotIn("net_export_kwh", merged_record)
        self.assertEqual(merged_record["imported_kwh"], "0.4")
        self.assertEqual(merged_record["exported_kwh"], "0.1")
        self.assertEqual(merged_record["net_import_counter_kwh"], "10")
        self.assertEqual(merged_record["net_export_counter_kwh"], "1")
        for key in (
            "record_id",
            "device_timestamp_raw",
            "device_timestamp_utc",
            "retries",
            "received_at_utc",
            "acked_at_utc",
            "net_delta_net_kwh",
            "generation_delta_export_kwh",
            "consumption_delta_import_kwh",
            "phase_a_delta_import_kwh",
            "phase_b_delta_import_kwh",
        ):
            self.assertEqual(merged_record[key], default_record[key])
        status = build_status(self.cfg, self.db, None, NOW)
        self.assertEqual(status["merge"]["counts"], {"neurio": 0, "esp32": 1})
        self.assertEqual(status["merge"]["divergence_count"], 0)
        self.assertEqual(status["merge"]["worst_divergent"], [])
        self.assertIsNone(status["merge"]["error"])

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
        self.assertIsNone(merge_section["error"])


class ProductionProvenanceTests(unittest.TestCase):
    """The live hourly_backfill_provenance table, not the column the first
    merged-view tests invented."""

    def setUp(self):
        self.cfg = support.make_config()
        self.db = support.open_db(self)

    def test_fixture_columns_are_the_live_ones(self):
        add_provenance(self.db, H0, "neurio_cloud_stats")
        names = [
            row["name"]
            for row in self.db.conn.execute(
                "PRAGMA table_info(hourly_backfill_provenance)"
            )
        ]
        self.assertEqual(names, list(PROVENANCE_COLUMNS))
        self.assertNotIn("backfill_source", names)

    def test_readyz_status_and_merged_view_against_live_schema(self):
        # H0: cloud stats backfill, no cloud-table row.
        add_esp(self.db, self.cfg, H0, 0.9, 0.4)
        add_provenance(self.db, H0, "neurio_cloud_stats")
        # H1: HA counters plus a cloud-table hour that would look divergent
        # if the counter row were treated as the board.
        h1 = H0 + 3600
        add_esp(self.db, self.cfg, h1, 0.8, 0.3)
        add_provenance(self.db, h1, "neurio_counters_ha")
        add_cloud(self.db, h1, 1.5, 0.2)
        # H2: a real board row (source "device") beside a different cloud hour.
        h2 = H0 + 2 * 3600
        add_esp(self.db, self.cfg, h2, 1.3, 0.5)
        add_provenance(self.db, h2, "device")
        add_cloud(self.db, h2, 1.0, 0.5)
        # H3: corrected board row, no cloud hour.
        h3 = H0 + 3 * 3600
        add_esp(self.db, self.cfg, h3, 0.7, 0.2)
        add_provenance(self.db, h3, "device_corrected")

        ready_status, ready, _ = dispatch("/readyz", self.cfg, self.db, None, NOW)
        self.assertEqual(ready_status, 200, ready)
        self.assertTrue(ready["ready"])
        self.assertTrue(ready["db_ok"])
        self.assertTrue(ready["device_fresh"])

        status_code, status, _ = dispatch(
            "/api/v1/status", self.cfg, self.db, None, NOW
        )
        self.assertEqual(status_code, 200, status)
        self.assertTrue(status["db_ok"])
        self.assertTrue(status["device_fresh"])
        self.assertIsNone(status["merge"]["error"])
        self.assertEqual(status["merge"]["counts"], {"neurio": 3, "esp32": 1})
        self.assertEqual(status["merge"]["divergence_count"], 1)
        self.assertEqual(status["merge"]["divergent_hours"], 1)

        records = by_id(merged(self.db, self.cfg))
        stats_row = records[H0]
        self.assertEqual(stats_row["origin"], "neurio")
        self.assertEqual(stats_row["imported_kwh"], "10")
        self.assertIsNone(stats_row["net_import_counter_kwh"])
        self.assertNotIn("divergent_consumption", stats_row)

        counter_row = records[h1]
        self.assertEqual(counter_row["origin"], "neurio")
        self.assertEqual(counter_row["consumption_delta_import_kwh"], "1.5")
        self.assertEqual(counter_row["imported_kwh"], "1.5")
        self.assertEqual(counter_row["net_import_counter_kwh"], "10")
        self.assertEqual(counter_row["net_export_counter_kwh"], "1")
        self.assertNotIn("divergent_consumption", counter_row)

        device_row = records[h2]
        self.assertEqual(device_row["origin"], "neurio")
        self.assertEqual(device_row["imported_kwh"], "1")
        self.assertNotEqual(device_row["imported_kwh"], device_row["net_import_counter_kwh"])
        self.assertEqual(device_row["net_import_counter_kwh"], "10")
        self.assertTrue(device_row["divergent_consumption"])
        self.assertFalse(device_row["divergent_generation"])

        corrected = records[h3]
        self.assertEqual(corrected["origin"], "esp32")
        self.assertEqual(corrected["imported_kwh"], "0.4")
        self.assertEqual(corrected["net_import_counter_kwh"], "10")

        default_status, default_payload, _ = dispatch(
            "/api/v1/hourly", self.cfg, self.db, None, NOW
        )
        self.assertEqual(default_status, 200)
        self.assertNotIn("origin", default_payload["hourly_records"][0])
        self.assertEqual(default_payload["hourly_records"][0]["net_import_kwh"], "10")
        self.assertNotIn("imported_kwh", default_payload["hourly_records"][0])

    def test_bad_provenance_schema_does_not_fail_readiness(self):
        add_esp(self.db, self.cfg, H0, 0.8, 0.3)
        with self.db.write_lock:
            with self.db.transaction():
                self.db.conn.execute(
                    "CREATE TABLE hourly_backfill_provenance ("
                    "record_id INTEGER PRIMARY KEY, "
                    "backfill_source TEXT NOT NULL, "
                    "detail TEXT NOT NULL)"
                )
                self.db.conn.execute(
                    "INSERT INTO hourly_backfill_provenance VALUES(?,?,?)",
                    (H0, "neurio_cloud_stats", "wrong schema"),
                )
        ready_status, ready, _ = dispatch("/readyz", self.cfg, self.db, None, NOW)
        self.assertEqual(ready_status, 200, ready)
        self.assertTrue(ready["db_ok"])
        self.assertTrue(ready["device_fresh"])
        status_code, status, _ = dispatch(
            "/api/v1/status", self.cfg, self.db, None, NOW
        )
        self.assertEqual(status_code, 200, status)
        self.assertTrue(status["db_ok"])
        self.assertTrue(status["device_fresh"])
        self.assertEqual(status["schema_version"], "4")
        self.assertEqual(status["counts"]["hourly_records"], 1)
        self.assertEqual(status["merge"]["error"], "merge_unavailable")
        self.assertEqual(status["merge"]["counts"], {"neurio": None, "esp32": None})
        merged_status, merged_payload, _ = dispatch(
            "/api/v1/hourly?view=merged", self.cfg, self.db, None, NOW
        )
        self.assertEqual(merged_status, 200, merged_payload)
        (record,) = merged_payload["hourly_records"]
        # Provenance could not be read, so the hour is not labeled cloud.
        self.assertEqual(record["origin"], "esp32")
        self.assertEqual(record["imported_kwh"], "0.4")
        default_status, default_payload, _ = dispatch(
            "/api/v1/hourly", self.cfg, self.db, None, NOW
        )
        self.assertEqual(default_status, 200)
        self.assertEqual(default_payload["hourly_records"][0]["net_import_kwh"], "10")


if __name__ == "__main__":
    unittest.main()
