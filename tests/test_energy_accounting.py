import json
import unittest

import support
from neurio_collector.energy import build_energy_report
from neurio_collector.observed import SYNTHETIC_SENSOR_ID
from neurio_collector.timeutil import parse_instant


def five(imp_net, exp_net, imp_gen, exp_gen, imp_con, imp_a=500000, imp_b=500000):
    return [
        support.channel("PHASE_A_CONSUMPTION", 1, imp_a, 0, 586),
        support.channel("PHASE_B_CONSUMPTION", 2, imp_b, 0, 153),
        support.channel("NET", 3, imp_net, exp_net, 738),
        support.channel("GENERATION", 4, imp_gen, exp_gen, 0),
        support.channel("CONSUMPTION", 5, imp_con, 0, 739),
    ]


class EnergyAccountingTests(unittest.TestCase):
    def test_dedicated_consumption_negative_net_and_no_phase_sum(self):
        db = support.open_db(self)
        cfg = support.make_config(data_dir=db.path.rsplit("/", 1)[0])
        support.feed(
            db,
            cfg,
            [
                ("2026-07-15T17:00:00Z", "2026-07-15T17:00:00Z", five(0, 0, 0, 0, 0, 0, 0)),
                (
                    "2026-07-15T17:00:15Z",
                    "2026-07-15T17:00:15Z",
                    five(1000, 5000, 9000, 100, 8000, 50000, 40000),
                ),
            ],
        )
        report = build_energy_report(
            db.conn,
            cfg,
            {"date": "2026-07-15", "interval": "hour", "timezone": "America/Chicago"},
        )
        hour = next(bucket for bucket in report["buckets"] if bucket["start_utc"].startswith("2026-07-15T17:00"))
        by_id = {item["identity_key"]: item for item in hour["channels"]}
        household = hour["household"]
        self.assertEqual(household["grid_import_ws"], 1000)
        self.assertEqual(household["grid_export_ws"], 5000)
        self.assertEqual(household["grid_net_ws"], -4000)
        self.assertTrue(household["grid_net_kwh"].startswith("-"))
        self.assertEqual(household["consumption_ws"], 8000)
        self.assertEqual(household["consumption_method"], "dedicated_channel_eImp_minus_eExp")
        self.assertEqual(household["consumption_source"], "ch:5|type:CONSUMPTION")
        self.assertEqual(household["generation_eimp_ws"], 9000)
        self.assertEqual(household["generation_eexp_ws"], 100)
        self.assertIsNone(household["generation_collapsed_kwh"])
        self.assertFalse(household["topology_verified"])
        self.assertIn("max(net,0)", household["rejected_methods"])
        phase_sum = (
            by_id["ch:1|type:PHASE_A_CONSUMPTION"]["allocated_import_ws"]
            + by_id["ch:2|type:PHASE_B_CONSUMPTION"]["allocated_import_ws"]
        )
        all_import = sum(item["allocated_import_ws"] for item in hour["channels"])
        self.assertNotEqual(household["grid_import_ws"], phase_sum)
        self.assertNotEqual(household["consumption_ws"], phase_sum)
        self.assertNotEqual(household["consumption_ws"], all_import)
        self.assertNotEqual(household["grid_import_ws"], 0)
        derived = household["grid_net_ws"] + household["generation_net_ws"]
        self.assertNotEqual(household["consumption_ws"], derived)

    def test_derivation_is_opt_in_and_labeled(self):
        db = support.open_db(self)
        cfg = support.make_config(
            data_dir=db.path.rsplit("/", 1)[0],
            topology_verified=True,
            consumption_source="derive_net_plus_generation",
        )
        support.feed(
            db,
            cfg,
            [
                ("2026-07-15T17:00:00Z", "2026-07-15T17:00:00Z", five(0, 0, 0, 0, 0, 0, 0)),
                (
                    "2026-07-15T17:00:15Z",
                    "2026-07-15T17:00:15Z",
                    five(1000, 5000, 9000, 100, 8000, 1, 1),
                ),
            ],
        )
        report = build_energy_report(
            db.conn, cfg, {"date": "2026-07-15", "interval": "hour", "timezone": "America/Chicago"}
        )
        hour = next(bucket for bucket in report["buckets"] if bucket["start_utc"].startswith("2026-07-15T17:00"))
        self.assertEqual(hour["household"]["consumption_ws"], -4000 + 8900)
        self.assertEqual(hour["household"]["consumption_method"], "derived_net_plus_generation")
        self.assertNotEqual(hour["household"]["consumption_ws"], 8000)

    def test_ambiguous_net_does_not_fall_back_to_phases(self):
        db = support.open_db(self)
        cfg = support.make_config(data_dir=db.path.rsplit("/", 1)[0])
        channels_a = five(0, 0, 0, 0, 0, 0, 0) + [
            support.channel("NET", 9, 0, 0, 1)
        ]
        channels_b = five(1000, 0, 0, 0, 50, 9, 9) + [
            support.channel("NET", 9, 7000, 0, 1)
        ]
        support.feed(
            db,
            cfg,
            [
                ("2026-07-15T17:00:00Z", "2026-07-15T17:00:00Z", channels_a),
                ("2026-07-15T17:00:15Z", "2026-07-15T17:00:15Z", channels_b),
            ],
        )
        report = build_energy_report(
            db.conn, cfg, {"date": "2026-07-15", "interval": "hour", "timezone": "America/Chicago"}
        )
        hour = next(bucket for bucket in report["buckets"] if bucket["start_utc"].startswith("2026-07-15T17:00"))
        self.assertEqual(hour["household"]["reason"], "ambiguous_net")
        self.assertIsNone(hour["household"]["grid_import_ws"])
        self.assertEqual(hour["household"]["consumption_ws"], 50)

    def test_reset_duplicate_conflict_order_and_clock(self):
        db = support.open_db(self)
        cfg = support.make_config(data_dir=db.path.rsplit("/", 1)[0])
        net = lambda imp, exp=0: [support.channel("NET", 3, imp, exp), support.channel("CONSUMPTION", 5, imp, 0)]
        rows = [
            ("2026-07-15T17:00:00Z", "2026-07-15T17:00:00Z", net(100)),
            ("2026-07-15T17:00:15Z", "2026-07-15T17:00:15Z", net(150)),
            ("2026-07-15T17:00:30Z", "2026-07-15T17:00:30Z", net(10)),
            ("2026-07-15T17:00:45Z", "2026-07-15T17:00:45Z", net(15)),
        ]
        results = support.feed(db, cfg, rows)
        self.assertTrue(all(item.applied for item in results))
        replay = support.ingest_body(
            db,
            support.sample_bytes(SYNTHETIC_SENSOR_ID, rows[-1][1], rows[-1][2]),
            parse_instant("2026-07-15T17:01:00Z"),
            cfg,
        )
        self.assertEqual(replay.outcome, "duplicate")
        stale = support.ingest_body(
            db,
            support.sample_bytes(SYNTHETIC_SENSOR_ID, "2026-07-15T16:00:00Z", net(999)),
            parse_instant("2026-07-15T17:01:15Z"),
            cfg,
        )
        self.assertEqual(stale.sequence_status, "out_of_order")
        self.assertFalse(stale.applied)
        conflict = support.ingest_body(
            db,
            support.sample_bytes(SYNTHETIC_SENSOR_ID, "2026-07-15T17:00:45Z", net(400)),
            parse_instant("2026-07-15T17:01:30Z"),
            cfg,
        )
        self.assertEqual(conflict.sequence_status, "conflict")
        self.assertFalse(conflict.applied)
        continued = support.feed(
            db,
            cfg,
            [("2026-07-15T17:02:00Z", "2026-07-15T17:02:00Z", net(20))],
        )
        self.assertTrue(continued[0].applied)
        total = db.conn.execute(
            "SELECT COALESCE(SUM(delta_ws), 0) AS n FROM energy_spans s "
            "JOIN channels c ON c.id=s.channel_pk WHERE c.type='NET' AND s.direction='import'"
        ).fetchone()["n"]
        self.assertEqual(total, 60)
        epochs = db.conn.execute(
            "SELECT end_reason FROM counter_epochs e JOIN channels c ON c.id=e.channel_pk "
            "WHERE c.type='NET' AND e.direction='import' ORDER BY e.epoch_number"
        ).fetchall()
        self.assertEqual([row["end_reason"] for row in epochs], ["decrease", None])
        self.assertIsNone(
            db.conn.execute(
                "SELECT delta_ws FROM energy_spans WHERE delta_ws < 0"
            ).fetchone()
        )

    def test_gap_is_preserved_and_not_spread_across_hours(self):
        db = support.open_db(self)
        cfg = support.make_config(data_dir=db.path.rsplit("/", 1)[0])
        net = lambda imp: [support.channel("NET", 3, imp, 0), support.channel("CONSUMPTION", 5, imp, 0)]
        support.feed(
            db,
            cfg,
            [
                ("2026-07-15T17:00:00Z", "2026-07-15T17:00:00Z", net(1000)),
                ("2026-07-15T17:00:15Z", "2026-07-15T17:00:15Z", net(4600)),
                ("2026-07-15T20:00:15Z", "2026-07-15T20:00:15Z", net(4600 + 3_600_000)),
                ("2026-07-15T20:00:30Z", "2026-07-15T20:00:30Z", net(4600 + 3_600_000 + 1800)),
            ],
        )
        report = build_energy_report(
            db.conn,
            cfg,
            {
                "start": "2026-07-15T17:00:00Z",
                "end": "2026-07-15T21:00:00Z",
                "interval": "hour",
                "timezone": "America/Chicago",
            },
        )
        by_start = {bucket["start_utc"]: bucket for bucket in report["buckets"]}
        first = by_start["2026-07-15T17:00:00.000Z"]
        gap_hour = by_start["2026-07-15T18:00:00.000Z"]
        last = by_start["2026-07-15T20:00:00.000Z"]
        self.assertEqual(first["household"]["grid_import_ws"], 3600)
        self.assertIsNone(gap_hour["household"]["grid_import_ws"])
        self.assertEqual(gap_hour["coverage"], "gap")
        self.assertEqual(last["household"]["grid_import_ws"], 1800)
        preserved = next(
            item for item in report["window"]["preserved_wholly_inside"] if item["type"] == "NET"
        )
        self.assertEqual(preserved["preserved_ws_by_status"]["unresolved_gap"]["import"], 3_600_000)
        hourly = []
        for bucket in report["buckets"]:
            value = bucket["household"]["grid_import_ws"]
            if value is not None:
                hourly.append(value)
        self.assertEqual(sum(hourly), 5400)
        self.assertNotIn(3_600_000, hourly)

    def test_unsynchronized_and_receive_time_are_not_allocated(self):
        db = support.open_db(self)
        cfg = support.make_config(data_dir=db.path.rsplit("/", 1)[0])
        body1 = support.sample_bytes(SYNTHETIC_SENSOR_ID, "NOT_SYNCHRONIZED", [support.channel("NET", 3, 10, 0)])
        body2 = support.sample_bytes(SYNTHETIC_SENSOR_ID, "NOT_SYNCHRONIZED", [support.channel("NET", 3, 3610, 0)])
        from neurio_collector.ingest import ingest_body

        ingest_body(db, body1, parse_instant("2026-07-15T17:00:00Z"), cfg)
        ingest_body(db, body2, parse_instant("2026-07-15T17:00:15Z"), cfg)
        span = db.conn.execute(
            "SELECT delta_ws, allocation_status, time_basis FROM energy_spans"
        ).fetchone()
        self.assertEqual(span["delta_ws"], 3600)
        self.assertEqual(span["allocation_status"], "unresolved_clock")
        self.assertEqual(span["time_basis"], "receive")
        report = build_energy_report(
            db.conn, cfg, {"date": "2026-07-15", "interval": "hour", "timezone": "America/Chicago"}
        )
        hour = next(bucket for bucket in report["buckets"] if bucket["start_utc"].startswith("2026-07-15T17:00"))
        self.assertIsNone(hour["household"]["grid_import_ws"])
        preserved = report["window"]["preserved_wholly_inside"][0]
        self.assertEqual(preserved["preserved_ws_by_status"]["unresolved_clock"]["import"], 3600)

    def test_stale_device_timestamp_is_unresolved(self):
        db = support.open_db(self)
        cfg = support.make_config(data_dir=db.path.rsplit("/", 1)[0])
        support.feed(
            db,
            cfg,
            [
                ("2026-07-15T17:00:00Z", "2026-07-15T17:00:00Z", [support.channel("NET", 3, 0, 0)]),
                ("2026-07-15T17:10:15Z", "2026-07-15T17:00:15Z", [support.channel("NET", 3, 50, 0)]),
            ],
        )
        span = db.conn.execute("SELECT allocation_status, time_basis FROM energy_spans").fetchone()
        self.assertEqual(span["allocation_status"], "unresolved_clock")

    def test_observed_fixture_does_not_open_epochs(self):
        db = support.open_db(self)
        cfg = support.make_config(data_dir=db.path.rsplit("/", 1)[0])
        raw = (support.OBSERVED / "ha-recorder-neurio-raw-channels-2026-11-06.json").read_bytes()
        from neurio_collector.ingest import ingest_body

        result = ingest_body(db, raw, parse_instant("2026-11-07T01:40:00Z"), cfg)
        self.assertEqual(result.sequence_status, "identity_missing")
        self.assertFalse(result.applied)
        self.assertEqual(result.spans_created, 0)
        stored = db.conn.execute(
            "SELECT e_imp_ws, e_exp_ws, p_w FROM sample_channels sc "
            "JOIN channels c ON c.id=sc.channel_pk WHERE c.type='CONSUMPTION'"
        ).fetchone()
        self.assertEqual(stored["e_imp_ws"], 626227269168)
        self.assertEqual(stored["e_exp_ws"], 0)
        self.assertEqual(stored["p_w"], 739)
        net = db.conn.execute(
            "SELECT p_w FROM sample_channels sc JOIN channels c ON c.id=sc.channel_pk WHERE c.type='NET'"
        ).fetchone()
        self.assertEqual(net["p_w"], 738)

    def test_sensor_change_does_not_bridge_counters(self):
        db = support.open_db(self)
        cfg = support.make_config(data_dir=db.path.rsplit("/", 1)[0])
        support.feed(
            db,
            cfg,
            [
                ("2026-07-15T17:00:00Z", "2026-07-15T17:00:00Z", [support.channel("NET", 3, 100, 0)]),
                ("2026-07-15T17:00:15Z", "2026-07-15T17:00:15Z", [support.channel("NET", 3, 140, 0)]),
            ],
            sensor="SYNTHETIC-NOT-A-DEVICE",
        )
        support.feed(
            db,
            cfg,
            [
                ("2026-07-15T17:00:30Z", "2026-07-15T17:00:30Z", [support.channel("NET", 3, 5, 0)]),
                ("2026-07-15T17:00:45Z", "2026-07-15T17:00:45Z", [support.channel("NET", 3, 8, 0)]),
            ],
            sensor="SYNTHETIC-OTHER-DEVICE",
        )
        spans = db.conn.execute(
            "SELECT delta_ws FROM energy_spans WHERE direction='import' ORDER BY id"
        ).fetchall()
        self.assertEqual([row["delta_ws"] for row in spans], [40, 3])

    def test_boundary_span_is_not_split(self):
        db = support.open_db(self)
        cfg = support.make_config(data_dir=db.path.rsplit("/", 1)[0])
        support.feed(
            db,
            cfg,
            [
                ("2026-07-15T17:59:50Z", "2026-07-15T17:59:50Z", [support.channel("NET", 3, 0, 0)]),
                ("2026-07-15T17:59:55Z", "2026-07-15T17:59:55Z", [support.channel("NET", 3, 100, 0)]),
                ("2026-07-15T18:00:05Z", "2026-07-15T18:00:05Z", [support.channel("NET", 3, 150, 0)]),
                ("2026-07-15T18:00:15Z", "2026-07-15T18:00:15Z", [support.channel("NET", 3, 175, 0)]),
            ],
        )
        report = build_energy_report(
            db.conn,
            cfg,
            {
                "start": "2026-07-15T17:00:00Z",
                "end": "2026-07-15T19:00:00Z",
                "interval": "hour",
                "timezone": "America/Chicago",
            },
        )
        first, second = report["buckets"]
        self.assertEqual(first["household"]["grid_import_ws"], 100)
        self.assertEqual(second["household"]["grid_import_ws"], 25)
        preserved = report["window"]["preserved_wholly_inside"][0]
        self.assertEqual(preserved["crossing_hour_import_ws"], 50)
        self.assertEqual(preserved["preserved_ws_by_status"]["allocatable"]["import"], 175)
        self.assertEqual(report["window"]["excluded_crossing_window_span_count"], 0)


if __name__ == "__main__":
    unittest.main()
