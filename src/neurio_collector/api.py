"""Read-only HTTP API and CSV exports.

POST, PUT, PATCH, and DELETE are rejected. Device freshness is reported and does
not by itself fail readiness. Readiness is the database. Liveness is the process.
"""

from __future__ import annotations

import csv
import io
import json
import sqlite3
from datetime import datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

from .config import Config
from .energy import build_energy_report
from .observed import (
    BECAME_UNAVAILABLE_CHICAGO,
    CANDIDATE_DEVICE_URL,
    LAST_VALID_NET_CHICAGO,
    MISSING_SENSOR_ID,
)
from .poller import FRESH_OUTCOMES
from .rpt_contract import contract_flags
from .timeutil import format_utc, parse_instant, utcnow
from .units import ws_to_kwh_str

MAX_LIMIT = 1000


def make_server(cfg: Config, db, poller=None) -> ThreadingHTTPServer:
    handler = _handler_factory(cfg, db, poller)
    server = ThreadingHTTPServer((cfg.bind_address, cfg.bind_port), handler)
    server.daemon_threads = True
    return server


def _handler_factory(cfg: Config, db, poller):
    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def do_GET(self):
            status, payload, content_type = dispatch(self.path, cfg, db, poller)
            _send(self, status, payload, content_type)

        def do_POST(self):
            _method_not_allowed(self)

        def do_PUT(self):
            _method_not_allowed(self)

        def do_PATCH(self):
            _method_not_allowed(self)

        def do_DELETE(self):
            _method_not_allowed(self)

        def log_message(self, fmt, *args):
            return

    return Handler


def dispatch(path: str, cfg: Config, db, poller, now: datetime | None = None):
    now = now or utcnow()
    parsed = urlparse(path)
    route = parsed.path
    query = {key: values[-1] for key, values in parse_qs(parsed.query).items()}
    try:
        if route == "/healthz":
            return 200, {"status": "ok", "process_ok": True}, "json"
        if route == "/readyz":
            return _ready(cfg, db, poller, now)
        if route == "/":
            return 200, {
                "service": "neurio-lan-collector",
                "read_only": True,
                "rateplantool_contract": False,
                "status": "/api/v1/status",
            }, "json"
        if route == "/api/v1/status":
            return 200, build_status(cfg, db, poller, now), "json"
        if route == "/api/v1/latest":
            return 200, latest_payload(cfg, db, now), "json"
        if route == "/api/v1/channels":
            return 200, {"channels": list_channels(cfg, db)}, "json"
        if route == "/api/v1/samples":
            return 200, list_samples(cfg, db, query), "json"
        if route == "/api/v1/spans":
            return 200, list_spans(cfg, db, query), "json"
        if route == "/api/v1/energy":
            return 200, _energy(db, cfg, query), "json"
        if route == "/api/v1/hourly":
            return 200, list_hourly_records(cfg, db, query), "json"
        if route == "/api/v1/export/samples.csv":
            return 200, samples_csv(cfg, db, query), "csv"
        if route == "/api/v1/export/spans.csv":
            return 200, spans_csv(cfg, db, query), "csv"
        if route == "/api/v1/export/general-hourly.csv":
            report = _energy(db, cfg, {**query, "interval": query.get("interval", "hour")})
            return 200, hourly_csv(report), "csv"
        if route == "/api/v1/export/general-household-hourly.csv":
            report = _energy(db, cfg, {**query, "interval": query.get("interval", "hour")})
            return 200, household_csv(report), "csv"
        return 404, {"error": "not found"}, "json"
    except ValueError as exc:
        return 400, {"error": str(exc)}, "json"
    except sqlite3.Error:
        return 503, {"error": "database unavailable"}, "json"


def build_status(cfg: Config, db, poller, now: datetime) -> dict:
    db_ok = True
    schema = None
    counts = {"samples": None, "spans": None, "channels": None, "hourly_records": None}
    hourly = {
        "stored": None,
        "acked": None,
        "last_record_id": None,
        "last_received_at_utc": None,
        "last_acked_at_utc": None,
    }
    last_poll = None
    try:
        reader = db.reader()
        try:
            row = reader.execute(
                "SELECT value FROM meta WHERE key='schema_version'"
            ).fetchone()
            schema = None if row is None else row["value"]
            counts = {
                "samples": reader.execute("SELECT COUNT(*) AS n FROM samples").fetchone()["n"],
                "spans": reader.execute("SELECT COUNT(*) AS n FROM energy_spans").fetchone()["n"],
                "channels": reader.execute("SELECT COUNT(*) AS n FROM channels").fetchone()["n"],
                "hourly_records": reader.execute(
                    "SELECT COUNT(*) AS n FROM hourly_records"
                ).fetchone()["n"],
            }
            agg = reader.execute(
                "SELECT COUNT(*) AS stored, "
                "SUM(CASE WHEN acked_at_utc IS NOT NULL THEN 1 ELSE 0 END) AS acked, "
                "MAX(record_id) AS last_id, MAX(received_at_utc) AS last_received, "
                "MAX(acked_at_utc) AS last_acked FROM hourly_records"
            ).fetchone()
            hourly = {
                "stored": agg["stored"],
                "acked": agg["acked"] or 0,
                "last_record_id": agg["last_id"],
                "last_received_at_utc": agg["last_received"],
                "last_acked_at_utc": agg["last_acked"],
            }
            last_poll = reader.execute(
                "SELECT outcome, finished_at_utc, error, http_status FROM poll_attempts "
                "ORDER BY id DESC LIMIT 1"
            ).fetchone()
        finally:
            reader.close()
    except sqlite3.Error:
        db_ok = False
    fresh, age = _freshness(last_poll, cfg, now)
    if cfg.source_mode == "homeassistant" and db_ok:
        fresh, age = _ha_feed_fresh(db, cfg, now)
    snap = poller.snapshot() if poller is not None else {
        "running": False,
        "in_flight": False,
        "consecutive_failures": None,
        "current_backoff_seconds": None,
        "last_attempt_started_at": None,
        "last_success_at": None,
        "last_outcome": None,
        "last_error": None,
        "storage_writable": True,
        "storage_error": None,
    }
    device = urlparse(cfg.neurio_url)
    return {
        "process_ok": True,
        "db_ok": db_ok,
        "storage_writable": snap.get("storage_writable", True),
        "storage_error": snap.get("storage_error"),
        "device_fresh": fresh,
        "device_age_seconds": age,
        "staleness_threshold_seconds": cfg.stale_after_seconds,
        "schema_version": schema,
        "counts": counts,
        "poll": snap,
        "last_poll": None if last_poll is None else {
            "outcome": last_poll["outcome"],
            "finished_at_utc": last_poll["finished_at_utc"],
            "error": last_poll["error"],
            "http_status": last_poll["http_status"],
        },
        "device_endpoint": {
            "host": device.hostname,
            "port": device.port or 80,
            "path": device.path,
            "reachability": "not claimed by this process",
        },
        "topology_verified": cfg.topology_verified,
        "consumption_source": cfg.consumption_source,
        "poll_interval_seconds": cfg.poll_interval_seconds,
        "ha_poll_modified": False,
        "source_mode": cfg.source_mode,
        "neurio_polling": cfg.source_mode == "device",
        "fallback_to_device": False,
        "ha_relay": None if cfg.source_mode != "homeassistant" else {
            "entity_id": cfg.ha_entity,
            "source_id": cfg.ha_source_id,
            "url_host": urlparse(cfg.ha_url or "").hostname,
            "token_configured": cfg.ha_token is not None,
            "device_timestamp": "unknown",
        },
        "hourly_records": hourly,
        "mqtt_relay": None if cfg.source_mode != "mqtt" else {
            "broker_host": cfg.mqtt_broker_host,
            "broker_port": cfg.mqtt_broker_port,
            "tls": cfg.mqtt_tls,
            "raw_topic": cfg.mqtt_raw_topic,
            "raw_topic_enabled": bool(
                cfg.mqtt_raw_topic and cfg.mqtt_raw_topic.strip()
            ),
            "raw_messages": snap.get("mqtt_raw_messages"),
            "last_raw_message_at_utc": snap.get("last_raw_message_at"),
            "hourly_topic": cfg.mqtt_hourly_topic,
            "hourly_messages": snap.get("mqtt_hourly_messages"),
            "last_hourly_message_at_utc": snap.get("last_hourly_message_at"),
            "ack_topic": cfg.mqtt_ack_topic,
        },
        "source_built": True,
        "unit_tested_claim": "fixture tests are not live verification; this process does not re-run the suite",
        "docker_image_built": False,
        "deployed": False,
        "lan_device_verified": False,
        **contract_flags(),
        "observations": {
            "last_valid_net_chicago": LAST_VALID_NET_CHICAGO,
            "became_unavailable_chicago": BECAME_UNAVAILABLE_CHICAGO,
            "candidate_url": CANDIDATE_DEVICE_URL,
            "candidate_basis": (
                "Example LAN current-sample URL for documentation. "
                "Set NEURIO_URL to the physical device. This is not a reachability proof."
            ),
            "note": (
                "The Home Assistant feed was reporting until "
                f"{LAST_VALID_NET_CHICAGO} and became unavailable at "
                f"{BECAME_UNAVAILABLE_CHICAGO}. This is a recent interruption, "
                "not a statement that the feed never worked."
            ),
        },
    }


def latest_payload(cfg: Config, db, now: datetime) -> dict:
    reader = db.reader()
    try:
        stored = _sample_by_sql(reader, cfg, "ORDER BY samples.id DESC LIMIT 1")
        applied = _sample_by_sql(
            reader,
            cfg,
            "WHERE samples.applied_to_counters=1 ORDER BY samples.id DESC LIMIT 1",
        )
        last_poll = reader.execute(
            "SELECT outcome, finished_at_utc FROM poll_attempts ORDER BY id DESC LIMIT 1"
        ).fetchone()
    finally:
        reader.close()
    fresh, age = _freshness(last_poll, cfg, now)
    if cfg.source_mode == "homeassistant":
        fresh, age = _ha_feed_fresh(db, cfg, now)
    return {
        "device_fresh": fresh,
        "device_age_seconds": age,
        "redacted": cfg.redact_identity,
        "last_stored_sample": stored,
        "last_applied_sample": applied,
    }


def list_channels(cfg: Config, db) -> list[dict]:
    reader = db.reader()
    try:
        rows = reader.execute(
            "SELECT c.identity_key, c.type, c.ch, c.label, c.first_seen_utc, c.last_seen_utc, "
            "sen.sensor_token, sen.sensor_id FROM channels c "
            "JOIN sensors sen ON sen.id=c.sensor_pk ORDER BY sen.sensor_token, c.identity_key"
        ).fetchall()
    finally:
        reader.close()
    result = []
    for row in rows:
        item = {
            "sensor_token": row["sensor_token"],
            "identity_key": row["identity_key"],
            "type": row["type"],
            "ch": row["ch"],
            "first_seen_utc": row["first_seen_utc"],
            "last_seen_utc": row["last_seen_utc"],
            "reading_notice": _notice(row["sensor_id"]),
        }
        if not cfg.redact_identity:
            item["label"] = row["label"]
            if row["sensor_id"] != MISSING_SENSOR_ID:
                item["sensor_id"] = row["sensor_id"]
        result.append(item)
    return result


def list_samples(cfg: Config, db, query: dict) -> dict:
    start, end = _range(query)
    limit, cursor = _page(query)
    reader = db.reader()
    try:
        sql = (
            "SELECT samples.id FROM samples JOIN sensors sen ON sen.id=samples.sensor_pk "
            "WHERE samples.received_at_utc>=? AND samples.received_at_utc<? AND samples.id>?"
        )
        args: list = [start, end, cursor]
        if query.get("sensor_token"):
            sql += " AND sen.sensor_token=?"
            args.append(query["sensor_token"])
        sql += " ORDER BY samples.id LIMIT ?"
        args.append(limit + 1)
        ids = [row["id"] for row in reader.execute(sql, args).fetchall()]
        more = len(ids) > limit
        ids = ids[:limit]
        samples = [_sample_by_id(reader, cfg, sample_id) for sample_id in ids]
    finally:
        reader.close()
    return {
        "samples": samples,
        "next_cursor": None if not more else str(ids[-1]),
    }


def list_spans(cfg: Config, db, query: dict) -> dict:
    start, end = _range(query)
    limit, cursor = _page(query)
    reader = db.reader()
    try:
        sql = (
            "SELECT s.id, s.direction, s.delta_ws, s.start_counter_ws, s.end_counter_ws, "
            "s.start_received_utc, s.end_received_utc, s.start_device_utc, s.end_device_utc, "
            "s.gap_seconds, s.time_basis, s.allocation_status, s.source_method, "
            "c.identity_key, c.type AS channel_type, sen.sensor_token, sen.sensor_id "
            "FROM energy_spans s JOIN channels c ON c.id=s.channel_pk "
            "JOIN sensors sen ON sen.id=c.sensor_pk "
            "WHERE s.end_received_utc>=? AND s.start_received_utc<? AND s.id>?"
        )
        args: list = [start, end, cursor]
        if query.get("sensor_token"):
            sql += " AND sen.sensor_token=?"
            args.append(query["sensor_token"])
        sql += " ORDER BY s.id LIMIT ?"
        args.append(limit + 1)
        rows = [dict(row) for row in reader.execute(sql, args).fetchall()]
    finally:
        reader.close()
    more = len(rows) > limit
    rows = rows[:limit]
    spans = []
    for row in rows:
        spans.append(_public_span(row, cfg))
    return {"spans": spans, "next_cursor": None if not more else str(rows[-1]["id"])}


def _millikwh_str(value) -> str | None:
    if value is None:
        return None
    return ws_to_kwh_str(value * 3600)


def list_hourly_records(cfg: Config, db, query: dict) -> dict:
    limit, cursor = _page(query)
    reader = db.reader()
    try:
        rows = [
            dict(row)
            for row in reader.execute(
                "SELECT record_id, device_timestamp_raw, device_timestamp_utc, retries, "
                "received_at_utc, acked_at_utc, net_import_millikwh, net_export_millikwh, "
                "net_delta_net_millikwh, generation_delta_export_millikwh, "
                "consumption_delta_import_millikwh, phase_a_delta_import_millikwh, "
                "phase_b_delta_import_millikwh FROM hourly_records "
                "WHERE record_id>? ORDER BY record_id LIMIT ?",
                (cursor, limit + 1),
            ).fetchall()
        ]
    finally:
        reader.close()
    more = len(rows) > limit
    rows = rows[:limit]
    records = [
        {
            "record_id": row["record_id"],
            "device_timestamp_raw": row["device_timestamp_raw"],
            "device_timestamp_utc": row["device_timestamp_utc"],
            "retries": row["retries"],
            "received_at_utc": row["received_at_utc"],
            "acked_at_utc": row["acked_at_utc"],
            "net_import_kwh": _millikwh_str(row["net_import_millikwh"]),
            "net_export_kwh": _millikwh_str(row["net_export_millikwh"]),
            "net_delta_net_kwh": _millikwh_str(row["net_delta_net_millikwh"]),
            "generation_delta_export_kwh": _millikwh_str(
                row["generation_delta_export_millikwh"]
            ),
            "consumption_delta_import_kwh": _millikwh_str(
                row["consumption_delta_import_millikwh"]
            ),
            "phase_a_delta_import_kwh": _millikwh_str(row["phase_a_delta_import_millikwh"]),
            "phase_b_delta_import_kwh": _millikwh_str(row["phase_b_delta_import_millikwh"]),
        }
        for row in rows
    ]
    return {
        "hourly_records": records,
        "next_cursor": None if not more else str(rows[-1]["record_id"]),
    }


def samples_csv(cfg: Config, db, query: dict) -> bytes:
    payload = list_samples(cfg, db, {**query, "limit": query.get("limit", "1000")})
    buffer = io.StringIO()
    writer = csv.writer(buffer)
    writer.writerow([
        "export_schema", "rateplantool_contract", "reading_notice", "sample_id",
        "received_at_utc", "device_timestamp_raw", "device_timestamp_utc", "clock_quality",
        "sequence_status", "applied_to_counters", "sensor_token", "identity_key",
        "channel_type", "ch", "p_w", "q_var", "v_v", "e_imp_ws", "e_exp_ws",
    ])
    for sample in payload["samples"]:
        channels = sample["channels"] or [None]
        for channel in channels:
            writer.writerow([
                "neurio-collector.samples.v1",
                "false",
                sample["reading_notice"],
                sample["id"],
                sample["received_at_utc"],
                sample["device_timestamp_raw"],
                sample["device_timestamp_utc"],
                sample["clock_quality"],
                sample["sequence_status"],
                sample["applied_to_counters"],
                sample["sensor_token"],
                None if channel is None else channel["identity_key"],
                None if channel is None else channel["type"],
                None if channel is None else channel["ch"],
                None if channel is None else channel["p_w"],
                None if channel is None else channel["q_var"],
                None if channel is None else channel["v_v"],
                None if channel is None else channel["e_imp_ws"],
                None if channel is None else channel["e_exp_ws"],
            ])
    return buffer.getvalue().encode("utf-8")


def spans_csv(cfg: Config, db, query: dict) -> bytes:
    payload = list_spans(cfg, db, {**query, "limit": query.get("limit", "1000")})
    buffer = io.StringIO()
    writer = csv.writer(buffer)
    writer.writerow([
        "export_schema", "rateplantool_contract", "reading_notice", "span_id",
        "sensor_token", "identity_key", "channel_type", "direction", "delta_ws",
        "start_counter_ws", "end_counter_ws", "start_received_utc", "end_received_utc",
        "start_device_utc", "end_device_utc", "gap_seconds", "time_basis",
        "allocation_status", "source_method",
    ])
    for span in payload["spans"]:
        writer.writerow([
            "neurio-collector.spans.v1",
            "false",
            span["reading_notice"],
            span["id"],
            span["sensor_token"],
            span["identity_key"],
            span["channel_type"],
            span["direction"],
            span["delta_ws"],
            span["start_counter_ws"],
            span["end_counter_ws"],
            span["start_received_utc"],
            span["end_received_utc"],
            span["start_device_utc"],
            span["end_device_utc"],
            span["gap_seconds"],
            span["time_basis"],
            span["allocation_status"],
            span["source_method"],
        ])
    return buffer.getvalue().encode("utf-8")


def hourly_csv(report: dict) -> bytes:
    buffer = io.StringIO()
    writer = csv.writer(buffer)
    writer.writerow([
        "export_schema", "rateplantool_contract", "reading_notice", "timezone",
        "bucket_start_utc", "bucket_end_utc", "bucket_start_local", "bucket_end_local",
        "utc_offset_start", "utc_offset_end", "duration_seconds", "local_date", "local_hour",
        "fold", "sensor_token", "identity_key", "channel_type", "allocated_import_ws",
        "allocated_export_ws", "allocated_net_ws", "allocated_import_kwh",
        "allocated_export_kwh", "allocated_net_kwh", "coverage", "source_method",
        "time_basis", "overlapping_unresolved_span_count",
    ])
    for bucket in report["buckets"]:
        channels = bucket["channels"] or [None]
        for channel in channels:
            writer.writerow([
                "neurio-collector.general-hourly.v1",
                "false",
                None if channel is None else channel.get("reading_notice"),
                report["timezone"],
                bucket["start_utc"],
                bucket["end_utc"],
                bucket["start_local"],
                bucket["end_local"],
                bucket["utc_offset_start"],
                bucket["utc_offset_end"],
                bucket["duration_seconds"],
                bucket["local_date"],
                bucket["local_hour"],
                bucket["fold"],
                None if channel is None else channel.get("sensor_token"),
                None if channel is None else channel.get("identity_key"),
                None if channel is None else channel.get("type"),
                None if channel is None else channel.get("allocated_import_ws"),
                None if channel is None else channel.get("allocated_export_ws"),
                None if channel is None else channel.get("allocated_net_ws"),
                None if channel is None else channel.get("allocated_import_kwh"),
                None if channel is None else channel.get("allocated_export_kwh"),
                None if channel is None else channel.get("allocated_net_kwh"),
                bucket["coverage"] if channel is None else channel.get("coverage"),
                None if channel is None else channel.get("source_method"),
                None if channel is None else channel.get("time_basis"),
                None if channel is None else channel.get("overlapping_unresolved_span_count"),
            ])
    return buffer.getvalue().encode("utf-8")


def household_csv(report: dict) -> bytes:
    buffer = io.StringIO()
    writer = csv.writer(buffer)
    fields = [
        "export_schema", "rateplantool_contract", "timezone", "bucket_start_utc",
        "bucket_end_utc", "bucket_start_local", "bucket_end_local", "utc_offset_start",
        "utc_offset_end", "duration_seconds", "local_date", "local_hour", "fold",
        "topology_verified", "provenance", "grid_import_kwh", "grid_export_kwh",
        "grid_net_kwh", "consumption_kwh", "generation_eimp_kwh", "generation_eexp_kwh",
        "generation_net_kwh", "generation_collapsed_kwh", "consumption_method", "reason",
    ]
    writer.writerow(fields)
    for bucket in report["buckets"]:
        household = bucket["household"]
        writer.writerow([
            "neurio-collector.general-household-hourly.v1",
            "false",
            report["timezone"],
            bucket["start_utc"],
            bucket["end_utc"],
            bucket["start_local"],
            bucket["end_local"],
            bucket["utc_offset_start"],
            bucket["utc_offset_end"],
            bucket["duration_seconds"],
            bucket["local_date"],
            bucket["local_hour"],
            bucket["fold"],
            household["topology_verified"],
            household["provenance"],
            household["grid_import_kwh"],
            household["grid_export_kwh"],
            household["grid_net_kwh"],
            household["consumption_kwh"],
            household["generation_eimp_kwh"],
            household["generation_eexp_kwh"],
            household["generation_net_kwh"],
            household["generation_collapsed_kwh"],
            household["consumption_method"],
            household["reason"],
        ])
    return buffer.getvalue().encode("utf-8")


def _ready(cfg: Config, db, poller, now: datetime):
    try:
        body = build_status(cfg, db, poller, now)
    except sqlite3.Error:
        return 503, {"ready": False, "db_ok": False, "process_ok": True}, "json"
    if not body["db_ok"] or body["schema_version"] not in {"1", "2"}:
        return 503, {"ready": False, "db_ok": False, "process_ok": True, "device_fresh": False}, "json"
    if body.get("storage_writable") is False:
        return 503, {
            "ready": False,
            "db_ok": True,
            "storage_writable": False,
            "storage_error": body.get("storage_error"),
            "process_ok": True,
            "device_fresh": body["device_fresh"],
        }, "json"
    return 200, {
        "ready": True,
        "db_ok": True,
        "storage_writable": True,
        "process_ok": True,
        "device_fresh": body["device_fresh"],
    }, "json"


def _ha_feed_fresh(db, cfg: Config, now: datetime):
    reader = db.reader()
    try:
        row = reader.execute(
            "SELECT ha_state, ha_last_reported_utc, ha_last_updated_utc FROM samples "
            "WHERE source_kind='homeassistant_relay' ORDER BY id DESC LIMIT 1"
        ).fetchone()
    finally:
        reader.close()
    if row is None or not row["ha_state"] or row["ha_state"].strip().lower() in {"unavailable", "unknown", "none"}:
        return False, None
    anchor = row["ha_last_reported_utc"] or row["ha_last_updated_utc"]
    if not anchor:
        return False, None
    age = (now - parse_instant(anchor)).total_seconds()
    return age <= cfg.stale_after_seconds and age >= 0, age


def _freshness(last_poll, cfg: Config, now: datetime):
    if last_poll is None or last_poll["outcome"] not in FRESH_OUTCOMES or not last_poll["finished_at_utc"]:
        return False, None
    finished = parse_instant(last_poll["finished_at_utc"])
    age = (now - finished).total_seconds()
    return age <= cfg.stale_after_seconds, age


def _energy(db, cfg: Config, query: dict) -> dict:
    reader = _ClosingReader(db.reader())
    try:
        return build_energy_report(reader, cfg, query)
    finally:
        reader.close()


class _ClosingReader:
    def __init__(self, conn):
        self.conn = conn
        self._used = False

    def execute(self, *args, **kwargs):
        self._used = True
        return self.conn.execute(*args, **kwargs)

    def close(self):
        self.conn.close()


def _sample_by_sql(reader, cfg: Config, tail: str):
    row = reader.execute(
        "SELECT samples.*, sen.sensor_token, sen.sensor_id FROM samples "
        "JOIN sensors sen ON sen.id=samples.sensor_pk " + tail
    ).fetchone()
    if row is None:
        return None
    return _public_sample(reader, cfg, row)


def _sample_by_id(reader, cfg: Config, sample_id: int):
    row = reader.execute(
        "SELECT samples.*, sen.sensor_token, sen.sensor_id FROM samples "
        "JOIN sensors sen ON sen.id=samples.sensor_pk WHERE samples.id=?",
        (sample_id,),
    ).fetchone()
    return _public_sample(reader, cfg, row)


def _public_sample(reader, cfg: Config, row) -> dict:
    channels = reader.execute(
        "SELECT sc.p_w, sc.q_var, sc.v_v, sc.e_imp_ws, sc.e_exp_ws, sc.field_quality, "
        "c.identity_key, c.type, c.ch, c.label FROM sample_channels sc "
        "JOIN channels c ON c.id=sc.channel_pk WHERE sc.sample_id=? ORDER BY c.identity_key",
        (row["id"],),
    ).fetchall()
    public_channels = []
    for channel in channels:
        item = {
            "identity_key": channel["identity_key"],
            "type": channel["type"],
            "ch": channel["ch"],
            "p_w": channel["p_w"],
            "q_var": channel["q_var"],
            "v_v": channel["v_v"],
            "e_imp_ws": channel["e_imp_ws"],
            "e_exp_ws": channel["e_exp_ws"],
            "field_quality": channel["field_quality"],
        }
        if not cfg.redact_identity:
            item["label"] = channel["label"]
        public_channels.append(item)
    body = {
        "id": row["id"],
        "sensor_token": row["sensor_token"],
        "reading_notice": _notice(row["sensor_id"]),
        "received_at_utc": row["received_at_utc"],
        "device_timestamp_raw": row["device_timestamp_raw"],
        "device_timestamp_utc": row["device_timestamp_utc"],
        "clock_quality": row["clock_quality"],
        "sequence_status": row["sequence_status"],
        "applied_to_counters": bool(row["applied_to_counters"]),
        "source_kind": row["source_kind"] if "source_kind" in row.keys() else "device",
        "ha_entity_id": row["ha_entity_id"] if "ha_entity_id" in row.keys() else None,
        "ha_state": row["ha_state"] if "ha_state" in row.keys() else None,
        "ha_last_changed_utc": row["ha_last_changed_utc"] if "ha_last_changed_utc" in row.keys() else None,
        "ha_last_updated_utc": row["ha_last_updated_utc"] if "ha_last_updated_utc" in row.keys() else None,
        "ha_last_reported_utc": row["ha_last_reported_utc"] if "ha_last_reported_utc" in row.keys() else None,
        "device_timestamp_known": row["device_timestamp_utc"] is not None,
        "channels": public_channels,
    }
    if not cfg.redact_identity and row["sensor_id"] != MISSING_SENSOR_ID:
        body["sensor_id"] = row["sensor_id"]
    return body


def _public_span(row: dict, cfg: Config) -> dict:
    return {
        "id": row["id"],
        "sensor_token": row["sensor_token"],
        "reading_notice": _notice(row["sensor_id"]),
        "identity_key": row["identity_key"],
        "channel_type": row["channel_type"],
        "direction": row["direction"],
        "delta_ws": row["delta_ws"],
        "start_counter_ws": row["start_counter_ws"],
        "end_counter_ws": row["end_counter_ws"],
        "start_received_utc": row["start_received_utc"],
        "end_received_utc": row["end_received_utc"],
        "start_device_utc": row["start_device_utc"],
        "end_device_utc": row["end_device_utc"],
        "gap_seconds": row["gap_seconds"],
        "time_basis": row["time_basis"],
        "allocation_status": row["allocation_status"],
        "source_method": row["source_method"],
    }


def _notice(sensor_id: str) -> str:
    if sensor_id.startswith("SYNTHETIC"):
        return "synthetic_fixture"
    if sensor_id == MISSING_SENSOR_ID:
        return "unidentified_payload"
    return "device_collector"


def _range(query: dict) -> tuple[str, str]:
    if "start" not in query or "end" not in query:
        raise ValueError("start and end are required")
    start = format_utc(parse_instant(query["start"]))
    end = format_utc(parse_instant(query["end"]))
    if start >= end:
        raise ValueError("end must be after start")
    return start, end


def _page(query: dict) -> tuple[int, int]:
    try:
        limit = int(query.get("limit", "100"))
        cursor = int(query.get("cursor", "0"))
    except ValueError as exc:
        raise ValueError("limit and cursor must be integers") from exc
    if limit < 1 or limit > MAX_LIMIT:
        raise ValueError(f"limit must be 1..{MAX_LIMIT}")
    if cursor < 0:
        raise ValueError("cursor must be >= 0")
    return limit, cursor


def _send(handler, status: int, payload, content_type: str) -> None:
    if content_type == "csv":
        body = payload if isinstance(payload, bytes) else str(payload).encode("utf-8")
        mime = "text/csv; charset=utf-8"
    else:
        if isinstance(payload, bytes):
            body = payload
        else:
            body = json.dumps(payload, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
        mime = "application/json; charset=utf-8"
    handler.send_response(status)
    handler.send_header("Content-Type", mime)
    handler.send_header("Content-Length", str(len(body)))
    handler.send_header("Cache-Control", "no-store")
    handler.send_header("X-Content-Type-Options", "nosniff")
    handler.end_headers()
    handler.wfile.write(body)


def _method_not_allowed(handler) -> None:
    body = b'{"error":"method not allowed"}'
    handler.send_response(405)
    handler.send_header("Allow", "GET")
    handler.send_header("Content-Type", "application/json; charset=utf-8")
    handler.send_header("Content-Length", str(len(body)))
    handler.end_headers()
    handler.wfile.write(body)
