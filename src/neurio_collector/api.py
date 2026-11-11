"""Read-only HTTP API for stored hourly energy records.

POST, PUT, PATCH, and DELETE are rejected. Freshness is the age of the last
hourly message and does not by itself fail readiness. Readiness is the
database. Liveness is the process.
"""

from __future__ import annotations

import json
import sqlite3
from datetime import datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

from .config import Config
from .db import SCHEMA_VERSION
from .timeutil import parse_instant, utcnow
from .units import ws_to_kwh_str

MAX_LIMIT = 1000
_HOURLY_RECEIVED_UNSET = object()
# Placeholder stored when a raw payload had no sensorId. Not a device id.
MISSING_SENSOR_ID = "__payload_missing_sensor_id__"


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
                "status": "/api/v1/status",
                "channels": "/api/v1/channels",
                "hourly": "/api/v1/hourly",
            }, "json"
        if route == "/api/v1/status":
            return 200, build_status(cfg, db, poller, now), "json"
        if route == "/api/v1/channels":
            return 200, list_channels(db), "json"
        if route == "/api/v1/hourly":
            return 200, list_hourly_records(cfg, db, query), "json"
        return 404, {"error": "not found"}, "json"
    except ValueError as exc:
        return 400, {"error": str(exc)}, "json"
    except sqlite3.Error:
        return 503, {"error": "database unavailable"}, "json"


def build_status(cfg: Config, db, poller, now: datetime) -> dict:
    db_ok = True
    schema = None
    hourly_count = None
    hourly = {
        "stored": None,
        "acked": None,
        "last_record_id": None,
        "last_received_at_utc": None,
        "last_acked_at_utc": None,
    }
    try:
        reader = db.reader()
        try:
            row = reader.execute(
                "SELECT value FROM meta WHERE key='schema_version'"
            ).fetchone()
            schema = None if row is None else row["value"]
            hourly_count = reader.execute(
                "SELECT COUNT(*) AS n FROM hourly_records"
            ).fetchone()["n"]
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
        finally:
            reader.close()
    except sqlite3.Error:
        db_ok = False
    fresh, age = _hourly_fresh(
        cfg,
        db if db_ok else None,
        poller,
        now,
        hourly["last_received_at_utc"] if db_ok else None,
    )
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
        "mqtt_hourly_messages": 0,
        "last_hourly_message_at": None,
    }
    return {
        "process_ok": True,
        "db_ok": db_ok,
        "storage_writable": snap.get("storage_writable", True),
        "storage_error": snap.get("storage_error"),
        "device_fresh": fresh,
        "device_age_seconds": age,
        "staleness_threshold_seconds": cfg.hourly_stale_after_seconds,
        "schema_version": schema,
        "counts": {"hourly_records": hourly_count},
        "poll": snap,
        "hourly_records": hourly,
        "source_mode": cfg.source_mode,
        "mqtt_relay": {
            "broker_host": cfg.mqtt_broker_host,
            "broker_port": cfg.mqtt_broker_port,
            "tls": cfg.mqtt_tls,
            "hourly_topic": cfg.mqtt_hourly_topic,
            "hourly_messages": snap.get("mqtt_hourly_messages"),
            "last_hourly_message_at_utc": snap.get("last_hourly_message_at"),
            "ack_topic": cfg.mqtt_ack_topic,
        },
        "source_built": True,
        "unit_tested_claim": "fixture tests are not live verification; this process does not re-run the suite",
        "docker_image_built": False,
        "deployed": False,
    }


def list_channels(db) -> dict:
    """Sensor identity for clients that do not already know it.

    rate-plan-tool calls ``GET /api/v1/channels`` only to learn the sensor id.
    The channels table is gone, so each preserved ``sensors`` row is returned
    as one object. ``sensor_id`` is the stored id. ``sensor_token`` is not
    selected and is not returned. Channel type, number, identity, and label
    were columns on ``channels`` and are null.
    """
    reader = db.reader()
    try:
        rows = reader.execute(
            "SELECT sensor_id, first_seen_utc, last_seen_utc FROM sensors ORDER BY id"
        ).fetchall()
    finally:
        reader.close()
    channels = []
    for row in rows:
        item = {
            "identity_key": None,
            "type": None,
            "ch": None,
            "first_seen_utc": row["first_seen_utc"],
            "last_seen_utc": row["last_seen_utc"],
            "reading_notice": _notice(row["sensor_id"]),
            "label": None,
        }
        if row["sensor_id"] != MISSING_SENSOR_ID:
            item["sensor_id"] = row["sensor_id"]
        channels.append(item)
    return {"channels": channels}


def _notice(sensor_id: str) -> str:
    if sensor_id.startswith("SYNTHETIC"):
        return "synthetic_fixture"
    if sensor_id == MISSING_SENSOR_ID:
        return "unidentified_payload"
    return "device_collector"


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


def _ready(cfg: Config, db, poller, now: datetime):
    try:
        body = build_status(cfg, db, poller, now)
    except sqlite3.Error:
        return 503, {"ready": False, "db_ok": False, "process_ok": True}, "json"
    if not body["db_ok"] or body["schema_version"] != SCHEMA_VERSION:
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


def _hourly_fresh(cfg, db, poller, now, hourly_received_at=_HOURLY_RECEIVED_UNSET):
    """device_fresh from the last hourly message.

    A running process uses the poller's last_hourly_message_at. If this process
    has not seen an hourly message yet, including just after a restart, it uses
    the newest hourly_records.received_at_utc. The threshold is
    NEURIO_HOURLY_STALE_AFTER_SECONDS (default 3900).
    """
    anchor = _poller_hourly_message_at(poller)
    if anchor is None:
        if hourly_received_at is _HOURLY_RECEIVED_UNSET:
            anchor = None if db is None else _latest_hourly_received_at(db)
        else:
            anchor = hourly_received_at
    if not anchor:
        return False, None
    age = (now - parse_instant(anchor)).total_seconds()
    return age <= cfg.hourly_stale_after_seconds, age


def _poller_hourly_message_at(poller):
    if poller is None:
        return None
    value = poller.snapshot().get("last_hourly_message_at")
    if isinstance(value, str) and value.strip():
        return value.strip()
    return None


def _latest_hourly_received_at(db):
    reader = db.reader()
    try:
        row = reader.execute(
            "SELECT MAX(received_at_utc) AS last_received FROM hourly_records"
        ).fetchone()
    finally:
        reader.close()
    if row is None:
        return None
    return row["last_received"]


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
