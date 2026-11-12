"""Read-only HTTP API for stored hourly energy records.

POST, PUT, PATCH, and DELETE are rejected. Freshness is the age of the last
hourly message and does not by itself fail readiness. Readiness is the
database. Liveness is the process.
"""

from __future__ import annotations

import json
import math
import sqlite3
from datetime import datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

from .config import CLOUD_PUBLISHED_LIMIT_PER_HOUR, Config
from .db import SCHEMA_VERSION
from .hourly_merge import (
    CLOUD_STATS_SOURCE,
    existing_tables,
    is_divergent,
    pct_difference,
    provenance_by_record,
)
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
    cloud = {
        "configured": cfg.cloud_configured,
        "sensor_id": cfg.cloud_sensor_id,
        "harvest_from_utc": None,
        "harvest_through_utc": None,
        "harvest_complete": False,
        "last_attempt_at_utc": None,
        "last_success_at_utc": None,
        "last_error": None,
        "last_error_at_utc": None,
        "hours_stored": None,
        "oldest_hour_end_utc": None,
        "newest_hour_end_utc": None,
        "published_limit_per_hour": CLOUD_PUBLISHED_LIMIT_PER_HOUR,
        "max_requests_per_hour": cfg.cloud_max_requests_per_hour,
        "min_request_interval_seconds": cfg.cloud_request_interval_seconds,
        "backing_off": False,
        "backoff_until_utc": None,
    }
    merge = _empty_merge()
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
            # The cloud tables are created lazily by cloud ingest, so a
            # database that has never synced has no rows to report rather
            # than an error to hide.
            cloud_tables = {
                row[0]
                for row in reader.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' "
                    "AND name IN ('cloud_hourly_records', 'cloud_sync_state')"
                )
            }
            if "cloud_hourly_records" in cloud_tables:
                cloud_agg = reader.execute(
                    "SELECT COUNT(*) AS n, MIN(hour_end_utc) AS oldest, "
                    "MAX(hour_end_utc) AS newest FROM cloud_hourly_records"
                ).fetchone()
                cloud["hours_stored"] = cloud_agg["n"]
                cloud["oldest_hour_end_utc"] = cloud_agg["oldest"]
                cloud["newest_hour_end_utc"] = cloud_agg["newest"]
            else:
                cloud["hours_stored"] = 0
            if "cloud_sync_state" in cloud_tables:
                state_columns = _table_columns(reader, "cloud_sync_state")
                wanted = (
                    "sensor_id",
                    "harvest_from_utc",
                    "harvest_through_utc",
                    "harvest_complete",
                    "last_attempt_at_utc",
                    "last_success_at_utc",
                    "last_error",
                    "last_error_at_utc",
                    "backoff_until_utc",
                )
                selected = [name for name in wanted if name in state_columns]
                state_row = None
                if selected:
                    state_row = reader.execute(
                        "SELECT " + ", ".join(selected) + " FROM cloud_sync_state WHERE id=1"
                    ).fetchone()
                if state_row is not None:
                    if cloud["sensor_id"] is None and "sensor_id" in selected:
                        cloud["sensor_id"] = state_row["sensor_id"]
                    for name in (
                        "harvest_from_utc",
                        "harvest_through_utc",
                        "last_attempt_at_utc",
                        "last_success_at_utc",
                        "last_error",
                        "last_error_at_utc",
                        "backoff_until_utc",
                    ):
                        if name in selected:
                            cloud[name] = state_row[name]
                    if "harvest_complete" in selected:
                        cloud["harvest_complete"] = bool(state_row["harvest_complete"])
                    cloud["backing_off"] = _backing_off(cloud["backoff_until_utc"], now)
        finally:
            reader.close()
    except sqlite3.Error:
        db_ok = False
    # The merge summary is a separate read. A bad provenance schema, or
    # any other sqlite failure while summarizing the union, must not flip
    # db_ok or /readyz: the recorder's own hourly store is fine.
    if db_ok:
        merge = _load_merge(db)
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
        "cloud_ingest": cloud,
        "merge": merge,
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
    view = query.get("view")
    if view is not None:
        if view != "merged":
            raise ValueError("view must be 'merged'")
        return list_merged_hourly_records(cfg, db, query)
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


def list_merged_hourly_records(cfg: Config, db, query: dict) -> dict:
    """The dual-source merged view (?view=merged): the union of the
    bridge's hourly_records and the cloud's cloud_hourly_records, keyed
    by hour-end epoch, cloud-preferred per hour. Read-time only — the
    stored tables are never touched.

    ``imported_kwh`` / ``exported_kwh`` are per-hour grid energy on every
    row. ``net_import_counter_kwh`` / ``net_export_counter_kwh`` are
    lifetime counters. The default listing's ``net_import_kwh`` /
    ``net_export_kwh`` names are not used here, because on that route
    they are the lifetime counters.
    """
    limit, cursor = _page(query)
    # Provenance is its own read. A table that does not match the live
    # column list degrades to "no provenance" instead of failing the
    # request; /api/v1/status reports merge.error for the same condition.
    prov = _provenance_or_empty(db)
    reader = db.reader()
    try:
        tables = existing_tables(reader)
        if "cloud_hourly_records" in tables:
            key_rows = reader.execute(
                "SELECT k FROM (SELECT record_id AS k FROM hourly_records "
                "UNION SELECT hour_end_epoch AS k FROM cloud_hourly_records) "
                "WHERE k>? ORDER BY k LIMIT ?",
                (cursor, limit + 1),
            ).fetchall()
        else:
            key_rows = reader.execute(
                "SELECT record_id AS k FROM hourly_records "
                "WHERE record_id>? ORDER BY record_id LIMIT ?",
                (cursor, limit + 1),
            ).fetchall()
        keys = [row[0] for row in key_rows]
        more = len(keys) > limit
        keys = keys[:limit]
        esp_rows = {}
        cloud_rows = {}
        if keys:
            marks = ",".join("?" for _ in keys)
            esp_rows = {
                row["record_id"]: dict(row)
                for row in reader.execute(
                    "SELECT record_id, device_timestamp_raw, device_timestamp_utc, "
                    "retries, received_at_utc, acked_at_utc, raw_json, "
                    "net_import_millikwh, net_export_millikwh, net_delta_net_millikwh, "
                    "generation_delta_export_millikwh, "
                    "consumption_delta_import_millikwh, "
                    "phase_a_delta_import_millikwh, phase_b_delta_import_millikwh "
                    f"FROM hourly_records WHERE record_id IN ({marks})",
                    keys,
                )
            }
            if "cloud_hourly_records" in tables:
                cloud_rows = {
                    row["hour_end_epoch"]: dict(row)
                    for row in reader.execute(
                        "SELECT hour_end_epoch, hour_end_utc, "
                        "consumption_millikwh, generation_millikwh, "
                        "imported_millikwh, exported_millikwh, fetched_at_utc "
                        f"FROM cloud_hourly_records WHERE hour_end_epoch IN ({marks})",
                        keys,
                    )
                }
    finally:
        reader.close()
    records = [
        _merged_record(key, esp_rows.get(key), cloud_rows.get(key), prov.get(key))
        for key in keys
    ]
    return {
        "hourly_records": records,
        "next_cursor": None if not more else str(keys[-1]),
    }


def _merged_record(key, esp, cloud, prov_source: str | None) -> dict:
    """One merged hour.

    ``imported_kwh`` and ``exported_kwh`` are per-hour on every row (null
    when that hour has no per-hour import or export). ``net_import_counter_kwh``
    and ``net_export_counter_kwh`` are lifetime registers. Cloud stats win
    the per-hour fields when ``cloud_hourly_records`` has the hour.
    """
    if esp is not None:
        record = {
            "record_id": esp["record_id"],
            "device_timestamp_raw": esp["device_timestamp_raw"],
            "device_timestamp_utc": esp["device_timestamp_utc"],
            "retries": esp["retries"],
            "received_at_utc": esp["received_at_utc"],
            "acked_at_utc": esp["acked_at_utc"],
            "net_delta_net_kwh": _millikwh_str(esp["net_delta_net_millikwh"]),
            "generation_delta_export_kwh": _millikwh_str(
                esp["generation_delta_export_millikwh"]
            ),
            "consumption_delta_import_kwh": _millikwh_str(
                esp["consumption_delta_import_millikwh"]
            ),
            "phase_a_delta_import_kwh": _millikwh_str(
                esp["phase_a_delta_import_millikwh"]
            ),
            "phase_b_delta_import_kwh": _millikwh_str(
                esp["phase_b_delta_import_millikwh"]
            ),
        }
    else:
        # A cloud-only hour: the bridge never delivered it, so the
        # lifecycle fields describe the cloud fetch instead.
        record = {
            "record_id": key,
            "device_timestamp_raw": None,
            "device_timestamp_utc": cloud["hour_end_utc"],
            "retries": 0,
            "received_at_utc": cloud["fetched_at_utc"],
            "acked_at_utc": None,
            "net_delta_net_kwh": None,
            "generation_delta_export_kwh": None,
            "consumption_delta_import_kwh": None,
            "phase_a_delta_import_kwh": None,
            "phase_b_delta_import_kwh": None,
        }
    imported, exported = _per_hour_import_export(esp, cloud, prov_source)
    counter_import, counter_export = _lifetime_counters(esp, prov_source)
    record["imported_kwh"] = _millikwh_str(imported)
    record["exported_kwh"] = _millikwh_str(exported)
    record["net_import_counter_kwh"] = _millikwh_str(counter_import)
    record["net_export_counter_kwh"] = _millikwh_str(counter_export)
    if cloud is not None:
        record["origin"] = "neurio"
        if cloud["consumption_millikwh"] is not None:
            record["consumption_delta_import_kwh"] = _millikwh_str(
                cloud["consumption_millikwh"]
            )
        if cloud["generation_millikwh"] is not None:
            record["generation_delta_export_kwh"] = _millikwh_str(
                cloud["generation_millikwh"]
            )
        if cloud["imported_millikwh"] is not None and cloud["exported_millikwh"] is not None:
            record["net_delta_net_kwh"] = _millikwh_str(
                cloud["imported_millikwh"] - cloud["exported_millikwh"]
            )
        if esp is not None and prov_source is None:
            record["esp32_consumption_kwh"] = _millikwh_str(
                esp["consumption_delta_import_millikwh"]
            )
            record["esp32_generation_kwh"] = _millikwh_str(
                esp["generation_delta_export_millikwh"]
            )
            record["divergent_consumption"] = is_divergent(
                cloud["consumption_millikwh"],
                esp["consumption_delta_import_millikwh"],
            )
            record["divergent_generation"] = is_divergent(
                cloud["generation_millikwh"],
                esp["generation_delta_export_millikwh"],
            )
    elif prov_source is not None:
        # A backfill row inside hourly_records is cloud data, not the board.
        record["origin"] = "neurio"
    else:
        record["origin"] = "esp32"
    return record


def _per_hour_import_export(esp, cloud, prov_source):
    """Per-hour grid import and export, in milli-kWh, or None.

    A cloud-table row wins, including when its value is null: the board's
    lifetime counter must not leak into this field. A ``neurio_cloud_stats``
    backfill row stored the interval import/export in the net columns.
    A board row (and an HA-counter row) publishes the per-hour split only
    as ``net.delta_import_kwh`` / ``net.delta_export_kwh`` in the raw payload.
    """
    if cloud is not None:
        return cloud["imported_millikwh"], cloud["exported_millikwh"]
    if esp is None:
        return None, None
    if prov_source == CLOUD_STATS_SOURCE:
        return esp["net_import_millikwh"], esp["net_export_millikwh"]
    return _payload_net_deltas(esp.get("raw_json"))


def _lifetime_counters(esp, prov_source):
    """Lifetime net import/export registers, in milli-kWh.

    Present for a board row and for an HA-counter snapshot. Absent for a
    cloud-only hour and for a ``neurio_cloud_stats`` row, whose net columns
    are per-hour interval energy rather than a running counter.
    """
    if esp is None or prov_source == CLOUD_STATS_SOURCE:
        return None, None
    return esp["net_import_millikwh"], esp["net_export_millikwh"]


def _payload_net_deltas(raw_json):
    """``net.delta_import_kwh`` / ``net.delta_export_kwh`` from a stored payload.

    The bridge publishes those per-hour deltas next to the lifetime
    counters. They are not columns. A payload that does not carry them
    yields None rather than the lifetime counter.
    """
    if not isinstance(raw_json, str) or not raw_json:
        return None, None
    try:
        data = json.loads(raw_json)
    except json.JSONDecodeError:
        return None, None
    if not isinstance(data, dict):
        return None, None
    net = data.get("net")
    if not isinstance(net, dict):
        return None, None
    return (
        _kwh_number_to_millikwh(net.get("delta_import_kwh")),
        _kwh_number_to_millikwh(net.get("delta_export_kwh")),
    )


def _kwh_number_to_millikwh(value):
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    if isinstance(value, float) and not math.isfinite(value):
        return None
    return round(value * 1000)


def _empty_merge() -> dict:
    return {
        "counts": {"neurio": None, "esp32": None},
        "divergence_count": None,
        "divergent_hours": None,
        "worst_divergent": [],
        "error": None,
    }


def _load_merge(db) -> dict:
    """Merge summary, or an error object that leaves the recorder healthy.

    sqlite3.Error here is the merge section's own problem (the live
    failure was ``no such column: backfill_source``). It is not a failure
    of hourly_records, so the caller keeps db_ok true.
    """
    try:
        reader = db.reader()
        try:
            return _merge_status(reader)
        finally:
            reader.close()
    except sqlite3.Error:
        failed = _empty_merge()
        failed["error"] = "merge_unavailable"
        return failed


def _merge_status(reader) -> dict:
    """Stored hours by origin, plus divergence between board and cloud rows."""
    esp_ids = {
        row[0] for row in reader.execute("SELECT record_id FROM hourly_records")
    }
    prov = provenance_by_record(reader)
    prov_ids = set(prov) & esp_ids
    cloud_tables = existing_tables(reader)
    cloud_keys = set()
    divergent = []
    if "cloud_hourly_records" in cloud_tables:
        cloud_keys = {
            row[0]
            for row in reader.execute(
                "SELECT hour_end_epoch FROM cloud_hourly_records"
            )
        }
        for row in reader.execute(
            "SELECT h.record_id AS rid, "
            "h.consumption_delta_import_millikwh AS esp_c, "
            "h.generation_delta_export_millikwh AS esp_g, "
            "c.consumption_millikwh AS cloud_c, "
            "c.generation_millikwh AS cloud_g, "
            "c.hour_end_utc AS hour_end FROM hourly_records h "
            "JOIN cloud_hourly_records c "
            "ON c.hour_end_epoch = h.record_id"
        ):
            if row["rid"] in prov_ids:
                continue
            for quantity, cloud_v, esp_v in (
                ("consumption", row["cloud_c"], row["esp_c"]),
                ("generation", row["cloud_g"], row["esp_g"]),
            ):
                if is_divergent(cloud_v, esp_v):
                    divergent.append(
                        {
                            "hour_end_utc": row["hour_end"],
                            "quantity": quantity,
                            "cloud_kwh": _millikwh_str(cloud_v),
                            "esp32_kwh": _millikwh_str(esp_v),
                            "pct_difference": pct_difference(cloud_v, esp_v),
                        }
                    )
    # Worst first: an undefined percentage (zero cloud value
    # with a real difference) outranks any finite percentage.
    divergent.sort(
        key=lambda item: (
            0 if item["pct_difference"] is None else 1,
            -(item["pct_difference"] or 0.0),
            item["hour_end_utc"],
            item["quantity"],
        )
    )
    summary = _empty_merge()
    summary["counts"] = {
        "neurio": len(prov_ids | cloud_keys),
        "esp32": len(esp_ids - prov_ids - cloud_keys),
    }
    summary["divergence_count"] = len(divergent)
    summary["divergent_hours"] = len({item["hour_end_utc"] for item in divergent})
    summary["worst_divergent"] = divergent[:10]
    return summary


def _provenance_or_empty(db) -> dict:
    try:
        reader = db.reader()
        try:
            return provenance_by_record(reader)
        finally:
            reader.close()
    except sqlite3.Error:
        return {}


def _table_columns(reader, table: str) -> set:
    return {row["name"] for row in reader.execute(f"PRAGMA table_info({table})")}


def _backing_off(until_text, now: datetime) -> bool:
    if not until_text:
        return False
    try:
        return parse_instant(until_text) > now
    except ValueError:
        return False


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
