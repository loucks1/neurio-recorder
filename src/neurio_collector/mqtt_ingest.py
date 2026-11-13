"""Store hourly energy records published by the ESPHome Neurio bridge.

The bridge publishes one JSON record per hour on the hourly topic (default
``neurio/hourly/energy``) and keeps the record queued until the consumer
acknowledges it on the ACK topic. Storing here is idempotent on the record
id: a redelivery of an already-stored record reports ``duplicate`` so the
caller can acknowledge it again. A payload that does not parse reports
``parse_error`` and must not be acknowledged, so the bridge keeps retrying
it instead of dropping an hour of energy data.

Energy values are stored as integer milli-kWh. The payload carries three
decimal places, so the conversion is exact.

A payload may carry a top-level boolean ``ack_required``: the bridge
saying whether it expects this record to be acknowledged. It is stored
as 1/0 in hourly_records.ack_required (NULL when absent or not a
boolean — never a parse error) so the status endpoint can tell
"not yet ACKed" apart from "no ACK was ever expected". The ACK itself
is published either way; the flag only classifies.
"""

from __future__ import annotations

import json
import math
from dataclasses import dataclass
from datetime import datetime

from .config import Config
from .db import Database
from .timeutil import format_utc, parse_instant

MAX_RECORD_ID = 4294967295


@dataclass
class HourlyResult:
    outcome: str
    record_id: int | None = None
    error: str | None = None


def _millikwh(value) -> int | None:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    if isinstance(value, float) and not math.isfinite(value):
        return None
    return round(value * 1000)


def _section(data: dict, name: str) -> dict:
    section = data.get(name)
    return section if isinstance(section, dict) else {}


def store_hourly_record(
    db: Database, body: bytes, received_at: datetime, cfg: Config
) -> HourlyResult:
    try:
        text = body.decode("utf-8")
    except UnicodeDecodeError:
        return HourlyResult(outcome="parse_error", error="body is not utf-8")
    try:
        data = json.loads(text)
    except json.JSONDecodeError:
        return HourlyResult(outcome="parse_error", error="body is not json")
    if not isinstance(data, dict):
        return HourlyResult(outcome="parse_error", error="hourly record must be an object")
    record_id = data.get("id")
    if (
        isinstance(record_id, bool)
        or not isinstance(record_id, int)
        or not 0 <= record_id <= MAX_RECORD_ID
    ):
        return HourlyResult(
            outcome="parse_error", error="hourly record id must be an unsigned 32-bit integer"
        )
    net = _section(data, "net")
    if _millikwh(net.get("delta_net_kwh")) is None:
        return HourlyResult(
            outcome="parse_error", error="net.delta_net_kwh must be a number"
        )
    retries = data.get("retries")
    if isinstance(retries, bool) or not isinstance(retries, int) or retries < 0:
        retries = 0
    ack_flag = data.get("ack_required")
    ack_required = (1 if ack_flag else 0) if isinstance(ack_flag, bool) else None
    raw_ts = data.get("timestamp")
    raw_ts_text = raw_ts if isinstance(raw_ts, str) else None
    device_ts = None
    if raw_ts_text:
        try:
            device_ts = format_utc(parse_instant(raw_ts_text))
        except ValueError:
            device_ts = None
    values = (
        _millikwh(net.get("import_kwh")),
        _millikwh(net.get("export_kwh")),
        _millikwh(net.get("delta_net_kwh")),
        _millikwh(_section(data, "generation").get("delta_export_kwh")),
        _millikwh(_section(data, "consumption").get("delta_import_kwh")),
        _millikwh(_section(data, "phase_a").get("delta_import_kwh")),
        _millikwh(_section(data, "phase_b").get("delta_import_kwh")),
    )
    with db.write_lock:
        with db.transaction():
            conn = db.conn
            existing = conn.execute(
                "SELECT record_id FROM hourly_records WHERE record_id=?", (record_id,)
            ).fetchone()
            if existing is not None:
                return HourlyResult(outcome="duplicate", record_id=record_id)
            conn.execute(
                "INSERT INTO hourly_records(record_id, device_timestamp_raw, "
                "device_timestamp_utc, retries, received_at_utc, raw_json, "
                "net_import_millikwh, net_export_millikwh, net_delta_net_millikwh, "
                "generation_delta_export_millikwh, consumption_delta_import_millikwh, "
                "phase_a_delta_import_millikwh, phase_b_delta_import_millikwh, "
                "ack_required) "
                "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    record_id,
                    raw_ts_text,
                    device_ts,
                    retries,
                    format_utc(received_at),
                    text,
                    *values,
                    ack_required,
                ),
            )
    return HourlyResult(outcome="stored", record_id=record_id)


def mark_hourly_acked(db: Database, record_id: int, when: datetime) -> None:
    with db.write_lock:
        with db.transaction():
            db.conn.execute(
                "UPDATE hourly_records SET acked_at_utc=? "
                "WHERE record_id=? AND acked_at_utc IS NULL",
                (format_utc(when), record_id),
            )
