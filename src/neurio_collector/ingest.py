"""Turn one payload into samples, epochs, and counter spans.

Counters are applied only for a stable sensor id, in receive order, and only
when the device timestamp is not a replay, a conflict, or an older sample.
A decrease opens a new epoch and does not create a negative span. A long gap
keeps the positive delta on the span and marks it unresolved so later queries
do not spread it across missing hours.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime

from .config import Config
from .db import Database
from .observed import MISSING_SENSOR_ID
from .parse import ParseError, ParsedSample, parse_payload
from .timeutil import format_utc

APPLY_STATUSES = {"accepted", "stale", "clock_uncertain"}


@dataclass
class IngestResult:
    outcome: str
    sample_id: int | None = None
    sequence_status: str | None = None
    applied: bool = False
    spans_created: int = 0
    error: str | None = None


def ingest_body(
    db: Database,
    body: bytes,
    received_at: datetime,
    cfg: Config,
    http_status: int | None = None,
    poll_latency_ms: int | None = None,
    source_kind: str = "device",
) -> IngestResult:
    try:
        parsed = parse_payload(body)
    except ParseError as exc:
        return IngestResult(outcome="parse_error", error=str(exc))
    raw_text = body.decode("utf-8")
    with db.write_lock:
        with db.transaction():
            return _apply(
                db, parsed, raw_text, received_at, cfg, http_status, poll_latency_ms, source_kind
            )


def _apply(
    db: Database,
    parsed: ParsedSample,
    raw_text: str,
    received_at: datetime,
    cfg: Config,
    http_status: int | None,
    poll_latency_ms: int | None,
    source_kind: str = "device",
) -> IngestResult:
    conn = db.conn
    received_s = format_utc(received_at)
    device_s = format_utc(parsed.device_timestamp_utc) if parsed.device_timestamp_utc else None
    sensor_key = parsed.sensor_id or MISSING_SENSOR_ID
    token = hashlib.sha256(sensor_key.encode("utf-8")).hexdigest()[:16]
    sensor_pk = _upsert_sensor(conn, sensor_key, token, received_s)
    existing = conn.execute(
        "SELECT id FROM samples WHERE sensor_pk=? AND payload_sha256=?",
        (sensor_pk, parsed.payload_sha256),
    ).fetchone()
    if existing is not None:
        return IngestResult(
            outcome="duplicate",
            sample_id=existing["id"],
            sequence_status="duplicate",
            applied=False,
        )
    status, applied = _sequence(conn, sensor_pk, parsed, device_s, received_s, cfg)
    if parsed.sensor_id is None:
        status = "identity_missing"
        applied = False
    sample_id = _insert_sample(
        conn,
        sensor_pk,
        received_s,
        parsed,
        device_s,
        status,
        raw_text,
        http_status,
        poll_latency_ms,
        applied,
        {"source_kind": source_kind},
    )
    spans = 0
    for channel in parsed.channels:
        channel_pk = _upsert_channel(conn, sensor_pk, channel, received_s)
        conn.execute(
            "INSERT INTO sample_channels(sample_id, channel_pk, p_w, q_var, v_v, "
            "e_imp_ws, e_exp_ws, field_quality, flags_json) VALUES(?,?,?,?,?,?,?,?,?)",
            (
                sample_id,
                channel_pk,
                channel.p_w,
                channel.q_var,
                channel.v_v,
                channel.e_imp_ws,
                channel.e_exp_ws,
                channel.field_quality,
                json.dumps(channel.flags),
            ),
        )
        if not applied:
            continue
        for direction, counter in (("import", channel.e_imp_ws), ("export", channel.e_exp_ws)):
            if counter is None:
                continue
            spans += _advance_counter(
                conn,
                channel_pk,
                direction,
                counter,
                sample_id,
                received_s,
                device_s,
                parsed,
                status,
                cfg,
            )
    for ct in parsed.cts:
        conn.execute(
            "INSERT INTO sample_cts(sample_id, ct, p_w, q_var, v_v) VALUES(?,?,?,?,?)",
            (sample_id, ct.ct, ct.p_w, ct.q_var, ct.v_v),
        )
    return IngestResult(
        outcome="stored",
        sample_id=sample_id,
        sequence_status=status,
        applied=applied,
        spans_created=spans,
    )


def _sequence(conn, sensor_pk, parsed: ParsedSample, device_s, received_s, cfg: Config):
    last = conn.execute(
        "SELECT received_at_utc, device_timestamp_utc FROM samples "
        "WHERE sensor_pk=? AND applied_to_counters=1 ORDER BY id DESC LIMIT 1",
        (sensor_pk,),
    ).fetchone()
    if last is not None and received_s < last["received_at_utc"]:
        return "out_of_order", False
    if device_s is not None:
        row = conn.execute(
            "SELECT id, payload_sha256 FROM samples WHERE sensor_pk=? AND applied_to_counters=1 "
            "AND device_timestamp_utc=?",
            (sensor_pk, device_s),
        ).fetchone()
        if row is not None and row["payload_sha256"] != parsed.payload_sha256:
            return "conflict", False
        latest = conn.execute(
            "SELECT MAX(device_timestamp_utc) AS mx FROM samples "
            "WHERE sensor_pk=? AND applied_to_counters=1 AND device_timestamp_utc IS NOT NULL",
            (sensor_pk,),
        ).fetchone()
        if latest and latest["mx"] and device_s < latest["mx"]:
            return "out_of_order", False
    skew_recv = None
    skew_dev = None
    if parsed.device_timestamp_utc is not None:
        delta = (parsed.device_timestamp_utc - _as_dt(received_s)).total_seconds()
        # received - device
        skew_recv = -delta
        skew_dev = delta
    if parsed.clock_quality != "synchronized":
        return "clock_uncertain", True
    if skew_recv is not None and skew_recv > cfg.sample_stale_skew_seconds:
        return "stale", True
    if skew_dev is not None and skew_dev > cfg.device_ahead_skew_seconds:
        return "clock_uncertain", True
    return "accepted", True


def _as_dt(text: str) -> datetime:
    from .timeutil import parse_instant

    return parse_instant(text)


def _advance_counter(
    conn,
    channel_pk,
    direction,
    counter,
    sample_id,
    received_s,
    device_s,
    parsed: ParsedSample,
    status,
    cfg: Config,
    basis_override=None,
) -> int:
    epoch = conn.execute(
        "SELECT * FROM counter_epochs WHERE channel_pk=? AND direction=? AND ended_received_utc IS NULL",
        (channel_pk, direction),
    ).fetchone()
    if epoch is None:
        number = conn.execute(
            "SELECT COALESCE(MAX(epoch_number), 0) + 1 AS n FROM counter_epochs "
            "WHERE channel_pk=? AND direction=?",
            (channel_pk, direction),
        ).fetchone()["n"]
        conn.execute(
            "INSERT INTO counter_epochs(channel_pk, direction, epoch_number, started_received_utc, "
            "started_device_utc, start_counter_ws, start_sample_id, last_counter_ws, last_sample_id, "
            "last_received_utc, last_device_utc) VALUES(?,?,?,?,?,?,?,?,?,?,?)",
            (
                channel_pk,
                direction,
                number,
                received_s,
                device_s,
                counter,
                sample_id,
                counter,
                sample_id,
                received_s,
                device_s,
            ),
        )
        return 0
    if counter < epoch["last_counter_ws"]:
        conn.execute(
            "UPDATE counter_epochs SET ended_received_utc=?, end_reason='decrease' WHERE id=?",
            (received_s, epoch["id"]),
        )
        conn.execute(
            "INSERT INTO counter_epochs(channel_pk, direction, epoch_number, started_received_utc, "
            "started_device_utc, start_counter_ws, start_sample_id, last_counter_ws, last_sample_id, "
            "last_received_utc, last_device_utc) VALUES(?,?,?,?,?,?,?,?,?,?,?)",
            (
                channel_pk,
                direction,
                epoch["epoch_number"] + 1,
                received_s,
                device_s,
                counter,
                sample_id,
                counter,
                sample_id,
                received_s,
                device_s,
            ),
        )
        return 0
    prev = conn.execute(
        "SELECT clock_quality, sequence_status, device_timestamp_utc, received_at_utc "
        "FROM samples WHERE id=?",
        (epoch["last_sample_id"],),
    ).fetchone()
    delta = counter - epoch["last_counter_ws"]
    if basis_override is None:
        gap, basis, allocation = _classify(prev, parsed, status, device_s, received_s, cfg)
        place_start, place_end = None, None
    else:
        gap, basis, allocation, place_start, place_end = basis_override
    quality = {
        "sequence_status": status,
        "clock_quality": parsed.clock_quality,
        "previous_clock_quality": prev["clock_quality"],
        "allocation_status": allocation,
    }
    conn.execute(
        "INSERT INTO energy_spans(channel_pk, direction, epoch_id, start_sample_id, end_sample_id, "
        "start_received_utc, end_received_utc, start_device_utc, end_device_utc, start_counter_ws, "
        "end_counter_ws, delta_ws, gap_seconds, time_basis, allocation_status, quality_json, "
        "source_method, placement_start_utc, placement_end_utc) "
        "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        (
            channel_pk,
            direction,
            epoch["id"],
            epoch["last_sample_id"],
            sample_id,
            epoch["last_received_utc"],
            received_s,
            epoch["last_device_utc"],
            device_s,
            epoch["last_counter_ws"],
            counter,
            delta,
            gap,
            basis,
            allocation,
            json.dumps(quality),
            "counter_delta",
            place_start,
            place_end,
        ),
    )
    conn.execute(
        "UPDATE counter_epochs SET last_counter_ws=?, last_sample_id=?, last_received_utc=?, "
        "last_device_utc=? WHERE id=?",
        (counter, sample_id, received_s, device_s, epoch["id"]),
    )
    return 1


def _classify(prev, parsed: ParsedSample, status: str, device_s, received_s, cfg: Config):
    recv_gap = _seconds(prev["received_at_utc"], received_s)
    dev_gap = _seconds(prev["device_timestamp_utc"], device_s)
    clock_issue = (
        status in {"stale", "clock_uncertain"}
        or prev["sequence_status"] in {"stale", "clock_uncertain"}
        or parsed.clock_quality != "synchronized"
        or prev["clock_quality"] != "synchronized"
        or dev_gap is None
        or dev_gap < 0
    )
    if clock_issue:
        return recv_gap, "receive", "unresolved_clock"
    if recv_gap is not None and recv_gap > cfg.max_allocatable_gap_seconds and dev_gap <= cfg.max_allocatable_gap_seconds:
        return dev_gap, "device", "unresolved_clock"
    if dev_gap > cfg.max_allocatable_gap_seconds:
        return dev_gap, "device", "unresolved_gap"
    return dev_gap, "device", "allocatable"


def _seconds(start: str | None, end: str | None) -> float | None:
    if not start or not end:
        return None
    return (_as_dt(end) - _as_dt(start)).total_seconds()


def _upsert_sensor(conn, sensor_id: str, token: str, seen: str) -> int:
    row = conn.execute("SELECT id FROM sensors WHERE sensor_id=?", (sensor_id,)).fetchone()
    if row is not None:
        conn.execute("UPDATE sensors SET last_seen_utc=? WHERE id=?", (seen, row["id"]))
        return row["id"]
    cur = conn.execute(
        "INSERT INTO sensors(sensor_id, sensor_token, first_seen_utc, last_seen_utc) VALUES(?,?,?,?)",
        (sensor_id, token, seen, seen),
    )
    return cur.lastrowid


def _upsert_channel(conn, sensor_pk: int, channel, seen: str) -> int:
    row = conn.execute(
        "SELECT id FROM channels WHERE sensor_pk=? AND identity_key=?",
        (sensor_pk, channel.identity_key),
    ).fetchone()
    if row is not None:
        conn.execute(
            "UPDATE channels SET last_seen_utc=?, label=COALESCE(label, ?) WHERE id=?",
            (seen, channel.label, row["id"]),
        )
        return row["id"]
    cur = conn.execute(
        "INSERT INTO channels(sensor_pk, ch, type, label, identity_key, first_seen_utc, last_seen_utc) "
        "VALUES(?,?,?,?,?,?,?)",
        (sensor_pk, channel.ch, channel.type, channel.label, channel.identity_key, seen, seen),
    )
    return cur.lastrowid


def _insert_sample(
    conn,
    sensor_pk,
    received_s,
    parsed: ParsedSample,
    device_s,
    status,
    raw_text,
    http_status,
    poll_latency_ms,
    applied,
    provenance: dict | None = None,
) -> int:
    provenance = provenance or {}
    cur = conn.execute(
        "INSERT INTO samples(sensor_pk, received_at_utc, device_timestamp_raw, device_timestamp_utc, "
        "clock_quality, sequence_status, payload_sha256, raw_json, raw_retained, http_status, "
        "poll_latency_ms, applied_to_counters, flags_json, source_kind, ha_entity_id, ha_state, "
        "ha_last_changed_utc, ha_last_updated_utc, ha_last_reported_utc) "
        "VALUES(?,?,?,?,?,?,?,?,1,?,?,?,?,?,?,?,?,?,?)",
        (
            sensor_pk,
            received_s,
            parsed.device_timestamp_raw,
            device_s,
            parsed.clock_quality,
            status,
            parsed.payload_sha256,
            raw_text,
            http_status,
            poll_latency_ms,
            1 if applied else 0,
            json.dumps(parsed.flags),
            provenance.get("source_kind", "device"),
            provenance.get("ha_entity_id"),
            provenance.get("ha_state"),
            provenance.get("ha_last_changed_utc"),
            provenance.get("ha_last_updated_utc"),
            provenance.get("ha_last_reported_utc"),
        ),
    )
    return cur.lastrowid
