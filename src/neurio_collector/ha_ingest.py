"""Store one Home Assistant state for sensor.neurio_raw.

The entity has no Neurio sensorId and no device sample timestamp. Those stay
null. Ordering and gap checks use HA last_updated, labeled as such. Repeating
the same HA snapshot does not create another energy span. An unavailable state
does not move counters. A long gap keeps the counter delta unresolved.
"""

from __future__ import annotations

import hashlib
import json
from datetime import datetime

from .config import Config
from .ha_fetch import scrub
from .ingest import IngestResult, _advance_counter, _insert_sample, _seconds, _upsert_channel, _upsert_sensor
from .parse import ParseError, parse_payload
from .timeutil import format_utc, parse_instant

UNAVAILABLE = {"unavailable", "unknown", "none", ""}


def ingest_ha_state(
    db,
    body: bytes,
    received_at: datetime,
    cfg: Config,
    http_status: int | None = None,
    poll_latency_ms: int | None = None,
) -> IngestResult:
    text = scrub(body.decode("utf-8", errors="replace"), cfg.ha_token)
    try:
        state = json.loads(text)
    except json.JSONDecodeError:
        return IngestResult(outcome="parse_error", error="body is not json")
    if not isinstance(state, dict):
        return IngestResult(outcome="parse_error", error="state must be an object")
    entity = state.get("entity_id") or cfg.ha_entity
    if entity != cfg.ha_entity:
        return IngestResult(outcome="parse_error", error="entity_id does not match HA_ENTITY")
    attributes = state.get("attributes") if isinstance(state.get("attributes"), dict) else {}
    channels = attributes.get("channels")
    if channels is None:
        channel_body = {"channels": []}
        flags_extra = ["channels_missing"]
    elif not isinstance(channels, list):
        return IngestResult(outcome="parse_error", error="attributes.channels must be a list")
    else:
        channel_body = {"channels": channels}
        flags_extra = []
    try:
        parsed = parse_payload(json.dumps(channel_body).encode("utf-8"))
    except ParseError as exc:
        return IngestResult(outcome="parse_error", error=str(exc))
    parsed.sensor_id = None
    parsed.device_timestamp_raw = None
    parsed.device_timestamp_utc = None
    parsed.clock_quality = "device_timestamp_unknown"
    parsed.flags = [
        "source_homeassistant_relay",
        "device_timestamp_unknown",
        "not_a_physical_current_sample",
        "sensor_id_not_from_device",
        *flags_extra,
        *parsed.flags,
    ]
    changed = _parse_ha_time(state.get("last_changed"))
    updated = _parse_ha_time(state.get("last_updated"))
    reported = _parse_ha_time(state.get("last_reported"))
    ha_state = state.get("state")
    ha_state_text = ha_state if isinstance(ha_state, str) else None
    digest = hashlib.sha256(
        json.dumps(
            {
                "entity_id": entity,
                "state": ha_state_text,
                "last_changed": state.get("last_changed"),
                "last_updated": state.get("last_updated"),
                "last_reported": state.get("last_reported"),
                "channels": channels if isinstance(channels, list) else None,
            },
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()
    parsed.payload_sha256 = digest
    provenance = {
        "source_kind": "homeassistant_relay",
        "ha_entity_id": entity,
        "ha_state": ha_state_text,
        "ha_last_changed_utc": changed[0],
        "ha_last_updated_utc": updated[0],
        "ha_last_reported_utc": reported[0],
    }
    received_s = format_utc(received_at)
    with db.write_lock:
        with db.transaction():
            return _apply_ha(
                db,
                parsed,
                text,
                received_s,
                cfg,
                http_status,
                poll_latency_ms,
                provenance,
                updated[0],
                reported[0],
                ha_state_text,
            )


def _apply_ha(
    db,
    parsed,
    raw_text,
    received_s,
    cfg: Config,
    http_status,
    poll_latency_ms,
    provenance,
    updated_s,
    reported_s,
    ha_state,
) -> IngestResult:
    conn = db.conn
    source_id = cfg.ha_source_id or ""
    token = hashlib.sha256(source_id.encode("utf-8")).hexdigest()[:16]
    sensor_pk = _upsert_sensor(conn, source_id, token, received_s)
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
    unavailable = ha_state is None or ha_state.strip().lower() in UNAVAILABLE
    if unavailable or not parsed.channels:
        sample_id = _store(conn, sensor_pk, parsed, raw_text, received_s, "unavailable", False, provenance, http_status, poll_latency_ms)
        return IngestResult(outcome="stored", sample_id=sample_id, sequence_status="unavailable", applied=False)
    if updated_s is None:
        sample_id = _store(
            conn, sensor_pk, parsed, raw_text, received_s, "provenance_missing", False, provenance, http_status, poll_latency_ms
        )
        return IngestResult(
            outcome="stored", sample_id=sample_id, sequence_status="provenance_missing", applied=False
        )
    last = conn.execute(
        "SELECT ha_last_updated_utc FROM samples WHERE sensor_pk=? AND applied_to_counters=1 "
        "ORDER BY id DESC LIMIT 1",
        (sensor_pk,),
    ).fetchone()
    if last is not None and last["ha_last_updated_utc"] and updated_s < last["ha_last_updated_utc"]:
        sample_id = _store(conn, sensor_pk, parsed, raw_text, received_s, "out_of_order", False, provenance, http_status, poll_latency_ms)
        return IngestResult(outcome="stored", sample_id=sample_id, sequence_status="out_of_order", applied=False)
    if last is not None and last["ha_last_updated_utc"] == updated_s:
        sample_id = _store(conn, sensor_pk, parsed, raw_text, received_s, "conflict", False, provenance, http_status, poll_latency_ms)
        return IngestResult(outcome="stored", sample_id=sample_id, sequence_status="conflict", applied=False)
    age_anchor = reported_s or updated_s
    age = _seconds(age_anchor, received_s)
    stale_feed = age is None or age > cfg.stale_after_seconds or age < 0
    status = "stale_feed" if stale_feed else "accepted"
    sample_id = _store(conn, sensor_pk, parsed, raw_text, received_s, status, True, provenance, http_status, poll_latency_ms)
    spans = 0
    previous_updated = last["ha_last_updated_utc"] if last is not None else None
    gap = _seconds(previous_updated, updated_s) if previous_updated else None
    if stale_feed:
        basis = ("ha_last_updated", "unresolved_stale_feed", gap, previous_updated, updated_s)
    elif gap is None:
        basis = None
    elif gap > cfg.max_allocatable_gap_seconds:
        basis = ("ha_last_updated", "unresolved_gap", gap, previous_updated, updated_s)
    else:
        basis = ("ha_last_updated", "allocatable", gap, previous_updated, updated_s)
    for channel in parsed.channels:
        channel_pk = _upsert_channel(conn, sensor_pk, channel, received_s)
        conn.execute(
            "INSERT INTO sample_channels(sample_id, channel_pk, p_w, q_var, v_v, e_imp_ws, e_exp_ws, "
            "field_quality, flags_json) VALUES(?,?,?,?,?,?,?,?,?)",
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
        for direction, counter in (("import", channel.e_imp_ws), ("export", channel.e_exp_ws)):
            if counter is None:
                continue
            override = None if basis is None else (basis[2], basis[0], basis[1], basis[3], basis[4])
            spans += _advance_counter(
                conn,
                channel_pk,
                direction,
                counter,
                sample_id,
                received_s,
                None,
                parsed,
                status,
                cfg,
                basis_override=override,
            )
    return IngestResult(outcome="stored", sample_id=sample_id, sequence_status=status, applied=True, spans_created=spans)


def _store(conn, sensor_pk, parsed, raw_text, received_s, status, applied, provenance, http_status, poll_latency_ms):
    return _insert_sample(
        conn,
        sensor_pk,
        received_s,
        parsed,
        None,
        status,
        raw_text,
        http_status,
        poll_latency_ms,
        applied,
        provenance,
    )


def _parse_ha_time(value):
    if not isinstance(value, str) or not value.strip():
        return None, "missing"
    try:
        parsed = parse_instant(value)
    except ValueError:
        return None, "invalid"
    return format_utc(parsed), "ok"
