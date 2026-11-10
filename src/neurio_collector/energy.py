"""Read counter spans into hour, day, and range reports.

A span contributes its watt-seconds to a bucket only when both endpoints lie
inside that bucket. Gaps and unsynchronized spans stay on the span and in the
preserved-by-status totals. They are not divided across hours and missing hours
stay null. Household consumption is the dedicated CONSUMPTION channel
(eImp - eExp). It is not max(net, 0), not the sum of phases, and not
net+generation unless that derivation was explicitly enabled after topology
verification. Grid import and export are the NET channel counters.
"""

from __future__ import annotations

import json
from datetime import date, datetime, timedelta

from .config import Config
from .observed import MISSING_SENSOR_ID, OBSERVED_CHANNEL_MAP
from .timeutil import day_bucket, format_utc, hours_for_local_date, iter_utc_hours, load_zone, parse_instant
from .units import ws_to_kwh_str, ws_to_wh_str

REJECTED_METHODS = [
    "max(net,0)",
    "sum(phase_channels)",
    "derive_net_plus_generation",
]


def build_energy_report(conn, cfg: Config, params: dict[str, str]) -> dict:
    zone = load_zone(params.get("timezone") or cfg.timezone)
    interval = params.get("interval") or "hour"
    if interval not in {"hour", "day", "range"}:
        raise ValueError("interval must be hour, day, or range")
    buckets, window_start, window_end = _buckets(params, zone, interval)
    if interval == "hour" and len(buckets) > 800:
        raise ValueError("hour queries are limited to 800 buckets")
    sensor_token = params.get("sensor_token")
    spans = _load_spans(conn, window_start, window_end, sensor_token)
    sensors = {row["sensor_token"] for row in spans}
    channel_rows = _channels_for(conn, sensor_token, sensors)
    report_buckets = []
    window_acc: dict[str, dict] = {}
    excluded = 0
    bucket_acc = [dict() for _ in buckets]
    window_start_s = format_utc(window_start)
    window_end_s = format_utc(window_end)
    for span in spans:
        start_s, end_s = _placement(span)
        wholly = start_s >= window_start_s and end_s <= window_end_s
        overlaps = start_s < window_end_s and end_s > window_start_s
        if not wholly:
            if overlaps:
                excluded += 1
            continue
        acc = window_acc.setdefault(span["identity_key"], _new_acc(span))
        _add_status(acc, span)
        placed = False
        for index, bucket in enumerate(buckets):
            if start_s >= bucket["start_utc"] and end_s <= bucket["end_utc"]:
                bacc = bucket_acc[index].setdefault(span["identity_key"], _new_acc(span))
                _add_status(bacc, span)
                if span["allocation_status"] == "allocatable":
                    _add_allocated(bacc, span)
                else:
                    bacc["unresolved_inside"] += 1
                placed = True
                break
        for index, bucket in enumerate(buckets):
            if start_s >= bucket["start_utc"] and end_s <= bucket["end_utc"]:
                continue
            if start_s < bucket["end_utc"] and end_s > bucket["start_utc"]:
                bacc = bucket_acc[index].setdefault(span["identity_key"], _new_acc(span))
                if span["allocation_status"] == "allocatable":
                    bacc["boundary_allocatable"] += 1
                else:
                    bacc["problem_overlap"] += 1
        if span["allocation_status"] == "allocatable" and not placed:
            _add_named(acc, "crossing_hour", span)
    rendered = []
    for index, bucket in enumerate(buckets):
        rendered.append(
            _render_bucket(bucket, bucket_acc[index], channel_rows, cfg, sensors)
        )
    return {
        "export_schema": "neurio-collector.energy.v1",
        "rateplantool_contract": False,
        "reading_origin": "neurio_lan_collector",
        "timezone": str(zone),
        "interval": interval,
        "start_utc": window_start_s,
        "end_utc": window_end_s,
        "bucket_count": len(rendered),
        "topology_verified": cfg.topology_verified,
        "household_channel_reads": cfg.household_channel_reads,
        "consumption_source": cfg.consumption_source,
        "observed_channel_map_reference": [
            {"type": typ, "ch": ch} for typ, ch in OBSERVED_CHANNEL_MAP
        ],
        "buckets": rendered,
        "window": {
            "preserved_wholly_inside": [_render_preserved(item) for item in window_acc.values()],
            "excluded_crossing_window_span_count": excluded,
        },
        "notes": [
            "Allocated watt-seconds are counter deltas wholly inside one bucket.",
            "Unresolved gaps and unsynchronized spans stay in preserved_by_status and are not spread across hours.",
            "Null means unknown. Zero means a measured zero delta.",
            "Consumption uses the dedicated CONSUMPTION channel when that source is selected.",
            "Grid import and export use the NET channel eImp_Ws and eExp_Ws counters.",
            "Phase channels are not added to household totals.",
            "This export is not a RatePlanTool import contract.",
        ],
    }


def _buckets(params, zone, interval):
    if params.get("date"):
        day = date.fromisoformat(params["date"])
        if interval == "hour":
            buckets = hours_for_local_date(day, zone)
            return buckets, buckets[0]["start"], buckets[-1]["end"]
        if interval == "day":
            bucket = day_bucket(day, zone)
            return [bucket], bucket["start"], bucket["end"]
        raise ValueError("date= is valid for interval=hour or interval=day")
    if not params.get("start") or not params.get("end"):
        raise ValueError("provide date= or start= and end=")
    start = parse_instant(params["start"])
    end = parse_instant(params["end"])
    if start >= end:
        raise ValueError("end must be after start")
    if interval == "range":
        bucket = {
            "start": start,
            "end": end,
            "start_utc": format_utc(start),
            "end_utc": format_utc(end),
            "start_local": start.astimezone(zone).isoformat(),
            "end_local": end.astimezone(zone).isoformat(),
            "utc_offset_start": start.astimezone(zone).isoformat()[-6:],
            "utc_offset_end": end.astimezone(zone).isoformat()[-6:],
            "fold": start.astimezone(zone).fold,
            "duration_seconds": int((end - start).total_seconds()),
            "local_date": start.astimezone(zone).date().isoformat(),
            "local_hour": None,
        }
        return [bucket], start, end
    if interval == "hour":
        if (end - start) > timedelta(days=40):
            raise ValueError("hour range is limited to 40 days")
        buckets = iter_utc_hours(start, end, zone)
        return buckets, start, end
    raise ValueError("interval=day requires date=")


def _load_spans(conn, start: datetime, end: datetime, sensor_token: str | None):
    start_s = format_utc(start - timedelta(days=2))
    end_s = format_utc(end + timedelta(days=2))
    sql = (
        "SELECT s.*, c.identity_key, c.type AS channel_type, c.ch, c.label, "
        "sen.sensor_id, sen.sensor_token "
        "FROM energy_spans s "
        "JOIN channels c ON c.id = s.channel_pk "
        "JOIN sensors sen ON sen.id = c.sensor_pk "
        "WHERE sen.sensor_id != ? "
        "AND s.end_received_utc >= ? AND s.start_received_utc <= ?"
    )
    args: list = [MISSING_SENSOR_ID, start_s, end_s]
    if sensor_token:
        sql += " AND sen.sensor_token=?"
        args.append(sensor_token)
    return [dict(row) for row in conn.execute(sql, args).fetchall()]


def _channels_for(conn, sensor_token: str | None, tokens_in_spans: set[str]):
    sql = (
        "SELECT c.identity_key, c.type, c.ch, c.label, sen.sensor_token, sen.sensor_id "
        "FROM channels c JOIN sensors sen ON sen.id = c.sensor_pk "
        "WHERE sen.sensor_id != ?"
    )
    args: list = [MISSING_SENSOR_ID]
    if sensor_token:
        sql += " AND sen.sensor_token=?"
        args.append(sensor_token)
    rows = [dict(row) for row in conn.execute(sql, args).fetchall()]
    if tokens_in_spans:
        rows = [row for row in rows if row["sensor_token"] in tokens_in_spans or sensor_token]
    return rows


def _placement(span: dict) -> tuple[str, str]:
    if span.get("placement_start_utc") and span.get("placement_end_utc"):
        return span["placement_start_utc"], span["placement_end_utc"]
    if (
        span["time_basis"] == "device"
        and span["start_device_utc"]
        and span["end_device_utc"]
    ):
        return span["start_device_utc"], span["end_device_utc"]
    return span["start_received_utc"], span["end_received_utc"]


def _new_acc(span: dict) -> dict:
    return {
        "identity_key": span["identity_key"],
        "type": span["channel_type"],
        "ch": span["ch"],
        "sensor_token": span["sensor_token"],
        "sensor_id": span["sensor_id"],
        "import_ws": None,
        "export_ws": None,
        "by_status": {},
        "crossing_hour": {"import": 0, "export": 0},
        "unresolved_inside": 0,
        "problem_overlap": 0,
        "boundary_allocatable": 0,
        "time_bases": set(),
        "methods": set(),
    }


def _add_status(acc: dict, span: dict) -> None:
    direction = span["direction"]
    status = span["allocation_status"]
    bucket = acc["by_status"].setdefault(status, {"import": 0, "export": 0})
    bucket[direction] += span["delta_ws"]
    acc["time_bases"].add(span["time_basis"])
    acc["methods"].add(span["source_method"])


def _add_allocated(acc: dict, span: dict) -> None:
    key = "import_ws" if span["direction"] == "import" else "export_ws"
    current = 0 if acc[key] is None else acc[key]
    acc[key] = current + span["delta_ws"]


def _add_named(acc: dict, name: str, span: dict) -> None:
    acc[name][span["direction"]] += span["delta_ws"]


def _render_bucket(bucket: dict, accs: dict, channel_rows: list[dict], cfg: Config, sensors: set[str]) -> dict:
    channels = [_render_channel(acc, bucket_level=True) for acc in accs.values()]
    channels.sort(key=lambda item: item["identity_key"])
    return {
        "start_utc": bucket["start_utc"],
        "end_utc": bucket["end_utc"],
        "start_local": bucket["start_local"],
        "end_local": bucket["end_local"],
        "utc_offset_start": bucket["utc_offset_start"],
        "utc_offset_end": bucket["utc_offset_end"],
        "fold": bucket["fold"],
        "duration_seconds": bucket["duration_seconds"],
        "local_date": bucket["local_date"],
        "local_hour": bucket["local_hour"],
        "coverage": _bucket_coverage(channels),
        "channels": channels,
        "household": _household(accs, channel_rows, cfg, sensors),
    }


def _render_channel(acc: dict, bucket_level: bool) -> dict:
    quality = []
    if acc["boundary_allocatable"]:
        quality.append("hour_boundary_residuals_excluded")
    if acc["problem_overlap"] or acc["unresolved_inside"]:
        quality.append("unresolved_spans_present")
    coverage = _channel_coverage(acc)
    net = _net(acc["import_ws"], acc["export_ws"])
    return {
        "sensor_token": acc["sensor_token"],
        "identity_key": acc["identity_key"],
        "type": acc["type"],
        "ch": acc["ch"],
        "allocated_import_ws": acc["import_ws"],
        "allocated_export_ws": acc["export_ws"],
        "allocated_net_ws": net,
        "allocated_import_kwh": _kwh(acc["import_ws"]),
        "allocated_export_kwh": _kwh(acc["export_ws"]),
        "allocated_net_kwh": _kwh(net),
        "allocated_import_wh": _wh(acc["import_ws"]),
        "allocated_export_wh": _wh(acc["export_ws"]),
        "preserved_ws_by_status": acc["by_status"],
        "crossing_hour_ws": acc["crossing_hour"],
        "coverage": coverage,
        "source_method": "counter_delta" if acc["import_ws"] is not None or acc["export_ws"] is not None else None,
        "time_basis": next(iter(acc["time_bases"])) if len(acc["time_bases"]) == 1 else ("mixed" if acc["time_bases"] else None),
        "overlapping_unresolved_span_count": acc["problem_overlap"] + acc["unresolved_inside"],
        "boundary_allocatable_span_count": acc["boundary_allocatable"],
        "quality": quality,
        "reading_notice": _notice(acc["sensor_id"]),
    }


def _render_preserved(acc: dict) -> dict:
    rendered = _render_channel(acc, bucket_level=False)
    rendered["crossing_hour_import_ws"] = acc["crossing_hour"]["import"]
    rendered["crossing_hour_export_ws"] = acc["crossing_hour"]["export"]
    return rendered


def _channel_coverage(acc: dict) -> str:
    has_alloc = acc["import_ws"] is not None or acc["export_ws"] is not None
    problems = acc["problem_overlap"] + acc["unresolved_inside"]
    if not has_alloc and problems == 0 and not acc["by_status"]:
        return "no_data"
    if has_alloc and problems == 0:
        return "allocated"
    if not has_alloc and problems > 0:
        return "gap"
    if has_alloc and problems > 0:
        return "partial"
    if acc["by_status"] and not has_alloc:
        return "gap"
    return "no_data"


def _bucket_coverage(channels: list[dict]) -> str:
    if not channels:
        return "no_data"
    names = {item["coverage"] for item in channels}
    if names == {"no_data"}:
        return "no_data"
    if names <= {"allocated"}:
        return "allocated"
    if "allocated" in names or "partial" in names:
        return "partial"
    return "gap"


def _household(accs: dict, channel_rows: list[dict], cfg: Config, sensors: set[str]) -> dict:
    base = {
        "topology_verified": cfg.topology_verified,
        "provenance": "disabled",
        "grid_import_ws": None,
        "grid_export_ws": None,
        "grid_net_ws": None,
        "grid_import_kwh": None,
        "grid_export_kwh": None,
        "grid_net_kwh": None,
        "grid_source": None,
        "consumption_ws": None,
        "consumption_kwh": None,
        "consumption_source": None,
        "consumption_method": None,
        "generation_eimp_ws": None,
        "generation_eexp_ws": None,
        "generation_net_ws": None,
        "generation_eimp_kwh": None,
        "generation_eexp_kwh": None,
        "generation_net_kwh": None,
        "generation_collapsed_kwh": None,
        "generation_source": None,
        "generation_collapsed_reason": None,
        "reason": None,
        "rejected_methods": list(REJECTED_METHODS),
    }
    if not cfg.household_channel_reads:
        base["reason"] = "household_channel_reads_disabled"
        return base
    if len(sensors) > 1:
        base["reason"] = "multiple_sensors"
        return base
    net, net_reason = _role(accs, channel_rows, "NET", cfg.role_net_identity)
    generation, gen_reason = _role(accs, channel_rows, "GENERATION", cfg.role_generation_identity)
    consumption, con_reason = _role(
        accs, channel_rows, "CONSUMPTION", cfg.role_consumption_identity
    )
    base["provenance"] = (
        "topology_verified" if cfg.topology_verified else "channel_type_observed_not_topology_verified"
    )
    if net is not None:
        base["grid_source"] = net["identity_key"]
        base["grid_import_ws"] = net["import_ws"]
        base["grid_export_ws"] = net["export_ws"]
        base["grid_net_ws"] = _net(net["import_ws"], net["export_ws"])
        base["grid_import_kwh"] = _kwh(base["grid_import_ws"])
        base["grid_export_kwh"] = _kwh(base["grid_export_ws"])
        base["grid_net_kwh"] = _kwh(base["grid_net_ws"])
    else:
        base["reason"] = net_reason
    if generation is not None:
        base["generation_source"] = generation["identity_key"]
        base["generation_eimp_ws"] = generation["import_ws"]
        base["generation_eexp_ws"] = generation["export_ws"]
        base["generation_net_ws"] = _net(generation["import_ws"], generation["export_ws"])
        base["generation_eimp_kwh"] = _kwh(base["generation_eimp_ws"])
        base["generation_eexp_kwh"] = _kwh(base["generation_eexp_ws"])
        base["generation_net_kwh"] = _kwh(base["generation_net_ws"])
        if cfg.generation_register == "e_imp":
            base["generation_collapsed_kwh"] = base["generation_eimp_kwh"]
            base["generation_collapsed_reason"] = "explicit_e_imp"
        elif cfg.generation_register == "e_exp":
            base["generation_collapsed_kwh"] = base["generation_eexp_kwh"]
            base["generation_collapsed_reason"] = "explicit_e_exp"
        else:
            base["generation_collapsed_reason"] = "register_unset_both_counters_preserved"
    if cfg.consumption_source == "none":
        base["consumption_method"] = "none"
        base["reason"] = base["reason"] or "consumption_source_none"
    elif cfg.consumption_source == "consumption_channel":
        base["consumption_method"] = "dedicated_channel_eImp_minus_eExp"
        if consumption is None:
            base["reason"] = base["reason"] or con_reason
        else:
            base["consumption_source"] = consumption["identity_key"]
            base["consumption_ws"] = _net(consumption["import_ws"], consumption["export_ws"])
            base["consumption_kwh"] = _kwh(base["consumption_ws"])
    elif cfg.consumption_source == "derive_net_plus_generation":
        base["consumption_method"] = "derived_net_plus_generation"
        base["rejected_methods"] = ["max(net,0)", "sum(phase_channels)"]
        net_ws = base["grid_net_ws"]
        gen_ws = base["generation_net_ws"]
        if net_ws is None or gen_ws is None:
            base["reason"] = base["reason"] or "derivation_inputs_unknown"
        else:
            base["consumption_ws"] = net_ws + gen_ws
            base["consumption_kwh"] = _kwh(base["consumption_ws"])
            base["consumption_source"] = "derived:" + (base["grid_source"] or "") + "+" + (
                base["generation_source"] or ""
            )
    return base


def _role(accs: dict, channel_rows: list[dict], type_name: str, identity: str | None):
    if identity:
        hits = [row for row in channel_rows if row["identity_key"] == identity]
    else:
        hits = [row for row in channel_rows if row["type"] == type_name]
    keys = {row["identity_key"] for row in hits}
    if len(keys) > 1:
        return None, f"ambiguous_{type_name.lower()}"
    if len(keys) == 0:
        return None, f"missing_{type_name.lower()}"
    key = next(iter(keys))
    if key not in accs:
        return None, f"no_allocated_span_{type_name.lower()}"
    return accs[key], None


def _notice(sensor_id: str) -> str:
    if sensor_id.startswith("SYNTHETIC"):
        return "synthetic_fixture"
    if sensor_id == MISSING_SENSOR_ID:
        return "unidentified_payload"
    return "device_collector"


def _net(imported, exported):
    if imported is None or exported is None:
        return None
    return imported - exported


def _kwh(ws):
    if ws is None:
        return None
    return ws_to_kwh_str(ws)


def _wh(ws):
    if ws is None:
        return None
    return ws_to_wh_str(ws)
