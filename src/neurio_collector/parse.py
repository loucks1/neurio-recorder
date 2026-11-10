"""Parse a local current-sample or a partial Home Assistant channel attribute object.

Unknown keys, including a leading-underscore provenance object, are ignored.
Voltage is read from v_V or the legacy sample key V_V. Reactive power is read
from q_VAR, q_Var, or q_var. Counters stay integers. Missing counters stay null.
A non-integral or out-of-range counter is null and flagged, not rounded.
"""

from __future__ import annotations

import hashlib
import json
import math
from dataclasses import dataclass, field

from .timeutil import parse_instant
from .units import fits_int64

KNOWN_TYPES = {
    "PHASE_A_CONSUMPTION",
    "PHASE_B_CONSUMPTION",
    "PHASE_C_CONSUMPTION",
    "NET",
    "GENERATION",
    "CONSUMPTION",
    "SUBMETER",
}


class ParseError(ValueError):
    pass


@dataclass
class ParsedChannel:
    identity_key: str
    ch: int | None
    type: str
    label: str | None
    p_w: float | None
    q_var: float | None
    v_v: float | None
    e_imp_ws: int | None
    e_exp_ws: int | None
    field_quality: str
    flags: list[str] = field(default_factory=list)


@dataclass
class ParsedCt:
    ct: int | None
    p_w: float | None
    q_var: float | None
    v_v: float | None


@dataclass
class ParsedSample:
    sensor_id: str | None
    device_timestamp_raw: str | None
    device_timestamp_utc: object | None
    clock_quality: str
    channels: list[ParsedChannel]
    cts: list[ParsedCt]
    flags: list[str]
    payload_sha256: str


def parse_payload(body: bytes) -> ParsedSample:
    if not isinstance(body, (bytes, bytearray)):
        raise ParseError("body must be bytes")
    try:
        text = bytes(body).decode("utf-8")
    except UnicodeDecodeError as exc:
        raise ParseError("body is not utf-8") from exc
    try:
        data = json.loads(text)
    except json.JSONDecodeError as exc:
        raise ParseError("body is not json") from exc
    if not isinstance(data, dict):
        raise ParseError("sample must be a JSON object")
    flags: list[str] = []
    sensor_id = _sensor_id(data.get("sensorId"), flags)
    raw_ts, clock_quality, device_dt = _timestamp(data.get("timestamp") if "timestamp" in data else _MISSING)
    if clock_quality == "missing":
        flags.append("device_timestamp_missing")
    channels_raw = data.get("channels", _MISSING)
    if channels_raw is _MISSING:
        flags.append("channels_missing")
        channel_items: list = []
    elif not isinstance(channels_raw, list):
        raise ParseError("channels must be a list")
    else:
        channel_items = channels_raw
    channels = _channels(channel_items, flags)
    cts = _cts(data.get("cts"), flags)
    digest = hashlib.sha256(bytes(body)).hexdigest()
    return ParsedSample(
        sensor_id=sensor_id,
        device_timestamp_raw=raw_ts,
        device_timestamp_utc=device_dt,
        clock_quality=clock_quality,
        channels=channels,
        cts=cts,
        flags=flags,
        payload_sha256=digest,
    )


class _Missing:
    pass


_MISSING = _Missing()


def _sensor_id(value, flags: list[str]) -> str | None:
    if value is None:
        flags.append("sensor_id_missing")
        return None
    if isinstance(value, str):
        if not value.strip():
            flags.append("sensor_id_missing")
            return None
        return value.strip()
    if isinstance(value, int) and not isinstance(value, bool):
        flags.append("sensor_id_coerced_from_int")
        return str(value)
    flags.append("sensor_id_missing")
    return None


def _timestamp(value):
    if value is _MISSING or value is None:
        return None, "missing", None
    if not isinstance(value, str):
        return str(value), "invalid", None
    if value.strip().upper() == "NOT_SYNCHRONIZED":
        return value, "not_synchronized", None
    try:
        parsed = parse_instant(value)
    except ValueError:
        return value, "invalid", None
    return value, "synchronized", parsed


def _channels(items: list, flags: list[str]) -> list[ParsedChannel]:
    parsed: list[ParsedChannel] = []
    seen: set[str] = set()
    for index, item in enumerate(items):
        if not isinstance(item, dict):
            flags.append("channel_not_object")
            continue
        channel = _channel(item, index)
        if channel.identity_key in seen:
            flags.append("duplicate_channel_identity")
            continue
        seen.add(channel.identity_key)
        if channel.type not in KNOWN_TYPES:
            flags.append("unknown_channel_type")
        parsed.append(channel)
    return parsed


def _channel(item: dict, index: int) -> ParsedChannel:
    flags: list[str] = []
    type_name = item.get("type")
    if isinstance(type_name, str) and type_name.strip():
        type_name = type_name.strip()
    else:
        type_name = "UNKNOWN"
        flags.append("type_missing")
    ch_value, ch_flag = _channel_number(item.get("ch"))
    if ch_flag:
        flags.append(ch_flag)
    if ch_value is None:
        identity = f"idx:{index}|type:{type_name}"
    else:
        identity = f"ch:{ch_value}|type:{type_name}"
    label = item.get("label")
    if label is not None and not isinstance(label, str):
        label = str(label)
        flags.append("label_coerced")
    p_w, p_flag = _finite_number(item.get("p_W") if "p_W" in item else _MISSING)
    if p_flag:
        flags.append(f"p_W_{p_flag}")
    q_raw, q_key = _pick(item, "q_VAR", "q_Var", "q_var")
    q_var, q_flag = _finite_number(q_raw)
    if q_flag:
        flags.append(f"q_{q_flag}")
    if q_key and q_key != "q_VAR":
        flags.append(f"reactive_key_{q_key}")
    v_raw, v_key = _pick(item, "v_V", "V_V")
    if "v_V" in item and "V_V" in item:
        left, _ = _finite_number(item.get("v_V"))
        right, _ = _finite_number(item.get("V_V"))
        if left is not None and right is not None and abs(left - right) > 0.05:
            flags.append("voltage_field_conflict")
    v_v, v_flag = _finite_number(v_raw)
    if v_flag:
        flags.append(f"v_{v_flag}")
    if v_key == "V_V":
        flags.append("voltage_key_V_V")
    e_imp, imp_flag = _counter(item.get("eImp_Ws") if "eImp_Ws" in item else _MISSING)
    e_exp, exp_flag = _counter(item.get("eExp_Ws") if "eExp_Ws" in item else _MISSING)
    if imp_flag and imp_flag != "ok":
        flags.append(f"eImp_{imp_flag}")
    if exp_flag and exp_flag != "ok":
        flags.append(f"eExp_{exp_flag}")
    qualities = [flag for flag in (imp_flag, exp_flag) if flag and flag != "ok"]
    if not qualities:
        field_quality = "ok"
    elif "missing" in qualities and len(qualities) == qualities.count("missing"):
        field_quality = "partial" if len(qualities) == 1 else "missing"
    else:
        field_quality = "partial"
    return ParsedChannel(
        identity_key=identity,
        ch=ch_value,
        type=type_name,
        label=label,
        p_w=p_w,
        q_var=q_var,
        v_v=v_v,
        e_imp_ws=e_imp,
        e_exp_ws=e_exp,
        field_quality=field_quality,
        flags=flags,
    )


def _cts(value, flags: list[str]) -> list[ParsedCt]:
    if value is None:
        return []
    if not isinstance(value, list):
        flags.append("cts_not_list")
        return []
    parsed: list[ParsedCt] = []
    for item in value:
        if not isinstance(item, dict):
            flags.append("ct_not_object")
            continue
        ct_num, _ = _channel_number(item.get("ct"))
        p_w, _ = _finite_number(item.get("p_W") if "p_W" in item else _MISSING)
        q_raw, _ = _pick(item, "q_VAR", "q_Var", "q_var")
        q_var, _ = _finite_number(q_raw)
        v_raw, _ = _pick(item, "v_V", "V_V")
        v_v, _ = _finite_number(v_raw)
        parsed.append(ParsedCt(ct=ct_num, p_w=p_w, q_var=q_var, v_v=v_v))
    return parsed


def _pick(item: dict, *keys: str):
    for key in keys:
        if key in item:
            return item.get(key), key
    return _MISSING, None


def _channel_number(value):
    if value is None:
        return None, "ch_missing"
    if isinstance(value, bool):
        return None, "ch_invalid"
    if isinstance(value, int):
        return value, None
    if isinstance(value, str) and value.strip().lstrip("-").isdigit():
        return int(value.strip()), "ch_coerced_from_str"
    return None, "ch_invalid"


def _finite_number(value):
    if value is _MISSING or value is None:
        return None, "missing" if value is _MISSING else None
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None, "invalid"
    if isinstance(value, float) and not math.isfinite(value):
        return None, "invalid"
    return float(value), None


def _counter(value):
    if value is _MISSING or value is None:
        return None, "missing"
    if isinstance(value, bool):
        return None, "invalid"
    if isinstance(value, int):
        if not fits_int64(value):
            return None, "out_of_range"
        return value, "ok"
    if isinstance(value, float):
        if not math.isfinite(value) or abs(value) > 2**53:
            return None, "unsafe_float"
        if value != math.trunc(value):
            return None, "non_integral"
        iv = int(value)
        if not fits_int64(iv):
            return None, "out_of_range"
        return iv, "integral_float"
    if isinstance(value, str) and value.strip().lstrip("-").isdigit():
        iv = int(value.strip())
        if not fits_int64(iv):
            return None, "out_of_range"
        return iv, "integral_string"
    return None, "invalid"
