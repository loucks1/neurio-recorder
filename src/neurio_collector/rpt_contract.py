"""Documented RatePlanTool behavior. This is not a client and not a server.

The measurements come from a read-only probe of commit
78fc57c348e93e11cc6b3cf26f02fb292c54bbfe. Nothing here listens for
oauth2/token, users/current, or samples, and nothing here builds the hourly
boundary cumulatives that parser would require.

`rateplantool_contract` stays false. Pointing RatePlanTool's base_URL at this
process is not supported.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta
from decimal import Decimal
from zoneinfo import ZoneInfo

from .timeutil import UTC, format_utc, parse_instant

RPT_COMMIT = "78fc57c348e93e11cc6b3cf26f02fb292c54bbfe"
RPT_SAMPLES_SHA256 = "312a2bd55403487ecdebf494995a3ac645a5262649b330ec02fe0625f3da9509"
CHICAGO = ZoneInfo("America/Chicago")
WS_PER_KWH = Decimal(3_600_000)

# Reasons a drop-in stand-in cannot meet the collector's accuracy rules.
REFUSAL_REASONS = (
    "rpt_reads_cumulative_ws_samples_not_general_csv",
    "hour_boundary_cumulatives_would_be_fabricated",
    "long_gap_delta_is_assigned_to_timestamp_minus_one_hour",
    "counter_decrease_becomes_zero_kwh",
    "request_window_is_start_plus_24h_not_chicago_midnight",
    "fall_back_last_hour_is_never_requested",
    "spring_forward_overlap_hour_can_be_summed_twice",
    "in_progress_day_cache_throws_when_local_hour_is_23",
    "oauth_token_and_users_current_are_not_implemented",
    "net_energy_is_ignored_and_is_not_grid_import_or_export",
)


class RptContractError(ValueError):
    pass


def contract_flags() -> dict:
    return {
        "rateplantool_contract": False,
        "rateplantool_integration": "not_wired",
        "rateplantool_source_sha": RPT_COMMIT,
        "rateplantool_contract_verified": True,
        "safe_drop_in_adapter": False,
        "general_csv_importable": False,
        "refusal_reasons": list(REFUSAL_REASONS),
    }


def refuse_hour_boundary_export() -> dict:
    """Return the refusal. Do not synthesize a samples array."""
    return {
        "export": None,
        "rateplantool_contract": False,
        "safe_drop_in_adapter": False,
        "reasons": list(REFUSAL_REASONS),
    }


def request_window(civil: date) -> dict:
    """The window NeurioService requests: Chicago midnight, then plus 24 hours.

    The next civil day starts at the next Chicago midnight, which is not the
    same instant on a DST boundary.
    """
    start = _chicago_midnight(civil)
    end = start + timedelta(hours=24)
    next_start = _chicago_midnight(civil + timedelta(days=1))
    gap_hours = (next_start - end).total_seconds() / 3600
    if gap_hours > 0:
        boundary = "gap"
    elif gap_hours < 0:
        boundary = "overlap"
    else:
        boundary = "adjacent"
    return {
        "civil_date": civil.isoformat(),
        "start_utc": format_utc(start),
        "end_utc": format_utc(end),
        "end_chicago": end.astimezone(CHICAGO).isoformat(),
        "next_start_utc": format_utc(next_start),
        "gap_hours": gap_hours,
        "boundary": boundary,
        "request_start_dotnet": _dotnet_roundtrip(start),
        "request_end_dotnet": _dotnet_roundtrip(end),
    }


def in_progress_cache_hour(last_raw_utc: datetime) -> dict:
    """Reproduce the cache expiry hour used for an in-progress civil day.

    The server does `lastRaw.ToLocalTime().Hour + 1` and passes that to
    `new DateTime`. Hour 23 becomes 24 and the constructor throws.
    """
    if last_raw_utc.tzinfo is None:
        raise RptContractError("last raw timestamp needs a zone")
    local = last_raw_utc.astimezone(CHICAGO)
    hour_plus = local.hour + 1
    return {
        "local": local.isoformat(),
        "local_hour": local.hour,
        "cache_hour": hour_plus,
        "throws_argument_out_of_range": hour_plus > 23,
    }


def emit_like_raw_data(points: list[dict]) -> list[dict]:
    """Walk an array the way Samples.RawData does.

    Point 0 is a baseline and emits nothing. Each later point emits
    `(cumulative - previous) / 3_600_000` at `timestamp - 1 hour`. A decrease
    emits 0. Elapsed time is not used, so a multi-hour jump is one row.
    """
    if not points:
        raise RptContractError(
            "an empty samples array throws ArgumentOutOfRangeException in Samples.set_RawData"
        )
    rows = []
    for index in range(1, len(points)):
        previous = points[index - 1]
        current = points[index]
        if not current.get("timestamp"):
            raise RptContractError(
                "a later point with no timestamp throws ArgumentOutOfRangeException and drops the day"
            )
        current_ts = parse_instant(current["timestamp"])
        previous_ts = parse_instant(previous["timestamp"]) if previous.get("timestamp") else None
        consumption = _step_kwh(previous.get("consumptionEnergy", 0), current.get("consumptionEnergy", 0))
        generation = _step_kwh(previous.get("generationEnergy", 0), current.get("generationEnergy", 0))
        elapsed_hours = None
        if previous_ts is not None:
            elapsed_hours = (current_ts - previous_ts).total_seconds() / 3600
        rows.append(
            {
                "emitted_utc": format_utc(current_ts - timedelta(hours=1)),
                "consumption_kwh": consumption,
                "generation_kwh": generation,
                "elapsed_hours": elapsed_hours,
                "decrease_forced_to_zero": _is_decrease(
                    previous.get("consumptionEnergy", 0), current.get("consumptionEnergy", 0)
                )
                or _is_decrease(previous.get("generationEnergy", 0), current.get("generationEnergy", 0)),
                "gap_assigned_to_one_hour": elapsed_hours is not None and elapsed_hours > 1,
            }
        )
    return rows


def document_would_fail_deserialization(value) -> str | None:
    """Name the documented whole-document failure. Do not parse a live feed."""
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "non_integer"
    if isinstance(value, float):
        return "fraction"
    if isinstance(value, int) and value < 0:
        return "negative"
    return None


def _step_kwh(previous, current) -> Decimal:
    if document_would_fail_deserialization(previous) or document_would_fail_deserialization(current):
        raise RptContractError("null, negative, or fractional energy fails the whole samples array")
    if current < previous:
        return Decimal(0)
    return (Decimal(int(current)) - Decimal(int(previous))) / WS_PER_KWH


def _is_decrease(previous, current) -> bool:
    if document_would_fail_deserialization(previous) or document_would_fail_deserialization(current):
        return False
    return current < previous


def _chicago_midnight(civil: date) -> datetime:
    local = datetime(civil.year, civil.month, civil.day, tzinfo=CHICAGO)
    return local.astimezone(UTC)


def _dotnet_roundtrip(value: datetime) -> str:
    return value.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%S.0000000Z")
