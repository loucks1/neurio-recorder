"""UTC storage and America/Chicago bucket boundaries.

Hour buckets are produced by walking UTC hour by hour and labeling each instant
in the requested IANA zone. That yields a 23-hour spring-forward day and a
25-hour fall-back day, including the repeated local hour at two different offsets.
Wall-clock subtraction across those labels is not a duration. Use the UTC bounds.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

UTC = timezone.utc


def utcnow() -> datetime:
    return datetime.now(UTC)


def format_utc(value: datetime) -> str:
    if value.tzinfo is None:
        raise ValueError("UTC formatting requires an aware datetime")
    value = value.astimezone(UTC)
    millis = value.microsecond // 1000
    return value.strftime("%Y-%m-%dT%H:%M:%S") + f".{millis:03d}Z"


def parse_instant(value: str) -> datetime:
    if not isinstance(value, str) or not value.strip():
        raise ValueError("timestamp is required")
    text = value.strip()
    if text.endswith(("Z", "z")):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError as exc:
        raise ValueError(f"invalid timestamp {value!r}") from exc
    if parsed.tzinfo is None:
        raise ValueError("timestamp must include a UTC offset")
    return parsed.astimezone(UTC)


def offset_str(value: datetime) -> str:
    text = value.isoformat()
    return text[-6:]


def load_zone(name: str) -> ZoneInfo:
    if not isinstance(name, str) or not name.strip():
        raise ValueError("timezone is required")
    cleaned = name.strip()
    if cleaned[0] in "+-" or cleaned.upper() in {"CST", "CDT", "EST", "EDT", "UTC-6", "UTC-5"}:
        raise ValueError(
            "use an IANA timezone such as America/Chicago; fixed offsets are rejected"
        )
    try:
        return ZoneInfo(cleaned)
    except ZoneInfoNotFoundError as exc:
        raise ValueError(f"unknown timezone {cleaned}") from exc


def local_day_bounds(day: date, zone: ZoneInfo) -> tuple[datetime, datetime]:
    start = datetime(day.year, day.month, day.day, tzinfo=zone)
    nxt = day + timedelta(days=1)
    end = datetime(nxt.year, nxt.month, nxt.day, tzinfo=zone)
    return start, end


def _hour_aligned(value: datetime) -> None:
    if value.minute or value.second or value.microsecond:
        raise ValueError("hour queries require UTC hour boundaries")


def iter_utc_hours(start: datetime, end: datetime, zone: ZoneInfo) -> list[dict]:
    cur = start.astimezone(UTC)
    stop = end.astimezone(UTC)
    _hour_aligned(cur)
    _hour_aligned(stop)
    if cur >= stop:
        raise ValueError("end must be after start")
    buckets: list[dict] = []
    while cur < stop:
        nxt = cur + timedelta(hours=1)
        local = cur.astimezone(zone)
        local_end = nxt.astimezone(zone)
        buckets.append(_bucket(cur, nxt, local, local_end, local.date().isoformat(), local.hour))
        cur = nxt
    return buckets


def hours_for_local_date(day: date, zone: ZoneInfo) -> list[dict]:
    start, end = local_day_bounds(day, zone)
    return iter_utc_hours(start, end, zone)


def day_bucket(day: date, zone: ZoneInfo) -> dict:
    start, end = local_day_bounds(day, zone)
    return _bucket(
        start.astimezone(UTC),
        end.astimezone(UTC),
        start,
        end,
        day.isoformat(),
        None,
    )


def _bucket(
    start_utc: datetime,
    end_utc: datetime,
    start_local: datetime,
    end_local: datetime,
    local_date: str,
    local_hour: int | None,
) -> dict:
    return {
        "start": start_utc,
        "end": end_utc,
        "start_utc": format_utc(start_utc),
        "end_utc": format_utc(end_utc),
        "start_local": start_local.isoformat(),
        "end_local": end_local.isoformat(),
        "utc_offset_start": offset_str(start_local),
        "utc_offset_end": offset_str(end_local),
        "fold": start_local.fold,
        "duration_seconds": int((end_utc - start_utc).total_seconds()),
        "local_date": local_date,
        "local_hour": local_hour,
    }
