"""UTC timestamps for stored hourly records."""

from __future__ import annotations

from datetime import datetime, timezone

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
