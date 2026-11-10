"""Watt-second conversions.

Integer watt-seconds are the stored unit. Watt-hours are Ws/3600. Kilowatt-hours
are Ws/3_600_000. kWh strings are quantized to 1e-9 kWh, which rounds back to
the original integer watt-second.
"""

from __future__ import annotations

from decimal import Decimal, localcontext, ROUND_HALF_EVEN

INT64_MIN = -9223372036854775808
INT64_MAX = 9223372036854775807
_KWH_QUANTUM = Decimal("0.000000001")
_WH_QUANTUM = Decimal("0.000001")


def fits_int64(value: int) -> bool:
    return INT64_MIN <= value <= INT64_MAX


def _trim(value: Decimal) -> str:
    text = format(value, "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    if text in {"", "-0"}:
        return "0"
    return text


def ws_to_kwh_str(ws: int) -> str:
    if isinstance(ws, bool) or not isinstance(ws, int):
        raise TypeError("watt-seconds must be an int")
    with localcontext() as ctx:
        ctx.prec = 50
        ctx.rounding = ROUND_HALF_EVEN
        quant = (Decimal(ws) / Decimal(3_600_000)).quantize(_KWH_QUANTUM)
    return _trim(quant)


def ws_to_wh_str(ws: int) -> str:
    if isinstance(ws, bool) or not isinstance(ws, int):
        raise TypeError("watt-seconds must be an int")
    with localcontext() as ctx:
        ctx.prec = 50
        ctx.rounding = ROUND_HALF_EVEN
        quant = (Decimal(ws) / Decimal(3600)).quantize(_WH_QUANTUM)
    return _trim(quant)


def kwh_str_to_ws(text: str) -> int:
    with localcontext() as ctx:
        ctx.prec = 50
        ctx.rounding = ROUND_HALF_EVEN
        ws = (Decimal(text) * Decimal(3_600_000)).quantize(Decimal("1"))
    return int(ws)
