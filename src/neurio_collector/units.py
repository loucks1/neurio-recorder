"""Kilowatt-hour strings for the hourly API.

Hourly records store integer milli-kWh. The API reports decimal kWh strings
quantized to 1e-9 kWh. milli-kWh times 3600 is watt-seconds, and
watt-seconds / 3_600_000 is kilowatt-hours, so the conversion is exact for
the three decimal places the bridge publishes.
"""

from __future__ import annotations

from decimal import Decimal, ROUND_HALF_EVEN, localcontext

_KWH_QUANTUM = Decimal("0.000000001")


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
