"""Optional short-interval power estimate.

Counter deltas are the stored energy. This helper is not called by ingest.
A long gap returns None so missing time is not filled by integrating power.
"""

from __future__ import annotations


def estimate_interval_ws(
    p1_w: float | None,
    p2_w: float | None,
    dt_seconds: float | None,
    max_gap_seconds: float,
) -> dict | None:
    if p1_w is None or p2_w is None or dt_seconds is None:
        return None
    if dt_seconds <= 0 or dt_seconds > max_gap_seconds:
        return None
    average = (p1_w + p2_w) / 2.0
    joules = average * dt_seconds
    if average >= 0:
        imported = joules
        exported = 0.0
    else:
        imported = 0.0
        exported = -joules
    return {
        "source_method": "power_integration_estimated",
        "import_ws_est": imported,
        "export_ws_est": exported,
        "persisted": False,
    }
