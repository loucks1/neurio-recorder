"""Drop aged raw payloads and poll logs.

Normalized channel counters, epochs, and energy spans are kept. Retention never
deletes those aggregates and never backfills the time that was removed.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from .db import Database
from .timeutil import format_utc


def apply_retention(db: Database, now: datetime, raw_days: int, poll_days: int) -> tuple[int, int]:
    """Null aged raw JSON and delete aged poll-attempt rows.

    raw_days == 0 keeps every raw_json value. A zero day count is not a cutoff
    of "now", which would clear almost every payload. Negative raw_days is
    rejected. This function never deletes samples, counters, spans, or epochs.
    """
    if raw_days < 0 or poll_days < 1:
        raise ValueError("raw_days must be >= 0 and poll_days must be >= 1")
    poll_cutoff = format_utc(now - timedelta(days=poll_days))
    with db.write_lock:
        with db.transaction():
            cleared = 0
            if raw_days > 0:
                raw_cutoff = format_utc(now - timedelta(days=raw_days))
                cursor = db.conn.execute(
                    "UPDATE samples SET raw_json=NULL, raw_retained=0 "
                    "WHERE raw_retained=1 AND received_at_utc < ?",
                    (raw_cutoff,),
                )
                cleared = cursor.rowcount
            db.conn.execute(
                "DELETE FROM poll_attempts WHERE started_at_utc < ?",
                (poll_cutoff,),
            )
            spans = db.conn.execute("SELECT COUNT(*) AS n FROM energy_spans").fetchone()["n"]
            return cleared, spans
