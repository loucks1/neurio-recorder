"""Store Neurio cloud hourly stats, separately from the local data.

Storage choice (dual-source design, decision 1 and 10): cloud values live
in their own table, ``cloud_hourly_records``, keyed by the hour's end —
never in the ESP32 bridge's ``hourly_records`` table. The two tables have
different invariants: ``hourly_records.record_id`` is the bridge's own id
space with an ACK lifecycle and a NOT NULL raw payload, while a cloud row
has neither. Sharing the table would force a primary-key change on a live
table and fake values for the bridge columns — the one-off
``backfill-20261110`` batch only got away with writing cloud rows into
``hourly_records`` because those hours had no ESP32 rows. Here both values
for the same hour always survive, one in each table, and the read-time
merge (a later step) joins them on the hour-end epoch.

Conversion rules, proven in the step-zero verification: /samples/stats
values are per-hour interval watt-seconds — store them as-is, never
differenced — keyed by the returned ``end``. The recorder's storage unit
is milli-kWh, mkWh = Ws / 3600, rounded half-up (the cloud's sub-mkWh
remainder is below the ESP32's own 3-decimal kWh precision).
"""

from __future__ import annotations

import math
import sys
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP

from .db import Database
from .timeutil import format_utc, parse_instant

# The cloud tables are created by this module, on first use, not by the
# core schema: a recorder that never enables cloud ingest keeps exactly
# the schema-3 table set (the same treatment hourly_backfill_provenance
# gets in db.py).
CLOUD_SCHEMA = """
CREATE TABLE IF NOT EXISTS cloud_hourly_records (
  hour_end_epoch INTEGER PRIMARY KEY,
  hour_start_utc TEXT NOT NULL,
  hour_end_utc TEXT NOT NULL,
  consumption_millikwh INTEGER,
  generation_millikwh INTEGER,
  imported_millikwh INTEGER,
  exported_millikwh INTEGER,
  fetched_at_utc TEXT NOT NULL,
  last_fetched_at_utc TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS cloud_sync_state (
  id INTEGER PRIMARY KEY CHECK (id = 1),
  sensor_id TEXT,
  harvest_from_utc TEXT,
  harvest_through_utc TEXT,
  harvest_complete INTEGER NOT NULL DEFAULT 0,
  last_attempt_at_utc TEXT,
  last_success_at_utc TEXT,
  last_error TEXT,
  last_error_at_utc TEXT,
  backoff_until_utc TEXT,
  unaligned_skipped_total INTEGER NOT NULL DEFAULT 0
);
"""


def ensure_cloud_tables(db: Database) -> None:
    with db.write_lock:
        db.conn.executescript(CLOUD_SCHEMA)
        # CREATE TABLE IF NOT EXISTS does not add columns to a table that
        # already exists. A database synced before pacing shipped has the
        # original cloud_sync_state and needs the backoff column.
        have = {
            row[1]
            for row in db.conn.execute("PRAGMA table_info(cloud_sync_state)")
        }
        if "backoff_until_utc" not in have:
            db.conn.execute(
                "ALTER TABLE cloud_sync_state ADD COLUMN backoff_until_utc TEXT"
            )
        if "unaligned_skipped_total" not in have:
            db.conn.execute(
                "ALTER TABLE cloud_sync_state ADD COLUMN "
                "unaligned_skipped_total INTEGER NOT NULL DEFAULT 0"
            )


_STATE_COLUMNS = {
    "sensor_id",
    "harvest_from_utc",
    "harvest_through_utc",
    "harvest_complete",
    "last_attempt_at_utc",
    "last_success_at_utc",
    "last_error",
    "last_error_at_utc",
    "backoff_until_utc",
    "unaligned_skipped_total",
}


@dataclass
class CloudHour:
    hour_end_epoch: int
    hour_start_utc: str
    hour_end_utc: str
    consumption_millikwh: int
    generation_millikwh: int | None
    imported_millikwh: int | None
    exported_millikwh: int | None


def ws_to_millikwh(value) -> int | None:
    """Watt-seconds (number or numeric string) -> integer milli-kWh."""
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        ws = Decimal(value)
    elif isinstance(value, float):
        if not math.isfinite(value):
            return None
        ws = Decimal(math.trunc(value))
    elif isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        try:
            parsed = Decimal(text)
        except InvalidOperation:
            return None
        if not parsed.is_finite():
            return None
        ws = Decimal(int(parsed))  # truncate toward zero, like the TS client
    else:
        return None
    return int((ws / Decimal(3600)).quantize(Decimal("1"), rounding=ROUND_HALF_UP))


def parse_stats_item(item) -> CloudHour | None:
    """One /samples/stats item -> a CloudHour, or None to skip the item.

    Hours are derived from the returned start/end, never from an assumed
    grid, so 23- and 25-hour DST days store exactly what the API returned.
    An item without a usable consumption value is skipped: consumption is
    a merge input and a missing one must not become a stored zero.
    """
    if not isinstance(item, dict):
        return None
    try:
        start = parse_instant(item.get("start"))
        end = parse_instant(item.get("end"))
    except ValueError:
        return None
    consumption = ws_to_millikwh(item.get("consumptionEnergy"))
    if consumption is None:
        return None
    return CloudHour(
        hour_end_epoch=int(end.timestamp()),
        hour_start_utc=format_utc(start),
        hour_end_utc=format_utc(end),
        consumption_millikwh=consumption,
        generation_millikwh=ws_to_millikwh(item.get("generationEnergy")),
        imported_millikwh=ws_to_millikwh(item.get("importedEnergy")),
        exported_millikwh=ws_to_millikwh(item.get("exportedEnergy")),
    )


def store_cloud_hours(db: Database, items: list, fetched_at: datetime) -> tuple[int, int]:
    """Upsert stats items by hour end. Returns (stored, skipped).

    Re-fetching an hour replaces its values (late cloud corrections land),
    and ``last_fetched_at_utc`` advances, but ``fetched_at_utc`` keeps the
    first time this recorder saw the hour — that first-fetch time is the
    cloud-lateness measurement. Replacement is per quantity: an incoming
    NULL (a partial stats response that omitted the field) never erases a
    stored non-NULL value, while a real value — including a genuine
    zero — always replaces.

    Defense in depth: a bucket whose end is not hour-aligned is never
    stored. The syncer requests only aligned windows, but if the API
    ever buckets differently again, an off-hour row would overlap the
    real on-the-hour records and be priced as an extra hour. Skips are
    counted in the return value and accumulated in
    cloud_sync_state.unaligned_skipped_total (surfaced by
    /api/v1/status), and noted on stderr.
    """
    hours = []
    skipped = 0
    unaligned = 0
    for item in items:
        hour = parse_stats_item(item)
        if hour is None:
            skipped += 1
        elif hour.hour_end_epoch % 3600 != 0:
            skipped += 1
            unaligned += 1
        else:
            hours.append(hour)
    stamp = format_utc(fetched_at)
    ensure_cloud_tables(db)
    with db.write_lock:
        with db.transaction():
            if unaligned:
                db.conn.execute(
                    "INSERT OR IGNORE INTO cloud_sync_state(id) VALUES(1)"
                )
                db.conn.execute(
                    "UPDATE cloud_sync_state SET unaligned_skipped_total = "
                    "unaligned_skipped_total + ? WHERE id=1",
                    (unaligned,),
                )
            for hour in hours:
                db.conn.execute(
                    "INSERT INTO cloud_hourly_records(hour_end_epoch, hour_start_utc, "
                    "hour_end_utc, consumption_millikwh, generation_millikwh, "
                    "imported_millikwh, exported_millikwh, fetched_at_utc, "
                    "last_fetched_at_utc) VALUES(?,?,?,?,?,?,?,?,?) "
                    "ON CONFLICT(hour_end_epoch) DO UPDATE SET "
                    "hour_start_utc=excluded.hour_start_utc, "
                    "hour_end_utc=excluded.hour_end_utc, "
                    "consumption_millikwh=COALESCE(excluded.consumption_millikwh, "
                    "cloud_hourly_records.consumption_millikwh), "
                    "generation_millikwh=COALESCE(excluded.generation_millikwh, "
                    "cloud_hourly_records.generation_millikwh), "
                    "imported_millikwh=COALESCE(excluded.imported_millikwh, "
                    "cloud_hourly_records.imported_millikwh), "
                    "exported_millikwh=COALESCE(excluded.exported_millikwh, "
                    "cloud_hourly_records.exported_millikwh), "
                    "last_fetched_at_utc=excluded.last_fetched_at_utc",
                    (
                        hour.hour_end_epoch,
                        hour.hour_start_utc,
                        hour.hour_end_utc,
                        hour.consumption_millikwh,
                        hour.generation_millikwh,
                        hour.imported_millikwh,
                        hour.exported_millikwh,
                        stamp,
                        stamp,
                    ),
                )
    if unaligned:
        print(
            f"neurio cloud ingest: skipped {unaligned} stats bucket(s) "
            "whose end is not hour-aligned",
            file=sys.stderr,
        )
    return len(hours), skipped


def ensure_sync_state(db: Database) -> None:
    ensure_cloud_tables(db)
    with db.write_lock:
        with db.transaction():
            db.conn.execute(
                "INSERT OR IGNORE INTO cloud_sync_state(id) VALUES(1)"
            )


def read_sync_state(db: Database) -> dict:
    ensure_sync_state(db)
    row = db.conn.execute("SELECT * FROM cloud_sync_state WHERE id=1").fetchone()
    return dict(row)


def update_sync_state(db: Database, **fields) -> None:
    unknown = set(fields) - _STATE_COLUMNS
    if unknown:
        raise ValueError(f"unknown cloud_sync_state columns: {sorted(unknown)}")
    if not fields:
        return
    ensure_sync_state(db)
    assignments = ", ".join(f"{name}=?" for name in fields)
    with db.write_lock:
        with db.transaction():
            db.conn.execute(
                f"UPDATE cloud_sync_state SET {assignments} WHERE id=1",
                tuple(fields.values()),
            )
