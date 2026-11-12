"""Read-time merge helpers for the dual-source hourly view.

The merge is computed at read time from two independently stored
datasets: the bridge's ``hourly_records`` and the cloud ingest's
``cloud_hourly_records``. Nothing here writes. A third, historical
wrinkle: the one-off backfill batch ``backfill-20261110`` wrote rows
directly into ``hourly_records`` and marked them in
``hourly_backfill_provenance``. This process does not create or modify
that table. The live columns are listed in ``PROVENANCE_COLUMNS``.

Cloud-origin ``source`` values
-------------------------------
A provenance row counts as cloud-origin when ``source`` is one of:

- ``neurio_cloud_stats`` — Neurio ``/v1/samples/stats`` interval energy.
  These rows are cloud data. Their ``hourly_records`` net import/export
  columns hold that per-hour imported/exported energy (the backfill had
  no separate columns for it), not a lifetime counter.
- ``neurio_counters_ha`` — the historical Home Assistant import of Neurio
  cumulative energy counters. These are not ESP32 board rows (those are
  ``device`` and ``device_corrected``). Home Assistant's Neurio energy
  sensors are cumulative counters from the same Neurio/Generac account
  the cloud stats describe, so a later ``cloud_hourly_records`` hour
  replaces them without a divergence flag. Comparing the two would
  report two exports of the same account as a board-vs-cloud
  disagreement. The stored net import/export columns on these rows are
  counter snapshots, not per-hour interval energy.

``device`` and ``device_corrected`` stay board rows. Any other source
stays a board row. When one ``record_id`` has both a stats row and a
counters row, stats wins: that row's net columns are interval energy.

Cloud values are preferred over board values for an hour that exists in
both stored datasets. That preference is unchanged.
"""

from __future__ import annotations

# Live production columns of hourly_backfill_provenance. The backfill
# script is not in this repo; this list is the schema observed on the
# production database. The recorder only reads record_id and source.
PROVENANCE_COLUMNS = (
    "record_id",
    "batch_id",
    "action",
    "source",
    "method",
    "approximate",
    "detail_json",
    "original_row_json",
    "applied_at_utc",
)

# Column list above, as a CREATE TABLE tests can execute. Constraints
# beyond the column names are not known from the production file, so
# none are invented here. record_id is not declared unique: the table
# is an action log and a record may have more than one row.
PRODUCTION_PROVENANCE_DDL = """
CREATE TABLE hourly_backfill_provenance (
  record_id INTEGER,
  batch_id TEXT,
  action TEXT,
  source TEXT,
  method TEXT,
  approximate INTEGER,
  detail_json TEXT,
  original_row_json TEXT,
  applied_at_utc TEXT
);
"""

CLOUD_STATS_SOURCE = "neurio_cloud_stats"
CLOUD_COUNTERS_SOURCE = "neurio_counters_ha"
CLOUD_PROVENANCE_SOURCES = frozenset({CLOUD_STATS_SOURCE, CLOUD_COUNTERS_SOURCE})

# Divergence rule, per quantity: flagged when the absolute difference
# exceeds 10% of the cloud value AND exceeds 0.05 kWh (50 milli-kWh).
# Exactly at either threshold is not flagged.
_DIVERGENCE_RELATIVE_NUM = 1
_DIVERGENCE_RELATIVE_DEN = 10
_DIVERGENCE_ABSOLUTE_MILLIKWH = 50

MERGE_TABLES = ("cloud_hourly_records", "hourly_backfill_provenance")


def is_divergent(cloud_millikwh, esp32_millikwh) -> bool:
    """Integer-exact divergence check. A missing value on either side is
    not a divergence — there is nothing to compare."""
    if cloud_millikwh is None or esp32_millikwh is None:
        return False
    diff = abs(cloud_millikwh - esp32_millikwh)
    return (
        diff > _DIVERGENCE_ABSOLUTE_MILLIKWH
        and diff * _DIVERGENCE_RELATIVE_DEN
        > abs(cloud_millikwh) * _DIVERGENCE_RELATIVE_NUM
    )


def pct_difference(cloud_millikwh, esp32_millikwh):
    """|cloud - esp32| as a percentage of the cloud value, or None when
    the percentage is undefined (missing value, or a zero cloud value
    with a nonzero difference)."""
    if cloud_millikwh is None or esp32_millikwh is None:
        return None
    diff = abs(cloud_millikwh - esp32_millikwh)
    if cloud_millikwh == 0:
        return 0.0 if diff == 0 else None
    return round(diff / abs(cloud_millikwh) * 100, 2)


def existing_tables(reader) -> set:
    return {
        row[0]
        for row in reader.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name IN "
            "('cloud_hourly_records', 'hourly_backfill_provenance')"
        )
    }


def provenance_by_record(reader) -> dict:
    """record_id -> cloud-origin source.

    Empty when the provenance table does not exist. Raises sqlite3.Error
    when the table exists but does not have the live ``source`` column;
    callers that must keep the recorder up treat that as "no provenance"
    rather than as a database outage. A record with both cloud sources
    is reported as ``neurio_cloud_stats``.
    """
    if "hourly_backfill_provenance" not in existing_tables(reader):
        return {}
    found: dict = {}
    for record_id, source in reader.execute(
        "SELECT record_id, source FROM hourly_backfill_provenance"
    ):
        if source not in CLOUD_PROVENANCE_SOURCES:
            continue
        if found.get(record_id) == CLOUD_STATS_SOURCE:
            continue
        found[record_id] = source
    return found
