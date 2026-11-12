"""Read-time merge helpers for the dual-source hourly view (step 3).

The merge is computed at read time from two independently stored
datasets: the bridge's ``hourly_records`` and the cloud ingest's
``cloud_hourly_records``. Nothing here writes. A third, historical
wrinkle: the one-off backfill batch ``backfill-20261110`` wrote cloud
rows directly into ``hourly_records`` and marked them in
``hourly_backfill_provenance`` — those rows are cloud data and are
treated as such.
"""

from __future__ import annotations

# hourly_backfill_provenance.backfill_source value written by the
# backfill batch for rows that came from Neurio cloud stats.
CLOUD_PROVENANCE_SOURCE = "neurio_cloud_stats"

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


def provenance_cloud_ids(reader) -> set:
    """record_ids inside hourly_records that are cloud data per the
    backfill provenance table. Empty when the table does not exist."""
    if "hourly_backfill_provenance" not in existing_tables(reader):
        return set()
    return {
        row[0]
        for row in reader.execute(
            "SELECT record_id FROM hourly_backfill_provenance "
            "WHERE backfill_source=?",
            (CLOUD_PROVENANCE_SOURCE,),
        )
    }
