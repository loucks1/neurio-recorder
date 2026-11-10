# General exports

These files are the collector's own contract. `rateplantool_contract` is false on every response. The reader described in `RATEPLANTOOL_CONTRACT.md` expects a JSON array of cumulative watt-seconds in `consumptionEnergy` and `generationEnergy`, not these columns. Feeding it this CSV would treat interval kilowatt-hours as watt-second cumulatives.

## JSON

- `GET /api/v1/energy?date=YYYY-MM-DD&interval=hour&timezone=America/Chicago`
- `interval=day` with `date=`
- `interval=hour` or `interval=range` with `start=` and `end=` as offset-aware instants

Each bucket includes UTC bounds, local bounds, both UTC offsets, duration in seconds, fold, per-channel allocated watt-seconds, and preserved watt-seconds grouped by allocation status. Household fields are separate and use the rules in `ENERGY_RULES.md`.

`allocated_*` is only the counter delta of spans wholly inside that bucket. `preserved_ws_by_status` keeps gap and clock-problem energy that is wholly inside the bucket without calling it an hourly allocation. The window object keeps spans that are wholly inside the query and not inside a single hour. Spans that cross the query boundary are counted and excluded, not prorated.

## CSV

- `/api/v1/export/general-hourly.csv`
- `/api/v1/export/general-household-hourly.csv`
- `/api/v1/export/samples.csv`
- `/api/v1/export/spans.csv`

Hourly rows include `bucket_start_utc`, `bucket_end_utc`, `bucket_start_local`, `bucket_end_local`, `utc_offset_start`, `utc_offset_end`, and `duration_seconds`. Empty cells are unknown. The column `grid_import_derived_kwh` is intentionally absent.

Samples and spans are paginated (`limit` 1..1000, `cursor`). Pass `start` and `end`.

Sensor IDs and submeter labels are omitted while `NEURIO_REDACT_IDENTITY=true` (the default). A stable `sensor_token` is the first 16 hex characters of the SHA-256 of the sensor id. Rows built from `SYNTHETIC-NOT-A-DEVICE` are marked `synthetic_fixture`. The recorder fixture has no sensor id and is marked `unidentified_payload`.
