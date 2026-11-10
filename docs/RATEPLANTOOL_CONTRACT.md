# RatePlanTool contract

This note is the Neurio reader contract this collector refuses to emulate. It matches commit `78fc57c348e93e11cc6b3cf26f02fb292c54bbfe`. This collector still has `rateplantool_contract=false` and does not speak that protocol.

## What that reader expects

`NeurioService` is the only energy reader. There is no CSV import and no energy POST.

1. `POST {base_URL}oauth2/token` as `application/x-www-form-urlencoded` with env `grant_type`, `client_id`, and `client_secret`. The response must contain `access_token`. Later calls send `Authorization: Bearer`.
2. `GET {base_URL}users/current`. The client keeps `locations[0].sensors[0]` from `sensorId` or `SensorID`. A sensor object that only has `id` leaves the id null.
3. For each civil day, `GET {base_URL}samples` with `start`, `end`, `granularity=hours`, `perPage=500`, and `sensorId`. There is no second page. `end` is `start + 24 hours`, not the next America/Chicago midnight.

`base_URL` in a typical launch profile is `https://api.neur.io/v1/`. This collector does not implement those three routes and does not accept a bearer token. The synthetic token in the stub fixture is a placeholder, not a credential.

The samples body is a JSON array. One object fails deserialization. Each element uses cumulative watt-seconds:

| JSON | Stored | Behavior |
|---|---|---|
| `timestamp` | `intervalTS` UTC | `DateTime.Parse(value).ToUniversalTime()`. A value with no offset is local to the process. The Docker image sets `TZ=America/Chicago`. The accepted fixture uses `Z`. |
| `consumptionEnergy` | `consumption` ulong | Neurio consumption cumulative. Omitted means `0`. |
| `generationEnergy` | `generation` ulong | Neurio generation cumulative. Omitted means `0`. |
| `netEnergy` | deserialized then dropped | `Sample` has no net field. The fixture sets every `netEnergy` to `1` and the emitted kWh still follow consumption and generation. |
| `consumptionPower`, `generationPower`, `netPower` | ignored | The fixture carries the constants 111, 222, and 333. |

`null`, a negative number, or a fraction such as `3600000.0` fails the whole array. Private storage names (`intervalTS`, `consumption`, `generation`, `net`) do not bind.

`Samples.RawData` uses point 0 as a baseline only. For each later point it emits `(current - previous) / 3_600_000` kilowatt-hours at `timestamp - 1 hour`. The elapsed time is not part of the divisor. A decrease emits `0` kWh. An empty array throws and the UI shows `Error getting all samples`.

Consumption in this UI is the Neurio consumption cumulative. Generation is the Neurio generation cumulative. After rate buckets, net usage is `consumption - generation`. There is no import field and no export field. Putting grid import in `consumptionEnergy` and grid export in `generationEnergy` would feed different quantities from the ones this UI subtracts.

## Measured parser results

The synthetic body `tests/fixtures/rateplantool/rateplantool-synthetic-samples.json` has SHA-256 `312a2bd55403487ecdebf494995a3ac645a5262649b330ec02fe0625f3da9509`. For civil 2026-01-15 the recorded request used `start=2026-01-15T06:00:00.0000000Z` and `end=2026-01-16T06:00:00.0000000Z` and emitted:

| Emitted UTC | Chicago | Consumption kWh | Generation kWh |
|---|---|---|---|
| 2026-01-15T21:00:00Z | 15:00 | 1.5 | 0.25 |
| 2026-01-15T22:00:00Z | 16:00 | 2 | 0 |
| 2026-01-15T23:00:00Z | 17:00 | 0.5 | 0.5 |
| 2026-01-16T00:00:00Z | 18:00 | 0.5 | 0 |
| 2026-01-16T03:00:00Z | 21:00 | 3 | 1 |
| 2026-01-16T04:00:00Z | 22:00 | 0.1 | 0 |
| 2026-01-16T05:00:00Z | 23:00 | 0 | 0.2 |

The 3 kWh / 1 kWh row is the entire jump from raw `01:00Z` to raw `04:00Z`. Chicago hours 19 and 20 are absent, not zero. The 23:00 consumption `0` is the reset to cumulative `42`. Generation on that same step still advanced 0.2 kWh.

Fixed 24-hour requests, measured from `GetMidnightDate`:

| Civil date | start | end (`start+24h`) | Next day start | Boundary |
|---|---|---|---|---|
| 2026-01-15 | 06:00Z | 2026-01-16 06:00Z | same instant | adjacent |
| 2026-07-15 | 05:00Z | 2026-07-16 05:00Z | same instant | adjacent |
| 2026-03-08 | 06:00Z | 2026-03-09 06:00Z (Mar 9 01:00 CDT) | 2026-03-09 05:00Z | 1 hour overlap, and `getSamples` does not distinct timestamps, so the overlap can be summed twice |
| 2026-11-01 | 05:00Z | 2026-11-02 05:00Z (Nov 1 23:00 CST) | 2026-11-02 06:00Z | the 23:00–midnight hour is never requested |

On an in-progress day, the memory-cache expiry uses process-local `lastRaw.ToLocalTime().Hour + 1`. For `2026-12-02T05:00:00Z` in `TZ=America/Chicago` that hour is 23, `Hour + 1` is 24, and `new DateTime` throws. The day returns no rows. The Docker image sets `TZ=America/Chicago`. A UTC process that writes LiteDB directly also shifts rate hours, because `Sample.IntervalTS` is Chicago wall time with `Kind=Unspecified`.

## Why this collector does not adapt

General CSV and `/api/v1/energy` are interval views with nulls for unknown hours. That reader would treat `consumptionEnergy` on those rows as cumulative watt-seconds. They are not importable.

A local stand-in that answered `oauth2/token`, `users/current`, and `samples` would also have to invent a cumulative at each hour boundary. That fabrication is rejected here. Any real gap in the cumulative series is then assigned by the existing parser to `laterTimestamp - 1 hour`. A counter reset becomes a measured zero. The fall-back hour cannot be delivered because the client never asks for it. The spring-forward overlap can be counted twice. Those outcomes belong to that reader, so a careful samples array from this collector cannot prevent them.

`refuse_hour_boundary_export()` returns `export: null` and the reason list. There is no route that returns a samples array.

## Minimal change on that side

Do not point `base_URL` at this collector. The change is not in this tree.

1. In `NeurioService`, set each day's `end` to the next America/Chicago midnight instead of `start + 24 hours`. De-duplicate on timestamp before a multi-day concatenation so the spring-forward overlap hour is not added twice.
2. In `Samples.set_RawData`, stop emitting a multi-hour cumulative delta at `timestamp - 1 hour`. Leave the uncovered hours absent and keep the delta out of the rate sum unless a new explicit quality field says the span is a single measured hour. A decrease must not become `0` kWh. Record it as a reset.
3. For the in-progress day, do not build a cache expiry from `Hour + 1` when that sum is 24.
4. After those three fixes, add one method that accepts interval rows `{intervalStartUtc, intervalEndUtc, consumptionWs or null, generationWs or null, quality}` and appends them without calling the cumulative differencer. `null` and any quality other than a single allocated hour stay out of the rate buckets.

`consumptionWs` in that method has to be the dedicated Neurio consumption counter delta, and `generationWs` the generation counter delta. Grid import and grid export stay out of those two fields. Net inside that UI remains `consumption - generation` after bucketing.

Until that method exists, this collector's integration state is `not_wired`.
