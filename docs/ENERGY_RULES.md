# Energy rules

Integer watt-seconds are the stored unit. `Wh = Ws / 3600`. `kWh = Ws / 3_600_000`. API and CSV kilowatt-hour fields are decimal strings quantized to 1e-9 kWh, which round back to the original integer. JSON numbers are not used for energy totals. Unknown values are null in JSON and empty in CSV. A measured zero stays zero.

## Channels

The November 6, 2026 Home Assistant recorder snapshot of `sensor.neurio_raw` showed five channels:

| Type | ch | Role used by this collector |
|---|---|---|
| PHASE_A_CONSUMPTION | 1 | Detail only. Never added to a household total. |
| PHASE_B_CONSUMPTION | 2 | Detail only. Never added to a household total. |
| NET | 3 | Grid import is `eImp_Ws`. Grid export is `eExp_Ws`. Net is import minus export. |
| GENERATION | 4 | Both counters are kept. They are not collapsed unless `NEURIO_GENERATION_REGISTER` is set. |
| CONSUMPTION | 5 | Dedicated consumption. Consumption is `eImp_Ws - eExp_Ws` on this channel. |

That snapshot is a channel-attribute object, not a full `/current-sample`. It had no `sensorId`, device timestamp, or `cts`. At that recorded state the powers were phase A 586 W, phase B 153 W, net 738 W, generation 0 W, and consumption 739 W. The one-watt difference is stored as reported. Cumulative import counters on the phases do not equal the consumption counter, so a power match is not a reason to overwrite either energy register.

Roles follow channel `type`. Channel numbers from that snapshot are the observed map, not a hardcoded requirement. A second channel of the same type makes that role null with an ambiguity reason instead of adding the channels together.

`cts` are stored and are not used for household energy. CT order is not treated as purpose.

## Household totals

With the default `NEURIO_CONSUMPTION_SOURCE=consumption_channel`:

- Grid import and export come from the unique `NET` channel counters.
- Consumption comes from the unique `CONSUMPTION` channel.
- Generation exposes `eImp` and `eExp` separately. `generation_collapsed_kwh` stays null while `NEURIO_GENERATION_REGISTER=unset`. The observed generation channel has a non-zero export counter, so collapsing it to `eImp` would drop measured energy.
- `max(net, 0)` is not import.
- Phases are not summed into consumption, generation, or grid.
- `net + generation` is not consumption unless `NEURIO_CONSUMPTION_SOURCE=derive_net_plus_generation` and `NEURIO_TOPOLOGY_VERIFIED=true`. The configuration rejects that derivation while topology is unverified.

`topology_verified` stays false until an operator sets it. Seeing these channel types in one recorder payload is a schema observation. It is not a certificate of CT placement, power sign, or reboot persistence. Household reads still name that provenance: `channel_type_observed_not_topology_verified`.

## Spans, resets, and gaps

Each applied sample moves the open epoch for each present counter.

- The first integer on a channel is a baseline. It creates no energy.
- A later counter that is greater or equal creates a span of `end - start` watt-seconds.
- A decrease closes the epoch with reason `decrease` and opens a new baseline. No negative span is stored.
- A repeated payload hash is a duplicate and creates no second sample.
- The same device timestamp with a different payload is a conflict and does not move counters.
- An older device timestamp is out of order and does not move counters.
- A different `sensorId` is a different sensor. Counters are not bridged.
- A payload without `sensorId`, including the recorder fixture, is stored and does not open epochs.

A span is `allocatable` only when both endpoints have synchronized device timestamps, the device gap is at most `NEURIO_MAX_ALLOCATABLE_GAP_SECONDS` (default 45), and the collector's receive gap is not large while the device gap looks short. Those spans can contribute to one bucket only when both endpoints lie inside that bucket.

Longer counter increases are kept as `unresolved_gap`. `NOT_SYNCHRONIZED`, missing, invalid, stale, or ahead timestamps are kept as `unresolved_clock` with `time_basis=receive`. Neither kind is divided across hours, and neither is written as zero. A span that crosses a bucket boundary is also left whole. It appears as preserved energy on the window, not as a fraction of each hour.

Power integration exists as a pure function and is not written into the database. It returns nothing for a gap longer than the allocatable limit.

## Time

Instants are stored in UTC. Exports default to `America/Chicago`. Hour buckets are UTC hours labeled in that zone, so March 8, 2026 has 23 hours and November 1, 2026 has 25 hours. The repeated 01:00 hour is two buckets, `-05:00` then `-06:00`. Fixed offsets such as `-05:00` are rejected as a timezone name. Duration comes from the UTC bounds, not from subtracting wall-clock labels.
