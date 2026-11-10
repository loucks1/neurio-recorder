# Observed Home Assistant recorder fixture

`ha-recorder-neurio-raw-channels-2026-11-06.json` is a real channel-attribute
payload recovered from Home Assistant recorder history for `sensor.neurio_raw`.

It is not a complete physical `GET /current-sample`. It has no `sensorId`, no
device timestamp, and no `cts` array. The `_provenance` object was added in this
repository so the file cannot be mistaken for a live device capture. The
channel objects are the recovered attributes.

The Home Assistant feed was reporting until 2026-11-06 19:30:50 America/Chicago
and became unavailable at 2026-11-06 19:31:06 America/Chicago. This fixture does
not show that the sensor was absent for a long time, and it is not a current
reachability test.

Do not treat these counters as a RatePlanTool import sample.
