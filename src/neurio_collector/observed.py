"""Recorded observations. These are not live device readings."""

# Example LAN current-sample URL for documentation and for the status payload.
# It is not contacted unless an operator sets NEURIO_URL to it. Home Assistant's
# own poll, when one exists, belongs to Home Assistant. This collector does not
# use a 5-second interval and must not change that poll.
CANDIDATE_DEVICE_URL = "http://192.168.1.50/current-sample"
CANDIDATE_DEVICE_URL_EXPLICIT_PORT = "http://192.168.1.50:80/current-sample"
HA_POLL_INTERVAL_SECONDS = 5
COLLECTOR_MIN_POLL_INTERVAL_SECONDS = 10
COLLECTOR_DEFAULT_POLL_INTERVAL_SECONDS = 15

# America/Chicago. November 6, 2026 is after the fall-back, so the offset is
# -06:00. The same instants in America/New_York (EST, UTC-5) are 8:30:50 PM
# and 8:31:06 PM.
LAST_VALID_NET_CHICAGO = "2026-11-06T19:30:50-06:00"
BECAME_UNAVAILABLE_CHICAGO = "2026-11-06T19:31:06-06:00"

# Channel types and numbers recovered from Home Assistant recorder attributes
# for sensor.neurio_raw. This is a schema observation, not a completed
# current-sample and not a CT-placement certificate.
OBSERVED_CHANNEL_MAP = (
    ("PHASE_A_CONSUMPTION", 1),
    ("PHASE_B_CONSUMPTION", 2),
    ("NET", 3),
    ("GENERATION", 4),
    ("CONSUMPTION", 5),
)

MISSING_SENSOR_ID = "__payload_missing_sensor_id__"
SYNTHETIC_SENSOR_ID = "SYNTHETIC-NOT-A-DEVICE"
