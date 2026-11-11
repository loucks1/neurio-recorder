"""Hourly MQTT recorder for a Neurio/PWRview monitor.

The process subscribes to hourly energy records, stores them in SQLite, and
serves a read-only API. It does not poll the monitor and it does not read
Home Assistant.
"""

__version__ = "1.0.0"
