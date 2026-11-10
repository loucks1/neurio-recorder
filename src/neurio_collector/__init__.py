"""LAN collector for a physical Neurio/PWRview sensor.

This package polls the device's local current-sample endpoint, stores raw and
normalized readings, and serves read-only history. It does not call Neurio
cloud APIs and it does not change Home Assistant's existing poll.
"""

__version__ = "1.0.0"
