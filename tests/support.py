"""Shared test helpers."""

from __future__ import annotations

import os
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from neurio_collector.config import Config
from neurio_collector.db import Database


def make_config(**overrides) -> Config:
    data = dict(
        bind_address="127.0.0.1",
        bind_port=8095,
        data_dir="/tmp/neurio-test",
        source_mode="mqtt",
        mqtt_broker_host="192.168.1.10",
        mqtt_broker_port=1883,
        mqtt_tls=False,
        mqtt_username=None,
        mqtt_password=None,
        mqtt_client_id="neurio-recorder",
        mqtt_hourly_topic="neurio/hourly/energy",
        mqtt_ack_topic="neurio/hourly/ack",
        mqtt_keepalive_seconds=60,
        poll_interval_seconds=15,
        poll_timeout_seconds=3,
        poll_max_backoff_seconds=300,
        poll_jitter_ratio=0.0,
        poll_min_gap_seconds=1,
        hourly_stale_after_seconds=3900,
        max_response_bytes=1_000_000,
        allow_container_wildcard_bind=None,
        ephemeral_port=False,
    )
    data.update(overrides)
    cfg = Config(**data)
    cfg.validate()
    return cfg


def open_db(testcase) -> Database:
    tmp = tempfile.TemporaryDirectory()
    testcase.addCleanup(tmp.cleanup)
    db = Database(os.path.join(tmp.name, "neurio.sqlite"))
    testcase.addCleanup(db.close)
    return db
