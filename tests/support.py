"""Shared test helpers. Payloads built here are synthetic unless a test loads the observed fixture."""

from __future__ import annotations

import json
import os
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from neurio_collector.config import Config
from neurio_collector.db import Database
from neurio_collector.ingest import ingest_body
from neurio_collector.observed import SYNTHETIC_SENSOR_ID
from neurio_collector.timeutil import parse_instant

OBSERVED = Path(__file__).resolve().parent / "fixtures" / "observed"
SYNTHETIC = Path(__file__).resolve().parent / "fixtures" / "synthetic"


def make_config(**overrides) -> Config:
    data = dict(
        bind_address="127.0.0.1",
        bind_port=8095,
        data_dir="/tmp/neurio-test",
        neurio_url="http://192.168.1.50/current-sample",
        poll_interval_seconds=15,
        poll_timeout_seconds=3,
        poll_max_backoff_seconds=300,
        poll_jitter_ratio=0.0,
        poll_min_gap_seconds=1,
        stale_after_seconds=60,
        sample_stale_skew_seconds=180,
        device_ahead_skew_seconds=30,
        max_allocatable_gap_seconds=45,
        raw_retention_days=14,
        poll_log_retention_days=30,
        timezone="America/Chicago",
        household_channel_reads=True,
        topology_verified=False,
        generation_register="unset",
        consumption_source="consumption_channel",
        role_net_identity=None,
        role_generation_identity=None,
        role_consumption_identity=None,
        redact_identity=True,
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


def channel(typ: str, number: int, imp: int, exp: int, power: float = 0) -> dict:
    return {
        "type": typ,
        "ch": number,
        "eImp_Ws": imp,
        "eExp_Ws": exp,
        "p_W": power,
        "q_VAR": 0,
        "v_V": 120,
    }


def sample_bytes(sensor: str, timestamp: str | None, channels: list[dict], cts=None) -> bytes:
    body = {"sensorId": sensor, "channels": channels}
    if timestamp is not None:
        body["timestamp"] = timestamp
    if cts is not None:
        body["cts"] = cts
    return json.dumps(body).encode("utf-8")


def feed(db: Database, cfg: Config, rows: list[tuple], sensor: str = SYNTHETIC_SENSOR_ID):
    results = []
    for received, device_ts, channels in rows:
        body = sample_bytes(sensor, device_ts, channels)
        results.append(ingest_body(db, body, parse_instant(received), cfg))
    return results
