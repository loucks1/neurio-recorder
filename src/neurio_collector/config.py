"""Process configuration.

BIND_ADDRESS has no default. Unspecified addresses (0.0.0.0, ::) are refused
unless the operator sets ALLOW_CONTAINER_WILDCARD_BIND=host-publish-pinned.
The only local source is MQTT hourly records. Device polling and the Home
Assistant relay are gone, and a non-empty MQTT_RAW_TOPIC is rejected. Cloud
ingest (NEURIO_CLIENT_ID / NEURIO_CLIENT_SECRET) is a separate, optional,
outbound-only source configured at the bottom of this module's validation.
"""

from __future__ import annotations

import ipaddress
import os
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlparse

from .timeutil import parse_instant


WILDCARD_OPT_IN = "host-publish-pinned"
_REMOVED_ENV = (
    "NEURIO_URL",
    "HA_URL",
    "HA_ENTITY",
    "HA_TOKEN",
    "HA_TOKEN_FILE",
    "HA_SOURCE_ID",
)


class ConfigError(ValueError):
    pass


class Secret:
    """A credential that must not appear in repr, str, or logs."""

    def __init__(self, value: str):
        self._value = value

    def reveal(self) -> str:
        return self._value

    def __repr__(self) -> str:
        return "Secret(<redacted>)"

    def __str__(self) -> str:
        return "<redacted>"

    def __eq__(self, other: object) -> bool:
        return isinstance(other, Secret) and self._value == other._value


@dataclass(frozen=True)
class Config:
    bind_address: str
    bind_port: int
    data_dir: str
    source_mode: str = "mqtt"
    mqtt_broker_host: str | None = None
    mqtt_broker_port: int = 1883
    mqtt_tls: bool = False
    mqtt_username: str | None = None
    mqtt_password: Secret | None = None
    mqtt_client_id: str = "neurio-recorder"
    mqtt_hourly_topic: str = "neurio/hourly/energy"
    mqtt_ack_topic: str = "neurio/hourly/ack"
    mqtt_keepalive_seconds: int = 60
    cloud_client_id: str | None = None
    cloud_client_secret: Secret | None = None
    cloud_grant_type: str = "client_credentials"
    cloud_sensor_id: str | None = None
    cloud_api_base: str = "https://api.neur.io/v1"
    cloud_sync_interval_seconds: float = 3600.0
    cloud_tail_hours: int = 48
    cloud_harvest_from: str | None = None
    cloud_timeout_seconds: float = 30.0
    poll_interval_seconds: float = 15.0
    poll_timeout_seconds: float = 3.0
    poll_max_backoff_seconds: float = 300.0
    poll_jitter_ratio: float = 0.2
    poll_min_gap_seconds: float = 1.0
    hourly_stale_after_seconds: float = 3900.0
    max_response_bytes: int = 1_000_000
    allow_container_wildcard_bind: str | None = None
    ephemeral_port: bool = False

    @property
    def db_path(self) -> str:
        return os.path.join(self.data_dir, "neurio.sqlite")

    @property
    def lock_path(self) -> str:
        return os.path.join(self.data_dir, "collector.lock")

    @property
    def cloud_configured(self) -> bool:
        """Cloud ingest is on only when both OAuth credentials are present.

        It is independent of the MQTT source: the cloud is a second,
        outbound-only source alongside the hourly MQTT records.
        """
        return bool(
            self.cloud_client_id
            and self.cloud_client_id.strip()
            and self.cloud_client_secret is not None
            and self.cloud_client_secret.reveal()
        )

    def validate(self) -> None:
        _validate_bind(
            self.bind_address,
            self.bind_port,
            self.allow_container_wildcard_bind,
            self.ephemeral_port,
        )
        if not self.data_dir or not os.path.isabs(self.data_dir):
            raise ConfigError("DATA_DIR must be an absolute path")
        if self.source_mode != "mqtt":
            raise ConfigError(
                "SOURCE_MODE must be mqtt. Device polling and the Home Assistant "
                "relay have been removed."
            )
        validate_mqtt_settings(self)
        validate_cloud_settings(self)
        if not 0 < self.poll_interval_seconds <= 3600:
            raise ConfigError(
                "NEURIO_POLL_INTERVAL_SECONDS must be > 0 and <= 3600. "
                "It is the base of the MQTT reconnect backoff."
            )
        if not 0 < self.poll_timeout_seconds <= 120:
            raise ConfigError(
                "NEURIO_POLL_TIMEOUT_SECONDS must be > 0 and <= 120. "
                "It is the MQTT socket timeout."
            )
        if self.poll_max_backoff_seconds < self.poll_interval_seconds:
            raise ConfigError("NEURIO_POLL_MAX_BACKOFF_SECONDS must be >= the poll interval")
        if not 0 <= self.poll_jitter_ratio <= 1:
            raise ConfigError("NEURIO_POLL_JITTER_RATIO must be between 0 and 1")
        if self.poll_min_gap_seconds < 0:
            raise ConfigError("NEURIO_POLL_MIN_GAP_SECONDS must be >= 0")
        if self.hourly_stale_after_seconds <= 0:
            raise ConfigError("NEURIO_HOURLY_STALE_AFTER_SECONDS must be positive")
        if self.max_response_bytes < 1024:
            raise ConfigError("NEURIO_MAX_RESPONSE_BYTES is too small")

    @classmethod
    def from_env(cls, env: dict[str, str] | None = None) -> "Config":
        values = os.environ if env is None else env
        source_mode = _required(values, "SOURCE_MODE")
        if source_mode != "mqtt":
            raise ConfigError(
                "SOURCE_MODE must be mqtt. Device polling and the Home Assistant "
                "relay have been removed."
            )
        for name in _REMOVED_ENV:
            if (values.get(name) or "").strip():
                raise ConfigError(
                    f"{name} is not used. This recorder stores MQTT hourly records only."
                )
        if (values.get("MQTT_RAW_TOPIC") or "").strip():
            raise ConfigError(
                "MQTT_RAW_TOPIC is no longer supported. This recorder stores hourly "
                "records only. Unset MQTT_RAW_TOPIC."
            )
        cfg = cls(
            bind_address=_required(values, "BIND_ADDRESS"),
            bind_port=_required_int(values, "BIND_PORT"),
            data_dir=_required(values, "DATA_DIR"),
            source_mode="mqtt",
            mqtt_broker_host=_required(values, "MQTT_BROKER_HOST"),
            mqtt_broker_port=_optional_int(values, "MQTT_BROKER_PORT", 1883),
            mqtt_tls=_optional_bool(values, "MQTT_TLS", False),
            mqtt_username=_optional_blank(values, "MQTT_USERNAME"),
            mqtt_password=_read_secret(values, "MQTT_PASSWORD_FILE", "MQTT_PASSWORD"),
            mqtt_client_id=_optional_str(values, "MQTT_CLIENT_ID", "neurio-recorder"),
            mqtt_hourly_topic=_optional_str(
                values, "MQTT_HOURLY_TOPIC", "neurio/hourly/energy"
            ),
            mqtt_ack_topic=_optional_str(values, "MQTT_ACK_TOPIC", "neurio/hourly/ack"),
            mqtt_keepalive_seconds=_optional_int(values, "MQTT_KEEPALIVE_SECONDS", 60),
            cloud_client_id=_optional_blank(values, "NEURIO_CLIENT_ID"),
            cloud_client_secret=_read_secret(
                values, "NEURIO_CLIENT_SECRET_FILE", "NEURIO_CLIENT_SECRET"
            ),
            cloud_grant_type=_optional_str(
                values, "NEURIO_GRANT_TYPE", "client_credentials"
            ),
            cloud_sensor_id=_optional_blank(values, "NEURIO_CLOUD_SENSOR_ID"),
            cloud_api_base=_optional_str(
                values, "NEURIO_CLOUD_API_BASE", "https://api.neur.io/v1"
            ).rstrip("/"),
            cloud_sync_interval_seconds=_optional_float(
                values, "NEURIO_CLOUD_SYNC_INTERVAL_SECONDS", 3600.0
            ),
            cloud_tail_hours=_optional_int(values, "NEURIO_CLOUD_TAIL_HOURS", 48),
            cloud_harvest_from=_optional_blank(values, "NEURIO_CLOUD_HARVEST_FROM"),
            cloud_timeout_seconds=_optional_float(
                values, "NEURIO_CLOUD_TIMEOUT_SECONDS", 30.0
            ),
            poll_interval_seconds=_optional_float(values, "NEURIO_POLL_INTERVAL_SECONDS", 15.0),
            poll_timeout_seconds=_optional_float(values, "NEURIO_POLL_TIMEOUT_SECONDS", 3.0),
            poll_max_backoff_seconds=_optional_float(
                values, "NEURIO_POLL_MAX_BACKOFF_SECONDS", 300.0
            ),
            poll_jitter_ratio=_optional_float(values, "NEURIO_POLL_JITTER_RATIO", 0.2),
            poll_min_gap_seconds=_optional_float(values, "NEURIO_POLL_MIN_GAP_SECONDS", 1.0),
            hourly_stale_after_seconds=_optional_float(
                values, "NEURIO_HOURLY_STALE_AFTER_SECONDS", 3900.0
            ),
            max_response_bytes=_optional_int(values, "NEURIO_MAX_RESPONSE_BYTES", 1_000_000),
            allow_container_wildcard_bind=_optional_blank(
                values, "ALLOW_CONTAINER_WILDCARD_BIND"
            ),
        )
        cfg.validate()
        return cfg


def _read_secret(env: dict[str, str], file_key: str, inline_key: str) -> Secret | None:
    """A secret from a file key or an inline key. None when neither carries a value."""
    file_name = (env.get(file_key) or "").strip()
    inline = (env.get(inline_key) or "").strip()
    if file_name and inline:
        raise ConfigError(f"set {file_key} or {inline_key}, not both")
    if file_key in env and inline_key in env and not file_name and not inline:
        raise ConfigError(f"{inline_key} and {file_key} are both empty")
    if file_name:
        try:
            value = Path(file_name).read_text(encoding="utf-8").strip()
        except OSError as exc:
            raise ConfigError(f"{file_key} could not be read") from exc
        if not value:
            raise ConfigError(f"{file_key} is empty")
        return Secret(value)
    if inline:
        return Secret(inline)
    return None


def validate_cloud_settings(cfg: "Config") -> None:
    """Cloud ingest settings. The cloud API base configured here is the one
    sanctioned cloud destination; it has nothing to do with the removed
    device-polling and Home Assistant URLs."""
    has_id = bool(cfg.cloud_client_id and cfg.cloud_client_id.strip())
    has_secret = (
        cfg.cloud_client_secret is not None and bool(cfg.cloud_client_secret.reveal())
    )
    if has_id != has_secret:
        raise ConfigError(
            "NEURIO_CLIENT_ID and NEURIO_CLIENT_SECRET (or "
            "NEURIO_CLIENT_SECRET_FILE) must be set together. Cloud ingest "
            "is disabled when neither is set."
        )
    if not has_id:
        return
    if not cfg.cloud_grant_type or not cfg.cloud_grant_type.strip():
        raise ConfigError("NEURIO_GRANT_TYPE must not be empty")
    parsed = urlparse(cfg.cloud_api_base)
    if parsed.username or parsed.password or parsed.query or parsed.fragment:
        raise ConfigError(
            "NEURIO_CLOUD_API_BASE must not include userinfo, a query, or a fragment"
        )
    host = parsed.hostname
    if not host:
        raise ConfigError("NEURIO_CLOUD_API_BASE is missing a host")
    if parsed.scheme == "http" and host not in {"127.0.0.1", "::1", "localhost"}:
        raise ConfigError(
            "NEURIO_CLOUD_API_BASE must be https. Plain http is accepted only "
            "for a loopback host, so a local test server can stand in for the "
            "cloud; the OAuth client secret must never cross the network "
            "unencrypted."
        )
    if parsed.scheme not in {"https", "http"}:
        raise ConfigError("NEURIO_CLOUD_API_BASE must be https (or loopback http)")
    if cfg.cloud_sync_interval_seconds < 60:
        raise ConfigError("NEURIO_CLOUD_SYNC_INTERVAL_SECONDS must be at least 60")
    if not 1 <= cfg.cloud_tail_hours <= 2160:
        raise ConfigError("NEURIO_CLOUD_TAIL_HOURS must be between 1 and 2160")
    if not 0 < cfg.cloud_timeout_seconds <= 300:
        raise ConfigError("NEURIO_CLOUD_TIMEOUT_SECONDS must be > 0 and <= 300")
    if cfg.cloud_harvest_from:
        try:
            parse_instant(cfg.cloud_harvest_from)
        except ValueError as exc:
            raise ConfigError(
                "NEURIO_CLOUD_HARVEST_FROM must be an ISO timestamp with an offset"
            ) from exc


def validate_mqtt_settings(cfg: "Config") -> None:
    host = cfg.mqtt_broker_host
    if not host or not host.strip():
        raise ConfigError("SOURCE_MODE=mqtt requires MQTT_BROKER_HOST. There is no default.")
    host = host.strip()
    if "://" in host or "/" in host or ":" in host or any(ch.isspace() for ch in host):
        raise ConfigError(
            "MQTT_BROKER_HOST must be a hostname or IP address, not a URL. "
            "Set the port with MQTT_BROKER_PORT."
        )
    port = cfg.mqtt_broker_port
    if not isinstance(port, int) or isinstance(port, bool) or not 1 <= port <= 65535:
        raise ConfigError("MQTT_BROKER_PORT must be an integer from 1 to 65535")
    if not cfg.mqtt_client_id or not cfg.mqtt_client_id.strip():
        raise ConfigError("MQTT_CLIENT_ID must not be empty")
    for name, topic in (
        ("MQTT_HOURLY_TOPIC", cfg.mqtt_hourly_topic),
        ("MQTT_ACK_TOPIC", cfg.mqtt_ack_topic),
    ):
        if not topic or not topic.strip():
            raise ConfigError(f"{name} must not be empty")
        if "+" in topic or "#" in topic:
            raise ConfigError(
                f"{name} must be an exact topic. Wildcards (+, #) are not accepted."
            )
    has_username = bool(cfg.mqtt_username and cfg.mqtt_username.strip())
    has_password = cfg.mqtt_password is not None and bool(cfg.mqtt_password.reveal())
    if has_username and not has_password:
        raise ConfigError(
            "MQTT_USERNAME is set but no MQTT password is configured. "
            "Set MQTT_PASSWORD_FILE or MQTT_PASSWORD, or unset MQTT_USERNAME."
        )
    if has_password and not has_username:
        raise ConfigError(
            "An MQTT password is configured but MQTT_USERNAME is not set. "
            "Set both, or neither for an anonymous broker login."
        )
    keepalive = cfg.mqtt_keepalive_seconds
    if not isinstance(keepalive, int) or isinstance(keepalive, bool) or not 0 <= keepalive <= 65535:
        raise ConfigError("MQTT_KEEPALIVE_SECONDS must be an integer from 0 to 65535")
    if cfg.poll_interval_seconds <= 0:
        raise ConfigError(
            "NEURIO_POLL_INTERVAL_SECONDS must be positive. "
            "It is the base of the reconnect backoff."
        )


def _validate_bind(address: str, port: int, opt_in: str | None, ephemeral: bool) -> None:
    if not address or not str(address).strip():
        raise ConfigError(
            "BIND_ADDRESS is required. There is no default, and 0.0.0.0 is not assumed."
        )
    try:
        ip = ipaddress.ip_address(address.strip())
    except ValueError as exc:
        raise ConfigError("BIND_ADDRESS must be an IP address, not a hostname") from exc
    if ip.is_unspecified and opt_in != WILDCARD_OPT_IN:
        raise ConfigError(
            "BIND_ADDRESS refuses 0.0.0.0 and ::. Set an approved address, or set "
            "ALLOW_CONTAINER_WILDCARD_BIND=host-publish-pinned together with a host "
            "port publish pinned to that approved address."
        )
    if ephemeral:
        if port != 0:
            raise ConfigError("ephemeral_port requires BIND_PORT=0")
        return
    if not isinstance(port, int) or isinstance(port, bool) or not 1 <= port <= 65535:
        raise ConfigError("BIND_PORT must be an integer from 1 to 65535")


def _required(env: dict[str, str], name: str) -> str:
    value = env.get(name)
    if value is None or not value.strip():
        raise ConfigError(
            f"{name} is required. There is no default. Refusing to start."
        )
    return value.strip()


def _required_int(env: dict[str, str], name: str) -> int:
    raw = _required(env, name)
    try:
        return int(raw)
    except ValueError as exc:
        raise ConfigError(f"{name} must be an integer") from exc


def _optional_str(env: dict[str, str], name: str, default: str) -> str:
    value = env.get(name)
    if value is None or not value.strip():
        return default
    return value.strip()


def _optional_blank(env: dict[str, str], name: str) -> str | None:
    value = env.get(name)
    if value is None or not value.strip():
        return None
    return value.strip()


def _optional_int(env: dict[str, str], name: str, default: int) -> int:
    value = env.get(name)
    if value is None or not value.strip():
        return default
    try:
        return int(value.strip())
    except ValueError as exc:
        raise ConfigError(f"{name} must be an integer") from exc


def _optional_float(env: dict[str, str], name: str, default: float) -> float:
    value = env.get(name)
    if value is None or not value.strip():
        return default
    try:
        return float(value.strip())
    except ValueError as exc:
        raise ConfigError(f"{name} must be a number") from exc


def _optional_bool(env: dict[str, str], name: str, default: bool) -> bool:
    value = env.get(name)
    if value is None or not value.strip():
        return default
    text = value.strip().lower()
    if text == "true":
        return True
    if text == "false":
        return False
    raise ConfigError(f"{name} must be true or false")
