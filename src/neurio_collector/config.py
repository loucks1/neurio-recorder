"""Process configuration.

BIND_ADDRESS has no default. Unspecified addresses (0.0.0.0, ::) are refused
unless the operator sets ALLOW_CONTAINER_WILDCARD_BIND=host-publish-pinned.
The device URL has no default either, so a process never probes the LAN until
the endpoint is set. An example URL is in observed.py.
"""

from __future__ import annotations

import ipaddress
import os
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlparse

from .observed import COLLECTOR_MIN_POLL_INTERVAL_SECONDS
from .timeutil import load_zone

CLOUD_HOST_SUFFIXES = (".neur.io",)
CLOUD_HOSTS = {"neur.io", "api.neur.io", "www.neur.io"}
WILDCARD_OPT_IN = "host-publish-pinned"
RFC1918_HTTP_OPT_IN = "accept-unencrypted-bearer"
_RFC1918 = (
    ipaddress.ip_network("10.0.0.0/8"),
    ipaddress.ip_network("172.16.0.0/12"),
    ipaddress.ip_network("192.168.0.0/16"),
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
    neurio_url: str
    source_mode: str = "device"
    ha_url: str | None = None
    ha_entity: str | None = None
    ha_source_id: str | None = None
    ha_token: Secret | None = None
    ha_allow_rfc1918_http: str | None = None
    mqtt_broker_host: str | None = None
    mqtt_broker_port: int = 1883
    mqtt_tls: bool = False
    mqtt_username: str | None = None
    mqtt_password: Secret | None = None
    mqtt_client_id: str = "neurio-recorder"
    mqtt_raw_topic: str = "neurio/raw"
    mqtt_hourly_topic: str = "neurio/hourly/energy"
    mqtt_ack_topic: str = "neurio/hourly/ack"
    mqtt_keepalive_seconds: int = 60
    poll_interval_seconds: float = 15.0
    poll_timeout_seconds: float = 3.0
    poll_max_backoff_seconds: float = 300.0
    poll_jitter_ratio: float = 0.2
    poll_min_gap_seconds: float = 1.0
    stale_after_seconds: float = 60.0
    hourly_stale_after_seconds: float = 3900.0
    sample_stale_skew_seconds: float = 180.0
    device_ahead_skew_seconds: float = 30.0
    max_allocatable_gap_seconds: float = 45.0
    raw_retention_days: int = 14
    poll_log_retention_days: int = 30
    timezone: str = "America/Chicago"
    household_channel_reads: bool = True
    topology_verified: bool = False
    generation_register: str = "unset"
    consumption_source: str = "consumption_channel"
    role_net_identity: str | None = None
    role_generation_identity: str | None = None
    role_consumption_identity: str | None = None
    redact_identity: bool = True
    max_response_bytes: int = 1_000_000
    allow_container_wildcard_bind: str | None = None
    ephemeral_port: bool = False

    @property
    def db_path(self) -> str:
        return os.path.join(self.data_dir, "neurio.sqlite")

    @property
    def lock_path(self) -> str:
        return os.path.join(self.data_dir, "collector.lock")

    def validate(self) -> None:
        _validate_bind(
            self.bind_address,
            self.bind_port,
            self.allow_container_wildcard_bind,
            self.ephemeral_port,
        )
        if not self.data_dir or not os.path.isabs(self.data_dir):
            raise ConfigError("DATA_DIR must be an absolute path")
        if self.source_mode not in {"device", "homeassistant", "mqtt"}:
            raise ConfigError("SOURCE_MODE must be device, homeassistant, or mqtt")
        if self.source_mode == "device":
            validate_device_url(self.neurio_url)
        elif self.source_mode == "homeassistant":
            validate_ha_url(self.ha_url or "", self.ha_allow_rfc1918_http)
            if not self.ha_entity or self.ha_entity.startswith("/") or " " in self.ha_entity:
                raise ConfigError("HA_ENTITY must be an entity id such as sensor.neurio_raw")
            if not self.ha_source_id or not self.ha_source_id.startswith("ha-relay:"):
                raise ConfigError("HA_SOURCE_ID must start with ha-relay:")
            if self.ha_token is None or not self.ha_token.reveal():
                raise ConfigError("homeassistant mode requires HA_TOKEN_FILE or HA_TOKEN")
            if self.neurio_url:
                raise ConfigError(
                    "SOURCE_MODE=homeassistant does not poll the Neurio and has no fallback. "
                    "Unset NEURIO_URL."
                )
        else:
            validate_mqtt_settings(self)
        if self.source_mode != "mqtt" and self.poll_interval_seconds < COLLECTOR_MIN_POLL_INTERVAL_SECONDS:
            raise ConfigError(
                "NEURIO_POLL_INTERVAL_SECONDS must be at least "
                f"{COLLECTOR_MIN_POLL_INTERVAL_SECONDS}. Home Assistant already "
                "polls neurio_raw every 5 seconds and this collector must not match that rate."
            )
        if self.poll_interval_seconds > 3600:
            raise ConfigError("NEURIO_POLL_INTERVAL_SECONDS must be <= 3600")
        if self.source_mode == "mqtt":
            if not 0 < self.poll_timeout_seconds <= 120:
                raise ConfigError(
                    "NEURIO_POLL_TIMEOUT_SECONDS must be > 0 and <= 120 in mqtt mode. "
                    "It is the MQTT socket timeout, not a poll timeout."
                )
        elif not 0 < self.poll_timeout_seconds < self.poll_interval_seconds:
            raise ConfigError("NEURIO_POLL_TIMEOUT_SECONDS must be > 0 and shorter than the interval")
        if self.poll_max_backoff_seconds < self.poll_interval_seconds:
            raise ConfigError("NEURIO_POLL_MAX_BACKOFF_SECONDS must be >= the poll interval")
        if not 0 <= self.poll_jitter_ratio <= 1:
            raise ConfigError("NEURIO_POLL_JITTER_RATIO must be between 0 and 1")
        if self.poll_min_gap_seconds < 0:
            raise ConfigError("NEURIO_POLL_MIN_GAP_SECONDS must be >= 0")
        if (
            self.stale_after_seconds <= 0
            or self.hourly_stale_after_seconds <= 0
            or self.sample_stale_skew_seconds <= 0
        ):
            raise ConfigError("staleness thresholds must be positive")
        if self.max_allocatable_gap_seconds <= 0:
            raise ConfigError("NEURIO_MAX_ALLOCATABLE_GAP_SECONDS must be positive")
        if self.raw_retention_days < 0 or self.poll_log_retention_days < 1:
            raise ConfigError(
                "NEURIO_RAW_RETENTION_DAYS must be >= 0. Zero keeps raw JSON indefinitely. "
                "NEURIO_POLL_LOG_RETENTION_DAYS must be >= 1."
            )
        load_zone(self.timezone)
        if self.generation_register not in {"unset", "e_imp", "e_exp"}:
            raise ConfigError("NEURIO_GENERATION_REGISTER must be unset, e_imp, or e_exp")
        if self.consumption_source not in {
            "consumption_channel",
            "none",
            "derive_net_plus_generation",
        }:
            raise ConfigError(
                "NEURIO_CONSUMPTION_SOURCE must be consumption_channel, none, "
                "or derive_net_plus_generation"
            )
        if self.consumption_source == "derive_net_plus_generation" and not self.topology_verified:
            raise ConfigError(
                "derive_net_plus_generation is rejected until TOPOLOGY_VERIFIED=true. "
                "The default consumption source is the dedicated CONSUMPTION channel."
            )
        if self.max_response_bytes < 1024:
            raise ConfigError("NEURIO_MAX_RESPONSE_BYTES is too small")

    @classmethod
    def from_env(cls, env: dict[str, str] | None = None) -> "Config":
        values = os.environ if env is None else env
        source_mode = _required(values, "SOURCE_MODE")
        mqtt_broker_host: str | None = None
        mqtt_broker_port = 1883
        mqtt_tls = False
        mqtt_username: str | None = None
        mqtt_password: Secret | None = None
        mqtt_client_id = "neurio-recorder"
        mqtt_raw_topic = "neurio/raw"
        mqtt_hourly_topic = "neurio/hourly/energy"
        mqtt_ack_topic = "neurio/hourly/ack"
        mqtt_keepalive_seconds = 60
        if source_mode == "homeassistant":
            if (values.get("NEURIO_URL") or "").strip():
                raise ConfigError(
                    "SOURCE_MODE=homeassistant does not poll the Neurio and has no fallback. "
                    "Unset NEURIO_URL."
                )
            neurio_url = ""
            ha_url = _required(values, "HA_URL")
            ha_entity = _required(values, "HA_ENTITY")
            ha_source_id = _optional_str(values, "HA_SOURCE_ID", f"ha-relay:{ha_entity}")
            ha_token = _load_ha_token(values)
            ha_allow_rfc1918_http = _optional_blank(values, "HA_ALLOW_RFC1918_HTTP")
        elif source_mode == "device":
            neurio_url = _required(values, "NEURIO_URL")
            ha_url = None
            ha_entity = None
            ha_source_id = None
            ha_token = None
            ha_allow_rfc1918_http = None
            if _optional_blank(values, "HA_TOKEN") or _optional_blank(values, "HA_TOKEN_FILE"):
                raise ConfigError("HA_TOKEN is only read when SOURCE_MODE=homeassistant")
        elif source_mode == "mqtt":
            if (values.get("NEURIO_URL") or "").strip():
                raise ConfigError(
                    "SOURCE_MODE=mqtt does not poll the Neurio directly. "
                    "Unset NEURIO_URL."
                )
            neurio_url = ""
            ha_url = None
            ha_entity = None
            ha_source_id = None
            ha_token = None
            ha_allow_rfc1918_http = None
            if _optional_blank(values, "HA_TOKEN") or _optional_blank(values, "HA_TOKEN_FILE"):
                raise ConfigError("HA_TOKEN is only read when SOURCE_MODE=homeassistant")
            mqtt_broker_host = _required(values, "MQTT_BROKER_HOST")
            mqtt_broker_port = _optional_int(values, "MQTT_BROKER_PORT", 1883)
            mqtt_tls = _optional_bool(values, "MQTT_TLS", False)
            mqtt_username = _optional_blank(values, "MQTT_USERNAME")
            mqtt_password = _read_secret(values, "MQTT_PASSWORD_FILE", "MQTT_PASSWORD")
            mqtt_client_id = _optional_str(values, "MQTT_CLIENT_ID", "neurio-recorder")
            if "MQTT_RAW_TOPIC" in values:
                # Presence is meaningful: an explicitly empty MQTT_RAW_TOPIC
                # disables the raw sample subscription. Current bridge
                # firmware publishes no raw feed (live values go to Home
                # Assistant over the ESPHome API), so MQTT mode then records
                # hourly records only.
                mqtt_raw_topic = (values["MQTT_RAW_TOPIC"] or "").strip()
            else:
                mqtt_raw_topic = "neurio/raw"
            mqtt_hourly_topic = _optional_str(
                values, "MQTT_HOURLY_TOPIC", "neurio/hourly/energy"
            )
            mqtt_ack_topic = _optional_str(values, "MQTT_ACK_TOPIC", "neurio/hourly/ack")
            mqtt_keepalive_seconds = _optional_int(values, "MQTT_KEEPALIVE_SECONDS", 60)
        else:
            raise ConfigError("SOURCE_MODE must be device, homeassistant, or mqtt")
        cfg = cls(
            bind_address=_required(values, "BIND_ADDRESS"),
            bind_port=_required_int(values, "BIND_PORT"),
            data_dir=_required(values, "DATA_DIR"),
            neurio_url=neurio_url,
            source_mode=source_mode,
            ha_url=ha_url,
            ha_entity=ha_entity,
            ha_source_id=ha_source_id,
            ha_token=ha_token,
            ha_allow_rfc1918_http=ha_allow_rfc1918_http,
            mqtt_broker_host=mqtt_broker_host,
            mqtt_broker_port=mqtt_broker_port,
            mqtt_tls=mqtt_tls,
            mqtt_username=mqtt_username,
            mqtt_password=mqtt_password,
            mqtt_client_id=mqtt_client_id,
            mqtt_raw_topic=mqtt_raw_topic,
            mqtt_hourly_topic=mqtt_hourly_topic,
            mqtt_ack_topic=mqtt_ack_topic,
            mqtt_keepalive_seconds=mqtt_keepalive_seconds,
            poll_interval_seconds=_optional_float(values, "NEURIO_POLL_INTERVAL_SECONDS", 15.0),
            poll_timeout_seconds=_optional_float(values, "NEURIO_POLL_TIMEOUT_SECONDS", 3.0),
            poll_max_backoff_seconds=_optional_float(values, "NEURIO_POLL_MAX_BACKOFF_SECONDS", 300.0),
            poll_jitter_ratio=_optional_float(values, "NEURIO_POLL_JITTER_RATIO", 0.2),
            poll_min_gap_seconds=_optional_float(values, "NEURIO_POLL_MIN_GAP_SECONDS", 1.0),
            stale_after_seconds=_optional_float(values, "NEURIO_STALE_AFTER_SECONDS", 60.0),
            hourly_stale_after_seconds=_optional_float(
                values, "NEURIO_HOURLY_STALE_AFTER_SECONDS", 3900.0
            ),
            sample_stale_skew_seconds=_optional_float(
                values, "NEURIO_SAMPLE_STALE_SKEW_SECONDS", 180.0
            ),
            device_ahead_skew_seconds=_optional_float(
                values, "NEURIO_DEVICE_AHEAD_SKEW_SECONDS", 30.0
            ),
            max_allocatable_gap_seconds=_optional_float(
                values, "NEURIO_MAX_ALLOCATABLE_GAP_SECONDS", 45.0
            ),
            raw_retention_days=_optional_int(values, "NEURIO_RAW_RETENTION_DAYS", 14),
            poll_log_retention_days=_optional_int(values, "NEURIO_POLL_LOG_RETENTION_DAYS", 30),
            timezone=_optional_str(values, "NEURIO_TIMEZONE", "America/Chicago"),
            household_channel_reads=_optional_bool(values, "NEURIO_HOUSEHOLD_CHANNEL_READS", True),
            topology_verified=_optional_bool(values, "NEURIO_TOPOLOGY_VERIFIED", False),
            generation_register=_optional_str(values, "NEURIO_GENERATION_REGISTER", "unset"),
            consumption_source=_optional_str(
                values, "NEURIO_CONSUMPTION_SOURCE", "consumption_channel"
            ),
            role_net_identity=_optional_blank(values, "NEURIO_ROLE_NET_IDENTITY"),
            role_generation_identity=_optional_blank(values, "NEURIO_ROLE_GENERATION_IDENTITY"),
            role_consumption_identity=_optional_blank(values, "NEURIO_ROLE_CONSUMPTION_IDENTITY"),
            redact_identity=_optional_bool(values, "NEURIO_REDACT_IDENTITY", True),
            max_response_bytes=_optional_int(values, "NEURIO_MAX_RESPONSE_BYTES", 1_000_000),
            allow_container_wildcard_bind=_optional_blank(
                values, "ALLOW_CONTAINER_WILDCARD_BIND"
            ),
        )
        cfg.validate()
        return cfg


def _literal_rfc1918(host: str) -> bool:
    try:
        address = ipaddress.ip_address(host)
    except ValueError:
        return False
    if not isinstance(address, ipaddress.IPv4Address):
        return False
    return any(address in network for network in _RFC1918)


def validate_ha_url(url: str, allow_rfc1918_http: str | None = None) -> None:
    parsed = urlparse(url)
    if parsed.username or parsed.password or parsed.query or parsed.fragment:
        raise ConfigError("HA_URL must not include userinfo, a query, or a fragment")
    if parsed.path not in {"", "/"}:
        raise ConfigError("HA_URL is the Home Assistant origin only. The entity path is added by the client.")
    host = parsed.hostname
    if not host:
        raise ConfigError("HA_URL is missing a host")
    if parsed.scheme == "https":
        return
    if parsed.scheme != "http":
        raise ConfigError("HA_URL must be https, or http to a literal loopback or RFC1918 address")
    if host in {"127.0.0.1", "::1"}:
        return
    if _literal_rfc1918(host) and allow_rfc1918_http == RFC1918_HTTP_OPT_IN:
        return
    if _literal_rfc1918(host):
        raise ConfigError(
            "HTTP to a literal RFC1918 address carries the Home Assistant bearer unencrypted. "
            "Set HA_ALLOW_RFC1918_HTTP=accept-unencrypted-bearer to opt in. "
            "Names, public addresses, link-local, and metadata addresses are not accepted."
        )
    raise ConfigError(
        "HTTP HA_URL is limited to literal 127.0.0.1, ::1, or an RFC1918 address with "
        "HA_ALLOW_RFC1918_HTTP=accept-unencrypted-bearer. DNS names and other addresses are rejected. "
        "HTTPS certificate checks are unchanged."
    )


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


def _load_ha_token(env: dict[str, str]) -> Secret:
    secret = _read_secret(env, "HA_TOKEN_FILE", "HA_TOKEN")
    if secret is None:
        raise ConfigError(
            "homeassistant mode requires HA_TOKEN_FILE or HA_TOKEN. "
            "The value is not requested here."
        )
    return secret


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
    if cfg.neurio_url:
        raise ConfigError(
            "SOURCE_MODE=mqtt does not poll the Neurio directly. Unset NEURIO_URL."
        )
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
    # The raw topic is the one exception to "must not be empty": empty means
    # the raw sample subscription is disabled (hourly-only MQTT mode).
    if cfg.mqtt_raw_topic and ("+" in cfg.mqtt_raw_topic or "#" in cfg.mqtt_raw_topic):
        raise ConfigError(
            "MQTT_RAW_TOPIC must be an exact topic. Wildcards (+, #) are not accepted."
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
            "NEURIO_POLL_INTERVAL_SECONDS must be positive in mqtt mode. "
            "It is the base of the reconnect backoff."
        )


def validate_device_url(url: str) -> None:
    parsed = urlparse(url)
    if parsed.scheme != "http":
        raise ConfigError(
            "NEURIO_URL must be an http:// URL on the physical LAN device. "
            "https://api.neur.io and other cloud endpoints are rejected."
        )
    if parsed.username or parsed.password:
        raise ConfigError("NEURIO_URL must not contain credentials")
    host = (parsed.hostname or "").rstrip(".").lower()
    if not host:
        raise ConfigError("NEURIO_URL is missing a host")
    if host in CLOUD_HOSTS or host.endswith(CLOUD_HOST_SUFFIXES):
        raise ConfigError(
            "NEURIO_URL points at a Neurio cloud host. Use the physical LAN current-sample URL."
        )
    if parsed.path in {"", "/"}:
        raise ConfigError("NEURIO_URL must include the device path, for example /current-sample")


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
