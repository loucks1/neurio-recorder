"""MQTT client for SOURCE_MODE=mqtt: a thin adapter over paho-mqtt.

paho-mqtt is this project's one runtime dependency. It owns the wire
protocol — packet framing, keepalive, QoS 1 PUBACKs, and reconnect-safe
socket handling — and runs its network loop on its own thread. This adapter
keeps the small blocking interface the poller was written against:
``connect`` / ``subscribe`` / ``publish`` / ``poll(timeout)`` returning the
next message or None / ``close``, with connection failures raised as
MqttError so the poller's backoff owns reconnect policy
(``reconnect_on_failure=False``; paho never reconnects on its own here).

paho is imported lazily, at client construction, so tooling that imports
this module still works where the package is not installed.

Inbound payloads larger than ``max_message_bytes`` are dropped before they
reach the ingest path and counted on ``dropped_oversize``.
"""

from __future__ import annotations

import importlib
import queue
import threading
from dataclasses import dataclass


class MqttError(Exception):
    pass


@dataclass
class MqttMessage:
    topic: str
    payload: bytes
    qos: int = 0
    retain: bool = False
    packet_id: int | None = None


def _load_paho():
    try:
        return importlib.import_module("paho.mqtt.client")
    except ImportError as exc:
        raise MqttError(
            "paho-mqtt is not installed. It is the one runtime dependency; "
            "install requirements.txt to use SOURCE_MODE=mqtt."
        ) from exc


def _reason_failed(reason_code) -> bool:
    if getattr(reason_code, "is_failure", None):
        return True
    try:
        return int(reason_code) != 0
    except (TypeError, ValueError):
        return False


class MqttClient:
    """One connection to a broker, via paho. Used from a single thread;

    paho callbacks run on paho's network thread and only touch the
    thread-safe queue and events.
    """

    def __init__(
        self,
        host: str,
        port: int = 1883,
        client_id: str = "neurio-recorder",
        keepalive_seconds: int = 60,
        username: str | None = None,
        password: str | None = None,
        tls: bool = False,
        timeout_seconds: float = 10.0,
        max_message_bytes: int | None = None,
        paho_module=None,
    ):
        self.host = host
        self.port = port
        self.client_id = client_id
        self.keepalive_seconds = keepalive_seconds
        self.username = username
        self.password = password
        self.tls = tls
        self.timeout_seconds = timeout_seconds
        self.max_message_bytes = max_message_bytes
        self.dropped_oversize = 0
        self._paho = paho_module if paho_module is not None else _load_paho()
        self._queue: queue.Queue[MqttMessage] = queue.Queue()
        self._connect_event = threading.Event()
        self._connect_error: str | None = None
        self._subscribe_event = threading.Event()
        self._subscribe_results: dict[int, list] = {}
        self._disconnect_reason: str | None = None
        self._closing = False
        self._client = None

    # -- lifecycle ------------------------------------------------------

    def connect(self) -> None:
        if self._client is not None:
            raise MqttError("already connected")
        paho = self._paho
        client = paho.Client(
            callback_api_version=paho.CallbackAPIVersion.VERSION2,
            client_id=self.client_id,
            clean_session=False,
            protocol=paho.MQTTv311,
            reconnect_on_failure=False,
        )
        client.on_connect = self._on_connect
        client.on_disconnect = self._on_disconnect
        client.on_message = self._on_message
        client.on_subscribe = self._on_subscribe
        if self.username is not None:
            client.username_pw_set(self.username, self.password)
        if self.tls:
            client.tls_set()
        self._client = client
        try:
            result = client.connect(self.host, self.port, self.keepalive_seconds)
        except OSError as exc:
            self._client = None
            raise MqttError(f"could not connect to the MQTT broker: {exc}") from exc
        if result != paho.MQTT_ERR_SUCCESS:
            self._client = None
            raise MqttError(f"could not connect to the MQTT broker, paho result {result}")
        client.loop_start()
        if not self._connect_event.wait(max(self.timeout_seconds, 1.0)):
            self.close()
            raise MqttError("timed out waiting for CONNACK")
        if self._connect_error is not None:
            error = self._connect_error
            self.close()
            raise MqttError(error)

    def subscribe(self, topics: list[tuple[str, int]]) -> None:
        client = self._require_client()
        if not topics:
            raise MqttError("SUBSCRIBE requires at least one topic")
        result, mid = client.subscribe(list(topics))
        if result != self._paho.MQTT_ERR_SUCCESS:
            raise MqttError(f"SUBSCRIBE failed, paho result {result}")
        deadline_event = self._subscribe_event
        deadline_event.clear()
        # The callback may already have recorded this mid before we waited.
        if mid not in self._subscribe_results:
            if not deadline_event.wait(max(self.timeout_seconds, 1.0)):
                raise MqttError("timed out waiting for SUBACK")
        codes = self._subscribe_results.pop(mid, None)
        if codes is None:
            raise MqttError("timed out waiting for SUBACK")
        if any(_reason_failed(code) for code in codes):
            raise MqttError("the broker refused a subscription")

    def publish(self, topic: str, payload: bytes) -> None:
        client = self._require_client()
        if self._disconnect_reason is not None:
            raise MqttError(f"the MQTT connection is closed: {self._disconnect_reason}")
        info = client.publish(topic, payload=bytes(payload), qos=0)
        if info.rc != self._paho.MQTT_ERR_SUCCESS:
            raise MqttError(f"publish to {topic} failed, paho result {info.rc}")

    def poll(self, timeout_seconds: float) -> MqttMessage | None:
        """Return the next inbound message, or None when the timeout elapses.

        Raises MqttError once the connection has dropped, so the caller can
        run its reconnect policy.
        """
        self._require_client()
        if self._disconnect_reason is not None:
            raise MqttError(f"the MQTT connection was lost: {self._disconnect_reason}")
        try:
            return self._queue.get(timeout=max(0.0, timeout_seconds))
        except queue.Empty:
            if self._disconnect_reason is not None:
                raise MqttError(
                    f"the MQTT connection was lost: {self._disconnect_reason}"
                ) from None
            return None

    def close(self) -> None:
        client, self._client = self._client, None
        if client is None:
            return
        self._closing = True
        try:
            client.disconnect()
        except Exception:
            pass
        try:
            client.loop_stop()
        except Exception:
            pass

    # -- paho callbacks (network thread) ---------------------------------

    def _on_connect(self, client, userdata, flags, reason_code, properties):
        if _reason_failed(reason_code):
            self._connect_error = (
                f"the broker refused the connection, reason: {reason_code}"
            )
        self._connect_event.set()

    def _on_disconnect(self, client, userdata, disconnect_flags, reason_code, properties):
        if not self._closing:
            self._disconnect_reason = f"disconnected, reason: {reason_code}"

    def _on_subscribe(self, client, userdata, mid, reason_code_list, properties):
        self._subscribe_results[mid] = list(reason_code_list)
        self._subscribe_event.set()

    def _on_message(self, client, userdata, message):
        payload = bytes(message.payload)
        if self.max_message_bytes is not None and len(payload) > self.max_message_bytes:
            self.dropped_oversize += 1
            return
        self._queue.put(
            MqttMessage(
                topic=message.topic,
                payload=payload,
                qos=message.qos,
                retain=bool(message.retain),
            )
        )

    def _require_client(self):
        if self._client is None:
            raise MqttError("not connected")
        return self._client
