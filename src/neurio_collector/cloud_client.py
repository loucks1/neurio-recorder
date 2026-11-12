"""Neurio cloud API client: OAuth client credentials, /v1 endpoints.

Mirrors the semantics of rate-plan-tool's cloud client: the access token is
cached until shortly before it expires, a 401 forces one token refresh and
one retry, and error bodies are truncated before they reach an exception
message. The transport is injectable so tests run against a fake cloud with
no network. This client is outbound-only and is used solely by cloud
ingest; the device-polling URL rules (LAN-only, cloud hosts rejected) are
unaffected.
"""

from __future__ import annotations

import json
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime

from .config import Config
from .timeutil import format_utc

USER_AGENT = "neurio-lan-collector/1.0"


class CloudError(Exception):
    def __init__(self, message: str, status: int | None = None):
        super().__init__(message)
        self.status = status


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        raise urllib.error.HTTPError(req.full_url, code, "redirect_blocked", headers, fp)


def _urllib_transport(method, url, headers, body, timeout):
    """(method, url, headers, body, timeout) -> (status, response body).

    HTTP statuses are returned, not raised. Network failures raise
    CloudError so the sync loop can record them like any other failure.
    """
    request = urllib.request.Request(url, data=body, method=method, headers=headers)
    opener = urllib.request.build_opener(_NoRedirect)
    try:
        with opener.open(request, timeout=timeout) as response:
            return getattr(response, "status", 200), response.read()
    except urllib.error.HTTPError as exc:
        try:
            detail = exc.read()
        except Exception:
            detail = b""
        return exc.code, detail
    except TimeoutError as exc:
        raise CloudError("timeout") from exc
    except urllib.error.URLError as exc:
        reason = exc.reason
        if isinstance(reason, TimeoutError) or "timed out" in str(reason).lower():
            raise CloudError("timeout") from exc
        raise CloudError("connection_error") from exc
    except OSError as exc:
        raise CloudError("connection_error") from exc


class NeurioCloudClient:
    def __init__(self, cfg: Config, transport=None, now=None):
        self.cfg = cfg
        self._transport = transport or _urllib_transport
        self._now = now or time.time
        self._token: str | None = None
        self._token_expires_at = 0.0

    def _request(self, method, url, headers, body):
        status, raw = self._transport(
            method, url, headers, body, self.cfg.cloud_timeout_seconds
        )
        if len(raw) > self.cfg.max_response_bytes:
            raise CloudError("response_too_large", status)
        return status, raw

    def _access_token(self, force: bool = False) -> str:
        if not force and self._token and self._now() < self._token_expires_at - 60:
            return self._token
        form = urllib.parse.urlencode(
            {
                "grant_type": self.cfg.cloud_grant_type,
                "client_id": self.cfg.cloud_client_id or "",
                "client_secret": self.cfg.cloud_client_secret.reveal()
                if self.cfg.cloud_client_secret
                else "",
            }
        ).encode("utf-8")
        status, raw = self._request(
            "POST",
            f"{self.cfg.cloud_api_base}/oauth2/token",
            {
                "Content-Type": "application/x-www-form-urlencoded",
                "Accept": "application/json",
                "User-Agent": USER_AGENT,
            },
            form,
        )
        if status != 200:
            raise CloudError(f"token request failed ({status})", status)
        try:
            payload = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise CloudError("token response is not json", status) from exc
        token = payload.get("access_token") if isinstance(payload, dict) else None
        if not isinstance(token, str) or not token:
            raise CloudError("token response did not include access_token", status)
        expires_in = payload.get("expires_in", 3600)
        if isinstance(expires_in, bool) or not isinstance(expires_in, (int, float)):
            expires_in = 3600
        self._token = token
        self._token_expires_at = self._now() + float(expires_in)
        return token

    def _get_json(self, url: str, retry: bool = True):
        status, raw = self._request(
            "GET",
            url,
            {
                "Authorization": f"Bearer {self._access_token()}",
                "Accept": "application/json",
                "User-Agent": USER_AGENT,
            },
            None,
        )
        if status == 401 and retry:
            self._access_token(force=True)
            return self._get_json(url, retry=False)
        if status != 200:
            text = raw.decode("utf-8", errors="replace")[:300]
            raise CloudError(f"request failed ({status}): {text}", status)
        try:
            return json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise CloudError("response is not json", status) from exc

    def get_sensor_id(self) -> str:
        body = self._get_json(f"{self.cfg.cloud_api_base}/users/current")
        sensor = None
        if isinstance(body, dict):
            locations = body.get("locations")
            if isinstance(locations, list) and locations:
                sensors = locations[0].get("sensors") if isinstance(locations[0], dict) else None
                if isinstance(sensors, list) and sensors and isinstance(sensors[0], dict):
                    sensor = sensors[0]
        if sensor is not None:
            for key in ("sensorId", "SensorID", "id"):
                value = sensor.get(key)
                if isinstance(value, str) and value:
                    return value
        raise CloudError("cloud user has no sensor id")

    def get_hourly_stats(
        self, sensor_id: str, start: datetime, end: datetime
    ) -> list:
        """Per-hour interval watt-seconds over [start, end) from /samples/stats.

        Window bounds are inclusive on the API, so adjacent windows share a
        boundary hour; callers key rows by the returned start/end and upsert,
        which makes the overlap harmless.
        """
        query = urllib.parse.urlencode(
            {
                "sensorId": sensor_id,
                "start": format_utc(start),
                "end": format_utc(end),
                "granularity": "hours",
                "frequency": "1",
            }
        )
        body = self._get_json(f"{self.cfg.cloud_api_base}/samples/stats?{query}")
        if not isinstance(body, list):
            raise CloudError("samples/stats response was not a list")
        return body
