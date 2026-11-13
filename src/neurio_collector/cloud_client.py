"""Neurio cloud API client: OAuth client credentials, /v1 endpoints.

Mirrors the semantics of rate-plan-tool's cloud client: the access token is
cached until shortly before it expires, a 401 forces one token refresh and
one retry, and error bodies are truncated before they reach an exception
message. The transport is injectable so tests run against a fake cloud with
no network. This client is outbound-only and is used solely by cloud
ingest, against the NEURIO_CLOUD_API_BASE the operator configured.
"""

from __future__ import annotations

import json
import re
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime

from .config import Config
from .timeutil import format_utc

USER_AGENT = "neurio-lan-collector/1.0"

# Neurio's 429 body says: "The user exceeds 144 rate limit. The remaining
# time to reset the limit is N milliseconds." The code is rate_limit_exceeding.
# RateLimit-Reset is the same remaining time, in milliseconds.
_RESET_MS_RE = re.compile(
    r"remaining time to reset the limit is\s+(\d+)\s+milliseconds",
    re.IGNORECASE,
)
_SAFE_CODE_RE = re.compile(r"^[A-Za-z0-9_.-]{1,64}$")


class CloudError(Exception):
    def __init__(
        self,
        message: str,
        status: int | None = None,
        retry_after_ms: int | None = None,
    ):
        super().__init__(message)
        self.status = status
        self.retry_after_ms = retry_after_ms


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        raise urllib.error.HTTPError(req.full_url, code, "redirect_blocked", headers, fp)


def _header_map(headers) -> dict:
    if not headers:
        return {}
    mapped = {}
    try:
        pairs = headers.items()
    except AttributeError:
        pairs = headers
    for key, value in pairs:
        if key is None:
            continue
        mapped[str(key).lower()] = "" if value is None else str(value)
    return mapped


def _urllib_transport(method, url, headers, body, timeout):
    """(method, url, headers, body, timeout) -> (status, body, header map).

    HTTP statuses are returned, not raised. Network failures raise
    CloudError so the sync loop can record them like any other failure.
    Header names are lower-case. Response bodies are not logged.
    """
    request = urllib.request.Request(url, data=body, method=method, headers=headers)
    opener = urllib.request.build_opener(_NoRedirect)
    try:
        with opener.open(request, timeout=timeout) as response:
            return (
                getattr(response, "status", 200),
                response.read(),
                _header_map(response.headers),
            )
    except urllib.error.HTTPError as exc:
        try:
            detail = exc.read()
        except Exception:
            detail = b""
        return exc.code, detail, _header_map(exc.headers)
    except TimeoutError as exc:
        raise CloudError("timeout") from exc
    except urllib.error.URLError as exc:
        reason = exc.reason
        if isinstance(reason, TimeoutError) or "timed out" in str(reason).lower():
            raise CloudError("timeout") from exc
        raise CloudError("connection_error") from exc
    except OSError as exc:
        raise CloudError("connection_error") from exc


def _unpack_transport(result):
    """Accept (status, body) and (status, body, headers).

    Tests and older fakes return a pair. The urllib transport returns
    the header map as well so a 429 can carry RateLimit-Reset.
    """
    if isinstance(result, tuple) and len(result) == 3:
        status, raw, headers = result
        return status, raw if raw is not None else b"", _header_map(headers)
    status, raw = result
    return status, raw if raw is not None else b"", {}


def _json_code(raw: bytes) -> str | None:
    """A short error code from a JSON body, or None.

    Only a single token is returned. Message text, nested objects, and
    anything that could be a credential or a raw body are dropped.
    """
    if not raw:
        return None
    try:
        payload = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        return None
    if not isinstance(payload, dict):
        return None
    for key in ("code", "error"):
        value = payload.get(key)
        if isinstance(value, str) and _SAFE_CODE_RE.fullmatch(value):
            return value
    return None


def _retry_after_ms(raw: bytes, headers: dict) -> int | None:
    """Milliseconds until the rate-limit window resets, if the response says.

    The body sentence is preferred. RateLimit-Reset is the same duration
    in milliseconds (Neurio's documented header). An unparseable value is
    absent, and the caller then waits out a full window.
    """
    if raw:
        match = _RESET_MS_RE.search(raw.decode("utf-8", errors="replace"))
        if match:
            return int(match.group(1))
    header = headers.get("ratelimit-reset") if headers else None
    if header is not None and str(header).strip().isdigit():
        return int(str(header).strip())
    return None


def _http_error(status: int, raw: bytes, headers: dict) -> CloudError:
    """A CloudError whose message is safe to store and return.

    The response body is not copied into the message. A 429 keeps the
    parsed reset time on the exception so the syncer can sleep until
    the window reopens.
    """
    if status == 429:
        retry_after_ms = _retry_after_ms(raw, headers)
        code = _json_code(raw) or "rate_limit_exceeding"
        if not _SAFE_CODE_RE.fullmatch(code):
            code = "rate_limit_exceeding"
        if retry_after_ms is None:
            message = code
        else:
            message = f"{code}; retry after {retry_after_ms} ms"
        return CloudError(message, status, retry_after_ms)
    code = _json_code(raw)
    if code:
        return CloudError(f"request failed ({status}): {code}", status)
    return CloudError(f"request failed ({status})", status)


class NeurioCloudClient:
    def __init__(self, cfg: Config, transport=None, now=None):
        self.cfg = cfg
        self._transport = transport or _urllib_transport
        self._now = now or time.time
        self._token: str | None = None
        self._token_expires_at = 0.0

    def _request(self, method, url, headers, body):
        status, raw, response_headers = _unpack_transport(
            self._transport(method, url, headers, body, self.cfg.cloud_timeout_seconds)
        )
        if len(raw) > self.cfg.max_response_bytes:
            raise CloudError("response_too_large", status)
        return status, raw, response_headers

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
        status, raw, response_headers = self._request(
            "POST",
            f"{self.cfg.cloud_api_base}/oauth2/token",
            {
                "Content-Type": "application/x-www-form-urlencoded",
                "Accept": "application/json",
                "User-Agent": USER_AGENT,
            },
            form,
        )
        if status == 429:
            raise _http_error(status, raw, response_headers)
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
        status, raw, response_headers = self._request(
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
            raise _http_error(status, raw, response_headers)
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
