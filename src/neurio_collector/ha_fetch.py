"""Read one Home Assistant entity state. GET only. TLS verification stays on.

The long-lived token is sent on the Authorization header and is not written
into errors, URLs, or the returned body.
"""

from __future__ import annotations

import ssl
import urllib.error
import urllib.request
from urllib.parse import quote

from .config import ConfigError, Secret, validate_ha_url
from .fetch import FetchResult, _NoRedirect


def tls_context() -> ssl.SSLContext:
    context = ssl.create_default_context()
    if context.verify_mode != ssl.CERT_REQUIRED or not context.check_hostname:
        raise RuntimeError("refusing a TLS context that does not verify certificates")
    return context


def ha_opener() -> urllib.request.OpenerDirector:
    """No environment proxy and no redirect. HTTPS still verifies certificates."""
    return urllib.request.build_opener(
        urllib.request.ProxyHandler({}),
        urllib.request.HTTPSHandler(context=tls_context()),
        _NoRedirect,
    )


def fetch_ha_state(
    base_url: str,
    entity_id: str,
    token: Secret,
    timeout: float,
    max_bytes: int,
    *,
    allow_rfc1918_http: str | None = None,
) -> FetchResult:
    try:
        validate_ha_url(base_url, allow_rfc1918_http)
    except ConfigError as exc:
        return FetchResult("rejected_url", None, b"", str(exc))
    secret = token.reveal()
    url = base_url.rstrip("/") + "/api/states/" + quote(entity_id, safe="")
    request = urllib.request.Request(
        url,
        method="GET",
        headers={
            "Accept": "application/json",
            "Authorization": "Bearer " + secret,
            "User-Agent": "neurio-lan-collector/1.0",
        },
    )
    opener = ha_opener()
    try:
        with opener.open(request, timeout=timeout) as response:
            status = getattr(response, "status", 200)
            body = response.read(max_bytes + 1)
    except urllib.error.HTTPError as exc:
        if exc.code in {401, 403}:
            return FetchResult("auth_error", exc.code, b"", "ha_auth_rejected")
        return FetchResult("http_error", exc.code, b"", f"http_{exc.code}")
    except TimeoutError:
        return FetchResult("timeout", None, b"", "timeout")
    except urllib.error.URLError as exc:
        reason = exc.reason
        if isinstance(reason, TimeoutError) or "timed out" in str(reason).lower():
            return FetchResult("timeout", None, b"", "timeout")
        return FetchResult("connection_error", None, b"", "connection_error")
    except OSError:
        return FetchResult("connection_error", None, b"", "connection_error")
    if secret and secret.encode("utf-8") in body:
        body = body.replace(secret.encode("utf-8"), b"[redacted]")
    if len(body) > max_bytes:
        return FetchResult("too_large", status, b"", "response_too_large")
    if status != 200:
        return FetchResult("http_error", status, b"", f"http_{status}")
    return FetchResult("http_ok", status, body, None)


def scrub(text: str, token: Secret | None) -> str:
    if not text or token is None:
        return text
    secret = token.reveal()
    if secret and secret in text:
        return text.replace(secret, "[redacted]")
    return text
