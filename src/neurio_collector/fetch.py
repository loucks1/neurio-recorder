"""One HTTP GET of the physical current-sample URL.

Redirects are not followed. Timeouts stay on this attempt. A 401 is recorded
and not retried with invented credentials.
"""

from __future__ import annotations

import urllib.error
import urllib.request
from dataclasses import dataclass

from .config import ConfigError, validate_device_url


@dataclass
class FetchResult:
    outcome: str
    status: int | None
    body: bytes
    error: str | None


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        raise urllib.error.HTTPError(req.full_url, code, "redirect_blocked", headers, fp)


def fetch_current_sample(url: str, timeout: float, max_bytes: int) -> FetchResult:
    try:
        validate_device_url(url)
    except ConfigError as exc:
        return FetchResult("rejected_url", None, b"", str(exc))
    request = urllib.request.Request(
        url,
        method="GET",
        headers={
            "Accept": "application/json",
            "User-Agent": "neurio-lan-collector/1.0",
        },
    )
    opener = urllib.request.build_opener(_NoRedirect)
    try:
        with opener.open(request, timeout=timeout) as response:
            status = getattr(response, "status", 200)
            body = response.read(max_bytes + 1)
    except urllib.error.HTTPError as exc:
        detail = "http_401_no_credential_retry" if exc.code == 401 else f"http_{exc.code}"
        return FetchResult("http_error", exc.code, b"", detail)
    except TimeoutError:
        return FetchResult("timeout", None, b"", "timeout")
    except urllib.error.URLError as exc:
        reason = exc.reason
        if isinstance(reason, TimeoutError) or "timed out" in str(reason).lower():
            return FetchResult("timeout", None, b"", "timeout")
        return FetchResult("connection_error", None, b"", "connection_error")
    except OSError:
        return FetchResult("connection_error", None, b"", "connection_error")
    if len(body) > max_bytes:
        return FetchResult("too_large", status, b"", "response_too_large")
    if status != 200:
        return FetchResult("http_error", status, b"", f"http_{status}")
    return FetchResult("http_ok", status, body, None)
