from __future__ import annotations

import json
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

DEFAULT_BASE_URL = "https://bgpp.misa.st/api/stations/bg/search"
DEFAULT_HEADERS = {
    "Accept": "application/json",
    "User-Agent": "roundabout/0.1",
}


@dataclass(frozen=True)
class FetchResult:
    stop_code: str
    observed_at: datetime
    payload: dict[str, Any] | None
    error: str | None
    status: int | None
    duration_ms: int
    attempts: int


def fetch_stop(
    stop_code: str,
    *,
    base_url: str = DEFAULT_BASE_URL,
    timeout_s: float = 10.0,
    retries: int = 2,
) -> FetchResult:
    url = f"{base_url}?id={stop_code}"
    last_error: str | None = None
    status: int | None = None
    overall_start = time.monotonic()
    for attempt in range(1, retries + 2):
        try:
            request = Request(url, headers=DEFAULT_HEADERS)
            with urlopen(request, timeout=timeout_s) as response:
                status = getattr(response, "status", None)
                body = response.read()
            payload = json.loads(body.decode("utf-8"))
            observed_at = datetime.now(timezone.utc)
            duration_ms = int((time.monotonic() - overall_start) * 1000)
            return FetchResult(
                stop_code=stop_code,
                observed_at=observed_at,
                payload=payload,
                error=None,
                status=status,
                duration_ms=duration_ms,
                attempts=attempt,
            )
        except HTTPError as exc:
            status = exc.code
            last_error = f"http_error:{exc.code}"
        except URLError as exc:
            last_error = f"url_error:{exc.reason}"
        except (OSError, ValueError) as exc:
            last_error = f"error:{exc}"
        if attempt <= retries:
            time.sleep(0.25 * attempt)
    observed_at = datetime.now(timezone.utc)
    duration_ms = int((time.monotonic() - overall_start) * 1000)
    return FetchResult(
        stop_code=stop_code,
        observed_at=observed_at,
        payload=None,
        error=last_error or "unknown_error",
        status=status,
        duration_ms=duration_ms,
        attempts=retries + 1,
    )
