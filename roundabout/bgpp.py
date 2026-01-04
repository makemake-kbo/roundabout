"""BG++ API client for fetching real-time vehicle arrival predictions."""

from __future__ import annotations

import json
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from roundabout.constants import (
    DEFAULT_BASE_URL,
    DEFAULT_HTTP_RETRIES,
    DEFAULT_HTTP_TIMEOUT_S,
    DEFAULT_USER_AGENT,
    RETRY_BASE_DELAY_S,
)

DEFAULT_HEADERS = {
    "Accept": "application/json",
    "User-Agent": DEFAULT_USER_AGENT,
}


@dataclass(frozen=True)
class FetchResult:
    """
    Result of fetching stop predictions from the BG++ API.

    Attributes:
        stop_code: The stop code that was queried.
        observed_at: Timestamp when the request completed (UTC).
        payload: Parsed JSON response if successful, None on error.
        error: Error description string if request failed, None on success.
        status: HTTP status code if available, None otherwise.
        duration_ms: Total request duration including retries in milliseconds.
        attempts: Number of attempts made (1 for success on first try).
    """

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
    timeout_s: float = DEFAULT_HTTP_TIMEOUT_S,
    retries: int = DEFAULT_HTTP_RETRIES,
) -> FetchResult:
    """
    Fetch real-time arrival predictions for a stop from the BG++ API.

    Makes an HTTP GET request to retrieve vehicle predictions for the specified stop.
    Automatically retries on failure with exponential backoff (base delay: 0.25s per attempt).

    The function always returns a FetchResult, never raises exceptions. Check the
    `error` field to determine if the request succeeded.

    Args:
        stop_code: The unique stop identifier to query.
        base_url: BG++ API base URL (default from constants).
        timeout_s: Request timeout in seconds (default: 10.0).
        retries: Number of retry attempts on failure (default: 2).

    Returns:
        FetchResult with either payload (on success) or error (on failure).

    Examples:
        >>> result = fetch_stop("1001", timeout_s=5.0, retries=1)
        >>> if result.error:
        ...     print(f"Failed: {result.error}")
        ... else:
        ...     vehicles = result.payload.get("vehicles", [])
        ...     print(f"Found {len(vehicles)} vehicles")
    """
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
            time.sleep(RETRY_BASE_DELAY_S * attempt)
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
