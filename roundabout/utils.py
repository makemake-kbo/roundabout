"""Shared utility functions for data parsing and formatting."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any


def parse_int(value: Any) -> int | None:
    """
    Parse a value to an integer, returning None if conversion fails.

    Args:
        value: The value to parse (can be string, int, or any type).

    Returns:
        Parsed integer or None if conversion fails or value is None.

    Examples:
        >>> parse_int("123")
        123
        >>> parse_int(456)
        456
        >>> parse_int("invalid")
        None
        >>> parse_int(None)
        None
    """
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def parse_float(value: Any) -> float | None:
    """
    Parse a value to a float, returning None if conversion fails.

    Args:
        value: The value to parse (can be string, float, int, or any type).

    Returns:
        Parsed float or None if conversion fails or value is None.

    Examples:
        >>> parse_float("123.45")
        123.45
        >>> parse_float(456.78)
        456.78
        >>> parse_float("invalid")
        None
        >>> parse_float(None)
        None
    """
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def format_timestamp(value: datetime, timespec: str = "milliseconds") -> str:
    """
    Format a datetime as ISO 8601 timestamp in UTC with 'Z' suffix.

    Args:
        value: The datetime to format.
        timespec: The precision of the timestamp (default: "milliseconds").

    Returns:
        ISO 8601 formatted timestamp string ending with 'Z'.

    Examples:
        >>> from datetime import datetime, timezone
        >>> dt = datetime(2024, 1, 1, 12, 30, 45, 123456, tzinfo=timezone.utc)
        >>> format_timestamp(dt)
        '2024-01-01T12:30:45.123Z'
    """
    return value.astimezone(timezone.utc).isoformat(timespec=timespec).replace("+00:00", "Z")


def parse_coords(coords: Any) -> tuple[float | None, float | None]:
    """
    Parse coordinate pair from a list or tuple.

    Args:
        coords: A list or tuple containing [latitude, longitude].

    Returns:
        Tuple of (latitude, longitude) as floats, or (None, None) if parsing fails.

    Examples:
        >>> parse_coords([44.7921, 20.5108])
        (44.7921, 20.5108)
        >>> parse_coords(["44.7921", "20.5108"])
        (44.7921, 20.5108)
        >>> parse_coords([44.7921])  # Missing longitude
        (None, None)
        >>> parse_coords("invalid")
        (None, None)
    """
    if not isinstance(coords, (list, tuple)) or len(coords) < 2:
        return None, None
    lat = parse_float(coords[0])
    lon = parse_float(coords[1])
    return lat, lon


def round_coordinate(value: float | None, decimal_places: int = 5) -> float | None:
    """
    Round a coordinate value to specified decimal places.

    Rounding to 5 decimal places provides approximately 1.1 meter precision,
    which is sufficient for vehicle tracking while reducing storage size.

    Args:
        value: The coordinate value to round.
        decimal_places: Number of decimal places (default: 5).

    Returns:
        Rounded coordinate or None if value is None.

    Examples:
        >>> round_coordinate(44.792145678)
        44.79215
        >>> round_coordinate(None)
        None
    """
    if value is None:
        return None
    return round(value, decimal_places)
