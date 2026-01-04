"""GTFS data loading and parsing for stops and stop times."""

from __future__ import annotations

import csv
from dataclasses import dataclass
from pathlib import Path
from typing import Iterator

from roundabout.utils import parse_float, parse_int


@dataclass(frozen=True)
class Stop:
    """
    GTFS stop record.

    Represents a physical stop location where vehicles pick up or drop off passengers.

    Attributes:
        stop_id: Unique stop identifier (integer).
        stop_code: Stop code displayed to riders (string).
        stop_name: Human-readable stop name.
        stop_lat: Stop latitude in WGS84.
        stop_lon: Stop longitude in WGS84.
    """

    stop_id: int
    stop_code: str
    stop_name: str
    stop_lat: float
    stop_lon: float


@dataclass(frozen=True)
class StopTime:
    """
    GTFS stop_time record.

    Represents a scheduled arrival/departure at a stop as part of a trip.
    Currently unused but defined for future schedule analysis.

    Attributes:
        trip_id: Trip identifier from trips.txt.
        arrival_time: Scheduled arrival time (HH:MM:SS format, may exceed 24h).
        departure_time: Scheduled departure time (HH:MM:SS format).
        stop_id: Stop identifier where event occurs.
        stop_sequence: Order of stop in the trip (1-indexed).
        pickup_type: Pickup availability (0=regular, 1=none, 2=phone, 3=driver).
        drop_off_type: Drop-off availability (same values as pickup_type).
        timepoint: Whether timing is exact (1) or approximate (0).
    """

    trip_id: str
    arrival_time: str
    departure_time: str
    stop_id: int
    stop_sequence: int
    pickup_type: int | None
    drop_off_type: int | None
    timepoint: int | None


def iter_stops(stops_csv: Path) -> Iterator[Stop]:
    """
    Iterate through stops from a GTFS stops.csv file.

    Skips rows with missing required fields (stop_code, stop_id, coordinates).
    Whitespace is automatically trimmed from all string fields.

    Args:
        stops_csv: Path to stops.csv file.

    Yields:
        Stop objects for each valid row.

    Raises:
        FileNotFoundError: If stops_csv doesn't exist.
        csv.Error: If CSV is malformed.

    Examples:
        >>> from pathlib import Path
        >>> for stop in iter_stops(Path("stops.csv")):
        ...     print(f"{stop.stop_code}: {stop.stop_name}")
    """
    with stops_csv.open("r", newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            stop_code = (row.get("stop_code") or "").strip()
            if not stop_code:
                continue
            stop_id_raw = (row.get("stop_id") or "").strip()
            if not stop_id_raw:
                continue
            try:
                stop_id = int(stop_id_raw)
            except ValueError:
                continue
            stop_name = (row.get("stop_name") or "").strip()
            stop_lat = parse_float(row.get("stop_lat"))
            stop_lon = parse_float(row.get("stop_lon"))
            if stop_lat is None or stop_lon is None:
                continue
            yield Stop(
                stop_id=stop_id,
                stop_code=stop_code,
                stop_name=stop_name,
                stop_lat=stop_lat,
                stop_lon=stop_lon,
            )


def load_stops(
    stops_csv: Path,
    *,
    stop_codes: set[str] | None = None,
    limit: int | None = None,
) -> list[Stop]:
    """
    Load stops from GTFS stops.csv file with optional filtering.

    Args:
        stops_csv: Path to stops.csv file.
        stop_codes: If provided, only load stops with these codes.
        limit: If provided, stop loading after this many stops.

    Returns:
        List of Stop objects matching the filters.

    Examples:
        >>> from pathlib import Path
        >>> # Load all stops
        >>> stops = load_stops(Path("stops.csv"))
        >>> # Load specific stops
        >>> stops = load_stops(Path("stops.csv"), stop_codes={"1001", "1002"})
        >>> # Load first 10 stops
        >>> stops = load_stops(Path("stops.csv"), limit=10)
    """
    stops: list[Stop] = []
    for stop in iter_stops(stops_csv):
        if stop_codes is not None and stop.stop_code not in stop_codes:
            continue
        stops.append(stop)
        if limit is not None and len(stops) >= limit:
            break
    return stops


def resolve_stop_times_files(stop_times_path: Path) -> list[Path]:
    """
    Resolve stop_times file path(s) for loading.

    Supports three patterns:
    1. Directory: Returns all stop_times*.csv files in directory
    2. File: Returns the single file
    3. Non-existent: Returns all stop_times_*.csv files in parent directory

    Args:
        stop_times_path: Path to file or directory.

    Returns:
        Sorted list of paths to stop_times CSV files.

    Note:
        This function is currently unused but defined for future schedule analysis.
    """
    if stop_times_path.is_dir():
        return sorted(stop_times_path.glob("stop_times*.csv"))
    if stop_times_path.exists():
        return [stop_times_path]
    return sorted(stop_times_path.parent.glob("stop_times_*.csv"))


def iter_stop_times(stop_times_path: Path) -> Iterator[StopTime]:
    """
    Iterate through stop_times from GTFS stop_times.csv file(s).

    Automatically resolves file paths and processes all matching files.
    Skips rows with missing required fields (trip_id, stop_id, stop_sequence).

    Args:
        stop_times_path: Path to file or directory containing stop_times files.

    Yields:
        StopTime objects for each valid row.

    Note:
        This function is currently unused but defined for future schedule analysis.
    """
    paths = resolve_stop_times_files(stop_times_path)
    for path in paths:
        with path.open("r", newline="", encoding="utf-8") as handle:
            reader = csv.DictReader(handle)
            for row in reader:
                trip_id = (row.get("trip_id") or "").strip()
                arrival_time = (row.get("arrival_time") or "").strip()
                departure_time = (row.get("departure_time") or "").strip()
                stop_id = parse_int((row.get("stop_id") or "").strip())
                stop_sequence = parse_int((row.get("stop_sequence") or "").strip())
                if not trip_id or stop_id is None or stop_sequence is None:
                    continue
                yield StopTime(
                    trip_id=trip_id,
                    arrival_time=arrival_time,
                    departure_time=departure_time,
                    stop_id=stop_id,
                    stop_sequence=stop_sequence,
                    pickup_type=parse_int((row.get("pickup_type") or "").strip() or None),
                    drop_off_type=parse_int((row.get("drop_off_type") or "").strip() or None),
                    timepoint=parse_int((row.get("timepoint") or "").strip() or None),
                )
