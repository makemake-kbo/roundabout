"""GTFS data loading and parsing for stops, routes, trips, and stop times."""

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


@dataclass(frozen=True)
class Route:
    """
    GTFS route record.

    Represents a transit route/line with a unique identifier and service type.

    Attributes:
        route_id: Unique route identifier.
        agency_id: Agency operating this route.
        route_short_name: Short name displayed to riders (e.g., "7", "84", "E2").
        route_long_name: Full route name with endpoints.
        route_type: Vehicle type code (0=Tram, 3=Bus, 11=Trolleybus).
        route_url: Optional URL for route information.
        route_color: Optional hex color code for route branding.
        route_text_color: Optional text color for contrast.
    """

    route_id: str
    agency_id: str
    route_short_name: str
    route_long_name: str
    route_type: int
    route_url: str | None
    route_color: str | None
    route_text_color: str | None


@dataclass(frozen=True)
class Trip:
    """
    GTFS trip record.

    Represents a single trip along a route with direction and schedule information.

    Attributes:
        route_id: Route identifier this trip belongs to.
        service_id: Service calendar identifier.
        trip_id: Unique trip identifier.
        trip_headsign: Destination text displayed on vehicle.
        direction_id: Direction of travel (0/1 for inbound/outbound).
    """

    route_id: str
    service_id: str
    trip_id: str
    trip_headsign: str | None
    direction_id: int | None


def iter_routes(routes_csv: Path) -> Iterator[Route]:
    """
    Iterate through routes from GTFS routes.csv file.

    Skips rows with missing required fields (route_id, agency_id, route_short_name).

    Args:
        routes_csv: Path to routes.csv file.

    Yields:
        Route objects for each valid row.

    Examples:
        >>> from pathlib import Path
        >>> for route in iter_routes(Path("routes.csv")):
        ...     print(f"{route.route_short_name}: {route.route_long_name}")
    """
    with routes_csv.open("r", newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            route_id = (row.get("route_id") or "").strip()
            if not route_id:
                continue
            agency_id = (row.get("agency_id") or "").strip()
            if not agency_id:
                continue
            route_short_name = (row.get("route_short_name") or "").strip()
            if not route_short_name:
                continue
            route_long_name = (row.get("route_long_name") or "").strip()
            route_type_raw = (row.get("route_type") or "").strip()
            try:
                route_type = int(route_type_raw)
            except ValueError:
                continue
            route_url = (row.get("route_url") or "").strip() or None
            route_color = (row.get("route_color") or "").strip() or None
            route_text_color = (row.get("route_text_color") or "").strip() or None

            yield Route(
                route_id=route_id,
                agency_id=agency_id,
                route_short_name=route_short_name,
                route_long_name=route_long_name,
                route_type=route_type,
                route_url=route_url,
                route_color=route_color,
                route_text_color=route_text_color,
            )


def load_routes(
    routes_csv: Path,
    *,
    route_short_names: set[str] | None = None,
) -> list[Route]:
    """
    Load routes from GTFS routes.csv file with optional filtering.

    Args:
        routes_csv: Path to routes.csv file.
        route_short_names: If provided, only load routes with these short names.

    Returns:
        List of Route objects matching the filters.

    Examples:
        >>> from pathlib import Path
        >>> # Load all routes
        >>> routes = load_routes(Path("routes.csv"))
        >>> # Load specific routes
        >>> routes = load_routes(Path("routes.csv"), route_short_names={"7", "84"})
    """
    routes: list[Route] = []
    for route in iter_routes(routes_csv):
        if route_short_names is not None and route.route_short_name not in route_short_names:
            continue
        routes.append(route)
    return routes


def iter_trips(trips_csv: Path) -> Iterator[Trip]:
    """
    Iterate through trips from GTFS trips.csv file.

    Skips rows with missing required fields (route_id, service_id, trip_id).

    Args:
        trips_csv: Path to trips.csv file.

    Yields:
        Trip objects for each valid row.

    Examples:
        >>> from pathlib import Path
        >>> for trip in iter_trips(Path("trips.csv")):
        ...     print(f"{trip.trip_id}: {trip.route_id}")
    """
    with trips_csv.open("r", newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            route_id = (row.get("route_id") or "").strip()
            if not route_id:
                continue
            service_id = (row.get("service_id") or "").strip()
            if not service_id:
                continue
            trip_id = (row.get("trip_id") or "").strip()
            if not trip_id:
                continue
            trip_headsign = (row.get("trip_headsign") or "").strip() or None
            direction_id = parse_int((row.get("direction_id") or "").strip() or None)

            yield Trip(
                route_id=route_id,
                service_id=service_id,
                trip_id=trip_id,
                trip_headsign=trip_headsign,
                direction_id=direction_id,
            )


def load_trips(
    trips_csv: Path,
    *,
    route_ids: set[str] | None = None,
) -> list[Trip]:
    """
    Load trips from GTFS trips.csv file with optional filtering.

    Args:
        trips_csv: Path to trips.csv file.
        route_ids: If provided, only load trips for these route IDs.

    Returns:
        List of Trip objects matching the filters.

    Examples:
        >>> from pathlib import Path
        >>> # Load all trips
        >>> trips = load_trips(Path("trips.csv"))
        >>> # Load trips for specific routes
        >>> trips = load_trips(Path("trips.csv"), route_ids={"00007", "00084"})
    """
    trips: list[Trip] = []
    for trip in iter_trips(trips_csv):
        if route_ids is not None and trip.route_id not in route_ids:
            continue
        trips.append(trip)
    return trips


def filter_stops_by_bbox(
    stops: list[Stop],
    min_lat: float,
    max_lat: float,
    min_lon: float,
    max_lon: float,
) -> list[Stop]:
    """
    Filter stops by geographic bounding box.

    Args:
        stops: List of stops to filter.
        min_lat: Minimum latitude (south boundary).
        max_lat: Maximum latitude (north boundary).
        min_lon: Minimum longitude (west boundary).
        max_lon: Maximum longitude (east boundary).

    Returns:
        List of stops within the bounding box.

    Examples:
        >>> # Belgrade city center approximate bounds
        >>> filtered = filter_stops_by_bbox(stops, 44.78, 44.82, 20.45, 20.52)
    """
    return [
        stop
        for stop in stops
        if min_lat <= stop.stop_lat <= max_lat and min_lon <= stop.stop_lon <= max_lon
    ]


def build_route_stops_mapping(
    routes: list[Route],
    trips: list[Trip],
    stop_times_path: Path,
    stops: list[Stop],
) -> dict[str, list[Stop]]:
    """
    Build mapping of route short names to their stops.

    Joins routes → trips → stop_times → stops to determine which stops
    each route serves. This allows querying all stops on specific routes.

    Args:
        routes: List of routes to map.
        trips: List of trips (pre-filtered for these routes).
        stop_times_path: Path to stop_times CSV file(s).
        stops: List of all stops (pre-filtered by bbox if desired).

    Returns:
        Dictionary mapping route_short_name → list of unique Stop objects.

    Note:
        This function may take several seconds for large GTFS datasets
        due to the need to scan stop_times files.

    Examples:
        >>> mapping = build_route_stops_mapping(routes, trips, Path("stop_times.csv"), stops)
        >>> print(f"Route 7 serves {len(mapping['7'])} stops")
    """
    # Build trip_id → route_short_name mapping
    trip_to_route_name: dict[str, str] = {}
    route_id_to_short_name: dict[str, str] = {r.route_id: r.route_short_name for r in routes}
    for trip in trips:
        if trip.route_id in route_id_to_short_name:
            trip_to_route_name[trip.trip_id] = route_id_to_short_name[trip.route_id]

    # Build stop_id → Stop mapping for fast lookup
    stop_id_to_stop: dict[int, Stop] = {s.stop_id: s for s in stops}

    # Collect stop_ids per route_short_name
    route_stop_ids: dict[str, set[int]] = {r.route_short_name: set() for r in routes}

    # Scan stop_times to find which stops are on which routes
    for stop_time in iter_stop_times(stop_times_path):
        route_short_name = trip_to_route_name.get(stop_time.trip_id)
        if route_short_name and stop_time.stop_id in stop_id_to_stop:
            route_stop_ids[route_short_name].add(stop_time.stop_id)

    # Convert stop_id sets to Stop lists
    route_stops: dict[str, list[Stop]] = {}
    for route_short_name, stop_ids in route_stop_ids.items():
        route_stops[route_short_name] = [
            stop_id_to_stop[stop_id] for stop_id in sorted(stop_ids) if stop_id in stop_id_to_stop
        ]

    return route_stops
