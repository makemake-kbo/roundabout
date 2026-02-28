"""Timetable simulation engine for optimistic vehicle position prediction."""

from __future__ import annotations

import bisect
import logging
from dataclasses import dataclass, field
from datetime import date, datetime
from pathlib import Path

from roundabout.gtfs import (
    Calendar,
    CalendarDate,
    Route,
    Trip,
    load_calendar,
    load_calendar_dates,
    load_routes,
    load_trips,
    resolve_stop_times_files,
)

LOG = logging.getLogger(__name__)


def parse_gtfs_time(time_str: str) -> int | None:
    """
    Parse a GTFS time string (HH:MM:SS) into seconds since midnight.

    Handles times >= 24:00:00 for late-night service that extends past midnight.

    Args:
        time_str: Time in HH:MM:SS format.

    Returns:
        Seconds since midnight, or None if parsing fails.
    """
    parts = time_str.strip().split(":")
    if len(parts) != 3:
        return None
    try:
        return int(parts[0]) * 3600 + int(parts[1]) * 60 + int(parts[2])
    except (ValueError, TypeError):
        return None


@dataclass(frozen=True)
class StopEvent:
    """A scheduled stop within a trip."""

    stop_id: int
    stop_sequence: int
    arrival_seconds: int
    departure_seconds: int


@dataclass(frozen=True)
class ActiveTrip:
    """A trip currently in progress based on the timetable."""

    trip_id: str
    route_id: str
    route_short_name: str
    direction_id: int | None
    service_id: str
    stop_events: list[StopEvent]
    first_departure: int
    last_arrival: int


@dataclass(frozen=True)
class EstimatedPosition:
    """Estimated vehicle position between two stops based on timetable interpolation."""

    prev_stop_id: int
    next_stop_id: int
    prev_stop_sequence: int
    next_stop_sequence: int
    fraction: float
    estimated_seconds_to_next: int
    schedule_deviation: int


class ServiceCalendar:
    """
    Determines active service_ids for a given date.

    Uses GTFS calendar.csv (weekly patterns) and calendar_dates.csv (exceptions).
    """

    def __init__(self, calendars: list[Calendar], exceptions: list[CalendarDate]) -> None:
        self._calendars = calendars
        # Index exceptions by date string for fast lookup
        self._additions: dict[str, set[str]] = {}
        self._removals: dict[str, set[str]] = {}
        for exc in exceptions:
            if exc.exception_type == 1:
                self._additions.setdefault(exc.date, set()).add(exc.service_id)
            elif exc.exception_type == 2:
                self._removals.setdefault(exc.date, set()).add(exc.service_id)

    @classmethod
    def from_csv(cls, calendar_csv: Path, calendar_dates_csv: Path) -> ServiceCalendar:
        """Load from GTFS CSV files."""
        calendars = load_calendar(calendar_csv)
        exceptions = load_calendar_dates(calendar_dates_csv)
        return cls(calendars, exceptions)

    def active_services(self, query_date: date) -> set[str]:
        """
        Return set of active service_ids for the given date.

        Checks weekly pattern from calendar.csv, then applies
        additions (exception_type=1) and removals (exception_type=2).
        """
        date_str = query_date.strftime("%Y%m%d")
        day_index = query_date.weekday()  # 0=Monday, 6=Sunday
        day_fields = [
            "monday", "tuesday", "wednesday", "thursday",
            "friday", "saturday", "sunday",
        ]

        active: set[str] = set()
        for cal in self._calendars:
            # Check date validity range
            if cal.start_date and date_str < cal.start_date:
                continue
            if cal.end_date and date_str > cal.end_date:
                continue
            # Check day-of-week flag
            day_value = getattr(cal, day_fields[day_index])
            if day_value == 1:
                active.add(cal.service_id)

        # Apply exceptions
        removals = self._removals.get(date_str, set())
        active -= removals
        additions = self._additions.get(date_str, set())
        active |= additions

        return active


class TimetableIndex:
    """
    Core timetable index for looking up active trips and estimating positions.

    Built once at startup by loading all GTFS data into memory.
    """

    def __init__(
        self,
        service_calendar: ServiceCalendar,
        trips: list[Trip],
        routes: list[Route],
        trip_stop_events: dict[str, list[StopEvent]],
    ) -> None:
        self._calendar = service_calendar
        self._trips = {t.trip_id: t for t in trips}
        self._routes = {r.route_id: r for r in routes}

        # Route short name lookup
        self._route_short_names: dict[str, str] = {
            r.route_id: r.route_short_name for r in routes
        }

        # Stop events indexed by trip_id, sorted by sequence
        self._trip_events = trip_stop_events

        # Build trip index by service_id for fast active trip lookup
        self._trips_by_service: dict[str, list[Trip]] = {}
        for trip in trips:
            self._trips_by_service.setdefault(trip.service_id, []).append(trip)

        # Precompute first_departure and last_arrival for each trip
        self._trip_times: dict[str, tuple[int, int]] = {}
        for trip_id, events in self._trip_events.items():
            if events:
                self._trip_times[trip_id] = (
                    events[0].departure_seconds,
                    events[-1].arrival_seconds,
                )

        LOG.info(
            "TimetableIndex built: %d trips, %d routes, %d trip schedules",
            len(self._trips),
            len(self._routes),
            len(self._trip_events),
        )

    @classmethod
    def build(
        cls,
        calendar_csv: Path,
        calendar_dates_csv: Path,
        routes_csv: Path,
        trips_csv: Path,
        stop_times_csv: Path,
    ) -> TimetableIndex:
        """
        Build a TimetableIndex from GTFS CSV files.

        This loads all data into memory. May take several seconds for large datasets.
        """
        import csv

        LOG.info("Building timetable index...")

        calendar = ServiceCalendar.from_csv(calendar_csv, calendar_dates_csv)
        routes = load_routes(routes_csv)
        trips = load_trips(trips_csv)

        # Load stop_times into trip -> events dict
        trip_stop_events: dict[str, list[StopEvent]] = {}
        rows_loaded = 0

        paths = resolve_stop_times_files(stop_times_csv)
        for path in paths:
            with path.open("r", newline="", encoding="utf-8") as handle:
                reader = csv.DictReader(handle)
                for row in reader:
                    trip_id = (row.get("trip_id") or "").strip()
                    if not trip_id:
                        continue
                    stop_id_raw = (row.get("stop_id") or "").strip()
                    seq_raw = (row.get("stop_sequence") or "").strip()
                    arrival_str = (row.get("arrival_time") or "").strip()
                    departure_str = (row.get("departure_time") or "").strip()

                    try:
                        stop_id = int(stop_id_raw)
                        stop_sequence = int(seq_raw)
                    except (ValueError, TypeError):
                        continue

                    arrival_s = parse_gtfs_time(arrival_str)
                    departure_s = parse_gtfs_time(departure_str)
                    if arrival_s is None or departure_s is None:
                        continue

                    event = StopEvent(
                        stop_id=stop_id,
                        stop_sequence=stop_sequence,
                        arrival_seconds=arrival_s,
                        departure_seconds=departure_s,
                    )
                    trip_stop_events.setdefault(trip_id, []).append(event)
                    rows_loaded += 1

        # Sort each trip's events by stop_sequence
        for events in trip_stop_events.values():
            events.sort(key=lambda e: e.stop_sequence)

        LOG.info("Loaded %d stop_time rows for %d trips", rows_loaded, len(trip_stop_events))

        return cls(calendar, trips, routes, trip_stop_events)

    def get_active_trips(self, now: datetime) -> list[ActiveTrip]:
        """
        Return trips currently in progress at the given time.

        A trip is active if:
        1. Its service_id is active for today's date
        2. The current time falls between its first departure and last arrival
        """
        current_date = now.date()
        active_services = self._calendar.active_services(current_date)

        if not active_services:
            return []

        now_seconds = now.hour * 3600 + now.minute * 60 + now.second

        # Also check previous day's late-night service (times >= 24:00:00)
        # If now_seconds < 3*3600 (before 3am), also check yesterday's trips with times > 24h
        check_late_night = now_seconds < 3 * 3600

        active_trips: list[ActiveTrip] = []

        for service_id in active_services:
            trips = self._trips_by_service.get(service_id, [])
            for trip in trips:
                times = self._trip_times.get(trip.trip_id)
                if times is None:
                    continue
                first_dep, last_arr = times
                if first_dep <= now_seconds <= last_arr:
                    events = self._trip_events.get(trip.trip_id, [])
                    route_name = self._route_short_names.get(trip.route_id, "")
                    active_trips.append(ActiveTrip(
                        trip_id=trip.trip_id,
                        route_id=trip.route_id,
                        route_short_name=route_name,
                        direction_id=trip.direction_id,
                        service_id=trip.service_id,
                        stop_events=events,
                        first_departure=first_dep,
                        last_arrival=last_arr,
                    ))

        # Handle late-night trips from today's schedule that wrap past midnight
        if check_late_night:
            adjusted_seconds = now_seconds + 24 * 3600
            for service_id in active_services:
                trips = self._trips_by_service.get(service_id, [])
                for trip in trips:
                    times = self._trip_times.get(trip.trip_id)
                    if times is None:
                        continue
                    first_dep, last_arr = times
                    # Only consider trips with times > 24h (late night)
                    if last_arr > 24 * 3600 and first_dep <= adjusted_seconds <= last_arr:
                        # Avoid duplicates
                        if any(at.trip_id == trip.trip_id for at in active_trips):
                            continue
                        events = self._trip_events.get(trip.trip_id, [])
                        route_name = self._route_short_names.get(trip.route_id, "")
                        active_trips.append(ActiveTrip(
                            trip_id=trip.trip_id,
                            route_id=trip.route_id,
                            route_short_name=route_name,
                            direction_id=trip.direction_id,
                            service_id=trip.service_id,
                            stop_events=events,
                            first_departure=first_dep,
                            last_arrival=last_arr,
                        ))

        return active_trips

    def estimate_position(
        self, trip: ActiveTrip, now_seconds: int
    ) -> EstimatedPosition | None:
        """
        Estimate where a vehicle is on a trip at the given time.

        Uses binary search to find which two stops the vehicle is between,
        then interpolates the position.

        Args:
            trip: The active trip.
            now_seconds: Current seconds since midnight (may exceed 24*3600 for late night).

        Returns:
            EstimatedPosition or None if position cannot be determined.
        """
        events = trip.stop_events
        if len(events) < 2:
            return None

        # Binary search for the current position
        # Find the first event whose departure_seconds > now_seconds
        departure_times = [e.departure_seconds for e in events]
        idx = bisect.bisect_right(departure_times, now_seconds)

        if idx == 0:
            # Before the first stop -- vehicle hasn't departed yet
            return EstimatedPosition(
                prev_stop_id=events[0].stop_id,
                next_stop_id=events[0].stop_id,
                prev_stop_sequence=events[0].stop_sequence,
                next_stop_sequence=events[0].stop_sequence,
                fraction=0.0,
                estimated_seconds_to_next=max(0, events[0].departure_seconds - now_seconds),
                schedule_deviation=0,
            )

        if idx >= len(events):
            # Past the last stop
            return EstimatedPosition(
                prev_stop_id=events[-1].stop_id,
                next_stop_id=events[-1].stop_id,
                prev_stop_sequence=events[-1].stop_sequence,
                next_stop_sequence=events[-1].stop_sequence,
                fraction=1.0,
                estimated_seconds_to_next=0,
                schedule_deviation=0,
            )

        # Between stops idx-1 and idx
        prev_event = events[idx - 1]
        next_event = events[idx]

        segment_duration = next_event.arrival_seconds - prev_event.departure_seconds
        if segment_duration <= 0:
            fraction = 1.0
        else:
            elapsed = now_seconds - prev_event.departure_seconds
            fraction = min(1.0, max(0.0, elapsed / segment_duration))

        seconds_to_next = max(0, next_event.arrival_seconds - now_seconds)

        return EstimatedPosition(
            prev_stop_id=prev_event.stop_id,
            next_stop_id=next_event.stop_id,
            prev_stop_sequence=prev_event.stop_sequence,
            next_stop_sequence=next_event.stop_sequence,
            fraction=fraction,
            estimated_seconds_to_next=seconds_to_next,
            schedule_deviation=0,
        )

    def get_route_short_name(self, route_id: str) -> str:
        """Look up route short name by route_id."""
        return self._route_short_names.get(route_id, "")

    def get_trip(self, trip_id: str) -> Trip | None:
        """Look up a trip by trip_id."""
        return self._trips.get(trip_id)

    def get_trip_events(self, trip_id: str) -> list[StopEvent]:
        """Get stop events for a trip."""
        return self._trip_events.get(trip_id, [])

    @property
    def all_trips(self) -> dict[str, Trip]:
        """All indexed trips."""
        return self._trips

    @property
    def trip_stop_events(self) -> dict[str, list[StopEvent]]:
        """All trip stop events."""
        return self._trip_events
