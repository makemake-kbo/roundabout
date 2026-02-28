"""Tests for timetable simulation engine."""

from __future__ import annotations

import csv
import tempfile
from datetime import date, datetime, timezone
from pathlib import Path

import pytest

from roundabout.gtfs import Calendar, CalendarDate
from roundabout.timetable import (
    ActiveTrip,
    EstimatedPosition,
    ServiceCalendar,
    StopEvent,
    TimetableIndex,
    parse_gtfs_time,
)


class TestParseGtfsTime:
    """Tests for GTFS time string parsing."""

    def test_normal_time(self):
        assert parse_gtfs_time("08:30:00") == 8 * 3600 + 30 * 60

    def test_midnight(self):
        assert parse_gtfs_time("00:00:00") == 0

    def test_late_night_past_midnight(self):
        assert parse_gtfs_time("25:30:00") == 25 * 3600 + 30 * 60

    def test_invalid_format(self):
        assert parse_gtfs_time("invalid") is None

    def test_empty_string(self):
        assert parse_gtfs_time("") is None

    def test_two_parts(self):
        assert parse_gtfs_time("08:30") is None


class TestServiceCalendar:
    """Tests for ServiceCalendar active service detection."""

    @pytest.fixture
    def calendar(self):
        calendars = [
            Calendar("RD", 1, 1, 1, 1, 1, 0, 0, "20230901", "20291231"),
            Calendar("S", 0, 0, 0, 0, 0, 1, 0, "20230901", "20291231"),
            Calendar("N", 0, 0, 0, 0, 0, 0, 1, "20230901", "20291231"),
        ]
        exceptions = [
            CalendarDate("N", "20250501", 1),  # Add Sunday service on May 1
            CalendarDate("RD", "20250501", 2),  # Remove weekday service on May 1
        ]
        return ServiceCalendar(calendars, exceptions)

    def test_weekday(self, calendar):
        # 2025-02-28 is a Friday
        active = calendar.active_services(date(2025, 2, 28))
        assert active == {"RD"}

    def test_saturday(self, calendar):
        # 2025-03-01 is a Saturday
        active = calendar.active_services(date(2025, 3, 1))
        assert active == {"S"}

    def test_sunday(self, calendar):
        # 2025-03-02 is a Sunday
        active = calendar.active_services(date(2025, 3, 2))
        assert active == {"N"}

    def test_holiday_override_add(self, calendar):
        # May 1 2025 is a Thursday, but has Sunday service added
        active = calendar.active_services(date(2025, 5, 1))
        assert "N" in active

    def test_holiday_override_remove(self, calendar):
        # May 1 2025 should have RD removed
        active = calendar.active_services(date(2025, 5, 1))
        assert "RD" not in active

    def test_out_of_range_date(self):
        calendars = [
            Calendar("RD", 1, 1, 1, 1, 1, 0, 0, "20230901", "20240101"),
        ]
        cal = ServiceCalendar(calendars, [])
        # 2025 is after end_date
        active = cal.active_services(date(2025, 1, 6))
        assert active == set()


class TestEstimatePosition:
    """Tests for TimetableIndex.estimate_position."""

    def _make_trip(self, events):
        return ActiveTrip(
            trip_id="test_trip",
            route_id="00007",
            route_short_name="7",
            direction_id=0,
            service_id="RD",
            stop_events=events,
            first_departure=events[0].departure_seconds if events else 0,
            last_arrival=events[-1].arrival_seconds if events else 0,
        )

    def _make_index(self):
        """Create a minimal TimetableIndex for testing estimate_position."""
        calendars = [Calendar("RD", 1, 1, 1, 1, 1, 0, 0, "20230901", "20291231")]
        cal = ServiceCalendar(calendars, [])
        from roundabout.gtfs import Route, Trip
        routes = [Route("00007", "1", "7", "Test Route", 3, None, None, None)]
        trips = [Trip("00007", "RD", "test_trip", None, 0)]
        return TimetableIndex(cal, trips, routes, {})

    def test_between_stops(self):
        events = [
            StopEvent(stop_id=100, stop_sequence=1, arrival_seconds=3600, departure_seconds=3600),
            StopEvent(stop_id=200, stop_sequence=2, arrival_seconds=3900, departure_seconds=3900),
            StopEvent(stop_id=300, stop_sequence=3, arrival_seconds=4200, departure_seconds=4200),
        ]
        trip = self._make_trip(events)
        index = self._make_index()

        # 3750s = halfway between stop 1 (dep 3600) and stop 2 (arr 3900)
        pos = index.estimate_position(trip, 3750)
        assert pos is not None
        assert pos.prev_stop_id == 100
        assert pos.next_stop_id == 200
        assert 0.4 < pos.fraction < 0.6

    def test_before_first_stop(self):
        events = [
            StopEvent(stop_id=100, stop_sequence=1, arrival_seconds=3600, departure_seconds=3600),
            StopEvent(stop_id=200, stop_sequence=2, arrival_seconds=3900, departure_seconds=3900),
        ]
        trip = self._make_trip(events)
        index = self._make_index()

        pos = index.estimate_position(trip, 3500)
        assert pos is not None
        assert pos.prev_stop_id == 100
        assert pos.fraction == 0.0

    def test_after_last_stop(self):
        events = [
            StopEvent(stop_id=100, stop_sequence=1, arrival_seconds=3600, departure_seconds=3600),
            StopEvent(stop_id=200, stop_sequence=2, arrival_seconds=3900, departure_seconds=3900),
        ]
        trip = self._make_trip(events)
        index = self._make_index()

        pos = index.estimate_position(trip, 4000)
        assert pos is not None
        assert pos.next_stop_id == 200
        assert pos.fraction == 1.0

    def test_single_stop_trip(self):
        events = [
            StopEvent(stop_id=100, stop_sequence=1, arrival_seconds=3600, departure_seconds=3600),
        ]
        trip = self._make_trip(events)
        index = self._make_index()

        pos = index.estimate_position(trip, 3600)
        assert pos is None


class TestTimetableIndexFromCSV:
    """Test loading TimetableIndex from actual GTFS CSV files."""

    @pytest.fixture
    def gtfs_dir(self, tmp_path):
        """Create minimal GTFS CSV files for testing."""
        # calendar.csv
        cal_path = tmp_path / "calendar.csv"
        with cal_path.open("w", newline="") as f:
            w = csv.writer(f)
            w.writerow(["service_id", "monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday", "start_date", "end_date"])
            w.writerow(["RD", 1, 1, 1, 1, 1, 0, 0, "20230901", "20291231"])

        # calendar_dates.csv
        cd_path = tmp_path / "calendar_dates.csv"
        with cd_path.open("w", newline="") as f:
            w = csv.writer(f)
            w.writerow(["service_id", "date", "exception_type"])

        # routes.csv
        routes_path = tmp_path / "routes.csv"
        with routes_path.open("w", newline="") as f:
            w = csv.writer(f)
            w.writerow(["route_id", "agency_id", "route_short_name", "route_long_name", "route_type"])
            w.writerow(["00007", "1", "7", "Test Route", 3])

        # trips.csv
        trips_path = tmp_path / "trips.csv"
        with trips_path.open("w", newline="") as f:
            w = csv.writer(f)
            w.writerow(["route_id", "service_id", "trip_id", "trip_headsign", "direction_id"])
            w.writerow(["00007", "RD", "00007_A_RD_0800", "Terminus", 0])

        # stop_times.csv
        st_path = tmp_path / "stop_times_00.csv"
        with st_path.open("w", newline="") as f:
            w = csv.writer(f)
            w.writerow(["trip_id", "arrival_time", "departure_time", "stop_id", "stop_sequence", "pickup_type", "drop_off_type", "timepoint"])
            w.writerow(["00007_A_RD_0800", "08:00:00", "08:00:00", 100, 1, 0, 0, 0])
            w.writerow(["00007_A_RD_0800", "08:05:00", "08:05:00", 200, 2, 0, 0, 0])
            w.writerow(["00007_A_RD_0800", "08:10:00", "08:10:00", 300, 3, 0, 0, 0])

        return tmp_path

    def test_build_from_csv(self, gtfs_dir):
        index = TimetableIndex.build(
            calendar_csv=gtfs_dir / "calendar.csv",
            calendar_dates_csv=gtfs_dir / "calendar_dates.csv",
            routes_csv=gtfs_dir / "routes.csv",
            trips_csv=gtfs_dir / "trips.csv",
            stop_times_csv=gtfs_dir / "stop_times_00.csv",
        )
        assert len(index.all_trips) == 1
        assert "00007_A_RD_0800" in index.trip_stop_events
        assert len(index.trip_stop_events["00007_A_RD_0800"]) == 3

    def test_active_trips_during_service(self, gtfs_dir):
        index = TimetableIndex.build(
            calendar_csv=gtfs_dir / "calendar.csv",
            calendar_dates_csv=gtfs_dir / "calendar_dates.csv",
            routes_csv=gtfs_dir / "routes.csv",
            trips_csv=gtfs_dir / "trips.csv",
            stop_times_csv=gtfs_dir / "stop_times_00.csv",
        )
        # Monday at 08:05 -- should find the trip
        now = datetime(2025, 2, 24, 8, 5, 0, tzinfo=timezone.utc)  # Monday
        active = index.get_active_trips(now)
        assert len(active) == 1
        assert active[0].trip_id == "00007_A_RD_0800"

    def test_no_active_trips_outside_service(self, gtfs_dir):
        index = TimetableIndex.build(
            calendar_csv=gtfs_dir / "calendar.csv",
            calendar_dates_csv=gtfs_dir / "calendar_dates.csv",
            routes_csv=gtfs_dir / "routes.csv",
            trips_csv=gtfs_dir / "trips.csv",
            stop_times_csv=gtfs_dir / "stop_times_00.csv",
        )
        # Saturday at 08:05 -- RD not active
        now = datetime(2025, 3, 1, 8, 5, 0, tzinfo=timezone.utc)  # Saturday
        active = index.get_active_trips(now)
        assert len(active) == 0
