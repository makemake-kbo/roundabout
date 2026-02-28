"""Tests for trip matcher."""

from __future__ import annotations

import pytest

from roundabout.timetable import ActiveTrip, StopEvent
from roundabout.trip_matcher import TripMatcher


def _make_active_trip(trip_id, route_name, direction_id, stop_events):
    return ActiveTrip(
        trip_id=trip_id,
        route_id="00007",
        route_short_name=route_name,
        direction_id=direction_id,
        service_id="RD",
        stop_events=stop_events,
        first_departure=stop_events[0].departure_seconds,
        last_arrival=stop_events[-1].arrival_seconds,
    )


@pytest.fixture
def active_trips():
    events_a = [
        StopEvent(stop_id=100, stop_sequence=1, arrival_seconds=28800, departure_seconds=28800),
        StopEvent(stop_id=200, stop_sequence=2, arrival_seconds=29100, departure_seconds=29100),
        StopEvent(stop_id=300, stop_sequence=3, arrival_seconds=29400, departure_seconds=29400),
    ]
    events_b = [
        StopEvent(stop_id=400, stop_sequence=1, arrival_seconds=28800, departure_seconds=28800),
        StopEvent(stop_id=500, stop_sequence=2, arrival_seconds=29100, departure_seconds=29100),
    ]
    return [
        _make_active_trip("trip_7_A", "7", 0, events_a),
        _make_active_trip("trip_7_B", "7", 1, events_b),
        _make_active_trip("trip_84_A", "84", 0, [
            StopEvent(stop_id=100, stop_sequence=1, arrival_seconds=28800, departure_seconds=28800),
            StopEvent(stop_id=600, stop_sequence=2, arrival_seconds=29100, departure_seconds=29100),
        ]),
    ]


class TestTripMatcher:
    def test_match_by_line_and_direction(self, active_trips):
        matcher = TripMatcher.__new__(TripMatcher)
        matcher._timetable = None
        matcher._threshold = 600

        match = matcher.match("7", "A", 200, 29100, active_trips)
        assert match is not None
        assert match.trip_id == "trip_7_A"
        assert match.time_deviation_s == 0  # On time

    def test_match_direction_b(self, active_trips):
        matcher = TripMatcher.__new__(TripMatcher)
        matcher._timetable = None
        matcher._threshold = 600

        match = matcher.match("7", "B", 500, 29100, active_trips)
        assert match is not None
        assert match.trip_id == "trip_7_B"

    def test_no_match_wrong_line(self, active_trips):
        matcher = TripMatcher.__new__(TripMatcher)
        matcher._timetable = None
        matcher._threshold = 600

        match = matcher.match("99", "A", 100, 29000, active_trips)
        assert match is None

    def test_no_match_wrong_stop(self, active_trips):
        matcher = TripMatcher.__new__(TripMatcher)
        matcher._timetable = None
        matcher._threshold = 600

        match = matcher.match("7", "A", 999, 29000, active_trips)
        assert match is None

    def test_match_with_deviation(self, active_trips):
        matcher = TripMatcher.__new__(TripMatcher)
        matcher._timetable = None
        matcher._threshold = 600

        # Vehicle at stop 200 at 29200 (100s late)
        match = matcher.match("7", "A", 200, 29200, active_trips)
        assert match is not None
        assert match.time_deviation_s == 100

    def test_no_match_excessive_deviation(self, active_trips):
        matcher = TripMatcher.__new__(TripMatcher)
        matcher._timetable = None
        matcher._threshold = 600

        # Vehicle at stop 200 at 30000 (900s late > 600s threshold)
        match = matcher.match("7", "A", 200, 30000, active_trips)
        assert match is None

    def test_direction_mapping(self):
        assert TripMatcher._map_direction("A") == 0
        assert TripMatcher._map_direction("B") == 1
        assert TripMatcher._map_direction("a") == 0
        assert TripMatcher._map_direction(None) is None
        assert TripMatcher._map_direction("C") is None
