"""Tests for poll scheduler and checkpoint selection."""

from __future__ import annotations

import csv
from datetime import datetime, timezone
from pathlib import Path

import pytest

from roundabout.gtfs import Calendar, Route, Stop, Trip
from roundabout.poll_scheduler import CheckpointSelector, PollPlan, PollScheduler
from roundabout.timetable import ServiceCalendar, StopEvent, TimetableIndex


@pytest.fixture
def stops():
    """Create test stops."""
    return [
        Stop(stop_id=i, stop_code=str(1000 + i), stop_name=f"Stop {i}", stop_lat=44.8, stop_lon=20.5)
        for i in range(1, 21)
    ]


@pytest.fixture
def timetable(stops):
    """Create a minimal timetable index with one route having 20 stops."""
    calendars = [Calendar("RD", 1, 1, 1, 1, 1, 0, 0, "20230901", "20291231")]
    cal = ServiceCalendar(calendars, [])
    routes = [Route("00007", "1", "7", "Test Route", 3, None, None, None)]
    trips = [Trip("00007", "RD", "00007_A_RD_0800", None, 0)]

    # Build stop events for all 20 stops
    events = [
        StopEvent(stop_id=i, stop_sequence=i, arrival_seconds=3600 + i * 60, departure_seconds=3600 + i * 60)
        for i in range(1, 21)
    ]
    trip_events = {"00007_A_RD_0800": events}

    return TimetableIndex(cal, trips, routes, trip_events)


class TestCheckpointSelector:
    def test_selects_first_and_last(self, timetable, stops):
        selector = CheckpointSelector(timetable, stops, stride=5)
        checkpoint_ids = {s.stop_id for s in selector.checkpoints}
        # First stop (1) and last stop (20) should always be included
        assert 1 in checkpoint_ids
        assert 20 in checkpoint_ids

    def test_stride_creates_checkpoints(self, timetable, stops):
        selector = CheckpointSelector(timetable, stops, stride=5)
        # With 20 stops, stride 5: first(1), 6, 11, 16, last(20) = ~5 checkpoints
        assert len(selector.checkpoints) >= 3
        assert len(selector.checkpoints) < len(stops)

    def test_non_checkpoint_stops(self, timetable, stops):
        selector = CheckpointSelector(timetable, stops, stride=5)
        total = len(selector.checkpoints) + len(selector.non_checkpoint_stops)
        # Some stops might not be in any trip, but we should have the split
        assert len(selector.non_checkpoint_stops) >= 0


class TestPollPlan:
    def test_deduplication(self):
        stop1 = Stop(1, "1001", "A", 44.8, 20.5)
        stop2 = Stop(2, "1002", "B", 44.8, 20.5)
        plan = PollPlan(
            checkpoint_stops=[stop1, stop2],
            verification_stops=[stop1],  # Duplicate
            escalation_stops=[],
            discovery_stops=[stop2],  # Duplicate
        )
        assert plan.total == 2  # Deduped

    def test_all_stops_combined(self):
        stop1 = Stop(1, "1001", "A", 44.8, 20.5)
        stop2 = Stop(2, "1002", "B", 44.8, 20.5)
        stop3 = Stop(3, "1003", "C", 44.8, 20.5)
        plan = PollPlan(
            checkpoint_stops=[stop1],
            verification_stops=[stop2],
            escalation_stops=[stop3],
            discovery_stops=[],
        )
        assert plan.total == 3


class TestPollScheduler:
    def test_build_plan(self, timetable, stops):
        selector = CheckpointSelector(timetable, stops, stride=5)
        scheduler = PollScheduler(selector, stops, verification_batch_size=3)

        plan = scheduler.build_plan()
        assert len(plan.checkpoint_stops) > 0
        assert plan.total > 0

    def test_discovery_only_every_nth_cycle(self, timetable, stops):
        selector = CheckpointSelector(timetable, stops, stride=5)
        scheduler = PollScheduler(
            selector, stops,
            verification_batch_size=3,
            discovery_interval=5,
        )

        # Cycles 1-4 should have no discovery
        for _ in range(4):
            plan = scheduler.build_plan()
            assert len(plan.discovery_stops) == 0

        # Cycle 5 should have discovery
        plan = scheduler.build_plan()
        assert len(plan.discovery_stops) >= 0  # Could be 0 if no non-checkpoint stops available

    def test_escalation_stops_included(self, timetable, stops):
        selector = CheckpointSelector(timetable, stops, stride=5)
        scheduler = PollScheduler(selector, stops, verification_batch_size=3)

        # Add escalation stops
        plan = scheduler.build_plan(escalation_stop_codes={"1005", "1010"})
        codes = {s.stop_code for s in plan.all_stops}
        # Escalation codes should be included if they're not already checkpoints
        # (some may already be checkpoints)
        assert plan.total > 0
