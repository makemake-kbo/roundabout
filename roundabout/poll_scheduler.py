"""Smart polling strategy for optimistic timetable-based collection."""

from __future__ import annotations

import logging
import random
from dataclasses import dataclass, field

from roundabout.gtfs import Stop
from roundabout.timetable import TimetableIndex

LOG = logging.getLogger(__name__)


@dataclass(frozen=True)
class PollPlan:
    """Stops to poll in a single cycle."""

    checkpoint_stops: list[Stop]
    verification_stops: list[Stop]
    escalation_stops: list[Stop]
    discovery_stops: list[Stop]

    @property
    def all_stops(self) -> list[Stop]:
        """All unique stops to poll, deduped by stop_code."""
        seen: set[str] = set()
        result: list[Stop] = []
        for stop in (
            self.checkpoint_stops
            + self.verification_stops
            + self.escalation_stops
            + self.discovery_stops
        ):
            if stop.stop_code not in seen:
                seen.add(stop.stop_code)
                result.append(stop)
        return result

    @property
    def total(self) -> int:
        return len(self.all_stops)


class CheckpointSelector:
    """
    Selects strategic checkpoint stops per route for consistent polling.

    For each route+direction, picks every Nth stop (stride), always including
    first and last. Prefers stops served by multiple routes.
    """

    def __init__(
        self,
        timetable: TimetableIndex,
        all_stops: list[Stop],
        stride: int = 5,
    ) -> None:
        self._stop_by_id: dict[int, Stop] = {s.stop_id: s for s in all_stops}
        self._checkpoints: list[Stop] = []
        self._checkpoint_codes: set[str] = set()
        self._non_checkpoint_stops: list[Stop] = []

        self._build(timetable, stride)

    def _build(self, timetable: TimetableIndex, stride: int) -> None:
        """Build checkpoint set from timetable route data."""
        # Count how many routes serve each stop (for prioritization)
        stop_route_count: dict[int, int] = {}

        # Collect ordered stop lists per route+direction
        route_dir_stops: dict[tuple[str, int | None], list[int]] = {}

        for trip_id, events in timetable.trip_stop_events.items():
            trip = timetable.get_trip(trip_id)
            if trip is None:
                continue
            route_name = timetable.get_route_short_name(trip.route_id)
            key = (route_name, trip.direction_id)

            if key not in route_dir_stops:
                route_dir_stops[key] = [e.stop_id for e in events]

            for e in events:
                stop_route_count[e.stop_id] = stop_route_count.get(e.stop_id, 0) + 1

        # Select checkpoints: every Nth stop per route+direction
        checkpoint_ids: set[int] = set()

        for (_route, _direction), stop_ids in route_dir_stops.items():
            if not stop_ids:
                continue
            # Always include first and last
            checkpoint_ids.add(stop_ids[0])
            checkpoint_ids.add(stop_ids[-1])
            # Every Nth stop
            for i in range(stride, len(stop_ids) - 1, stride):
                checkpoint_ids.add(stop_ids[i])

        # Convert to Stop objects
        for stop_id in checkpoint_ids:
            stop = self._stop_by_id.get(stop_id)
            if stop:
                self._checkpoints.append(stop)
                self._checkpoint_codes.add(stop.stop_code)

        # Build non-checkpoint list
        for stop in self._stop_by_id.values():
            if stop.stop_code not in self._checkpoint_codes:
                self._non_checkpoint_stops.append(stop)

        LOG.info(
            "CheckpointSelector: %d checkpoints, %d non-checkpoint stops",
            len(self._checkpoints),
            len(self._non_checkpoint_stops),
        )

    @property
    def checkpoints(self) -> list[Stop]:
        return self._checkpoints

    @property
    def checkpoint_codes(self) -> set[str]:
        return self._checkpoint_codes

    @property
    def non_checkpoint_stops(self) -> list[Stop]:
        return self._non_checkpoint_stops


class PollScheduler:
    """
    Decides which stops to poll each cycle.

    Combines:
    - Checkpoints: fixed strategic stops, always polled
    - Verification: rotating random sample from non-checkpoint stops
    - Escalation: extra stops near delayed/stuck vehicles
    - Discovery: broader sweep every Nth cycle for unscheduled vehicles
    """

    def __init__(
        self,
        checkpoint_selector: CheckpointSelector,
        all_stops: list[Stop],
        verification_batch_size: int = 80,
        discovery_interval: int = 10,
    ) -> None:
        self._selector = checkpoint_selector
        self._all_stops = all_stops
        self._stop_by_code: dict[str, Stop] = {s.stop_code: s for s in all_stops}
        self._verification_batch_size = verification_batch_size
        self._discovery_interval = discovery_interval
        self._cycle_count = 0

        # Rotating verification: track which non-checkpoint stops have been sampled
        self._verification_pool = list(checkpoint_selector.non_checkpoint_stops)
        self._verification_index = 0

    def build_plan(
        self,
        escalation_stop_codes: set[str] | None = None,
    ) -> PollPlan:
        """
        Build a poll plan for the current cycle.

        Args:
            escalation_stop_codes: Extra stop codes to poll due to delays/stuck vehicles.

        Returns:
            PollPlan with categorized stops.
        """
        self._cycle_count += 1

        # 1. Checkpoints -- always polled
        checkpoint_stops = list(self._selector.checkpoints)

        # 2. Verification -- rotating sample
        verification_stops = self._get_verification_sample()

        # 3. Escalation -- extra stops for delayed vehicles
        escalation_stops: list[Stop] = []
        if escalation_stop_codes:
            for code in escalation_stop_codes:
                stop = self._stop_by_code.get(code)
                if stop and code not in self._selector.checkpoint_codes:
                    escalation_stops.append(stop)

        # 4. Discovery -- broader sweep every Nth cycle
        discovery_stops: list[Stop] = []
        if self._cycle_count % self._discovery_interval == 0:
            # Sample ~300 non-checkpoint stops not already in verification
            verification_codes = {s.stop_code for s in verification_stops}
            available = [
                s for s in self._selector.non_checkpoint_stops
                if s.stop_code not in verification_codes
            ]
            discovery_count = min(300, len(available))
            if available:
                discovery_stops = random.sample(available, discovery_count)

        return PollPlan(
            checkpoint_stops=checkpoint_stops,
            verification_stops=verification_stops,
            escalation_stops=escalation_stops,
            discovery_stops=discovery_stops,
        )

    def _get_verification_sample(self) -> list[Stop]:
        """Get the next batch of verification stops from the rotating pool."""
        pool = self._verification_pool
        if not pool:
            return []

        # Reshuffle when we've gone through all stops
        if self._verification_index >= len(pool):
            random.shuffle(pool)
            self._verification_index = 0

        batch_size = min(self._verification_batch_size, len(pool))
        start = self._verification_index
        end = start + batch_size

        if end <= len(pool):
            sample = pool[start:end]
        else:
            # Wrap around
            sample = pool[start:] + pool[: end - len(pool)]

        self._verification_index = end % len(pool)
        return sample
