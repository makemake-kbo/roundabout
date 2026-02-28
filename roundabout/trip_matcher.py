"""Vehicle-to-trip matching for optimistic timetable-based collection."""

from __future__ import annotations

import logging
from dataclasses import dataclass

from roundabout.timetable import ActiveTrip, TimetableIndex

LOG = logging.getLogger(__name__)


@dataclass(frozen=True)
class TripMatch:
    """Result of matching a vehicle to a GTFS trip."""

    trip_id: str
    route_short_name: str
    direction_id: int | None
    time_deviation_s: int
    confidence: float


class TripMatcher:
    """
    Matches observed API vehicles to GTFS trips.

    Strategy:
    1. Filter active trips by route_short_name == line_number
    2. Filter by direction (API direction A->0, B->1)
    3. Find the trip where the vehicle should be near the observed stop
    4. Accept match if time deviation < threshold
    """

    def __init__(
        self,
        timetable: TimetableIndex,
        deviation_threshold_s: int = 600,
    ) -> None:
        self._timetable = timetable
        self._threshold = deviation_threshold_s

    def match(
        self,
        line_number: str,
        direction: str | None,
        stop_id: int,
        now_seconds: int,
        active_trips: list[ActiveTrip],
    ) -> TripMatch | None:
        """
        Match an observed vehicle to a GTFS trip.

        Args:
            line_number: Line number from API (e.g., "7", "84").
            direction: Direction from API (e.g., "A", "B").
            stop_id: Stop ID where the vehicle was observed.
            now_seconds: Current seconds since midnight.
            active_trips: Currently active trips from the timetable.

        Returns:
            TripMatch if a match is found, None otherwise.
        """
        # Map API direction to GTFS direction_id
        direction_id = self._map_direction(direction)

        # Filter by route
        candidates = [
            t for t in active_trips
            if t.route_short_name == line_number
        ]

        if not candidates:
            return None

        # Filter by direction if available
        if direction_id is not None:
            directed = [t for t in candidates if t.direction_id == direction_id]
            if directed:
                candidates = directed

        # Find best match: trip where the vehicle should be near stop_id at now_seconds
        best_match: TripMatch | None = None
        best_deviation = self._threshold + 1

        for trip in candidates:
            deviation = self._compute_deviation(trip, stop_id, now_seconds)
            if deviation is not None and abs(deviation) < best_deviation:
                best_deviation = abs(deviation)
                confidence = max(0.0, 1.0 - (best_deviation / self._threshold))
                best_match = TripMatch(
                    trip_id=trip.trip_id,
                    route_short_name=trip.route_short_name,
                    direction_id=trip.direction_id,
                    time_deviation_s=deviation,
                    confidence=confidence,
                )

        return best_match

    def _compute_deviation(
        self, trip: ActiveTrip, stop_id: int, now_seconds: int
    ) -> int | None:
        """
        Compute time deviation between observed and scheduled position.

        Returns how many seconds the vehicle is from where it should be
        relative to the given stop. Negative = early, positive = late.
        Returns None if stop not found in trip.
        """
        for event in trip.stop_events:
            if event.stop_id == stop_id:
                # Vehicle is at this stop; deviation = now - scheduled_arrival
                return now_seconds - event.arrival_seconds
        return None

    @staticmethod
    def _map_direction(direction: str | None) -> int | None:
        """
        Map API direction string to GTFS direction_id.

        Convention: A -> 0, B -> 1.
        Also checks trip_id convention where direction is embedded.
        """
        if direction is None:
            return None
        direction = direction.strip().upper()
        if direction == "A":
            return 0
        if direction == "B":
            return 1
        return None
