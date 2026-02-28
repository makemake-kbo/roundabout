"""Vehicle state machine for optimistic timetable-based tracking."""

from __future__ import annotations

import enum
import logging
from dataclasses import dataclass, field
from datetime import datetime

LOG = logging.getLogger(__name__)


class VehicleStatus(enum.Enum):
    """Status of a tracked vehicle in the optimistic system."""

    PREDICTED = "predicted"
    VERIFIED = "verified"
    DELAYED = "delayed"
    STUCK = "stuck"
    LOST = "lost"
    UNMATCHED = "unmatched"


@dataclass
class TrackedVehicle:
    """
    State of a vehicle tracked across optimistic collection cycles.

    Attributes:
        vehicle_key: Unique vehicle identifier (garage:X or hash:X).
        matched_trip_id: GTFS trip_id this vehicle is matched to, if any.
        status: Current tracking status.
        line_number: Route/line number.
        direction: Direction of travel.
        last_stop_id: Last observed stop_id from API.
        last_stop_code: Last observed stop_code from API.
        schedule_deviation_s: Seconds behind (positive) or ahead of schedule.
        last_api_time: When this vehicle was last seen via API.
        last_api_cycle: Cycle ID of last API observation.
        consecutive_same_stop: Number of consecutive API polls at the same stop.
        cycles_since_api: Number of cycles since last API observation.
    """

    vehicle_key: str
    matched_trip_id: str | None = None
    status: VehicleStatus = VehicleStatus.PREDICTED
    line_number: str | None = None
    direction: str | None = None
    last_stop_id: int | None = None
    last_stop_code: str | None = None
    schedule_deviation_s: int = 0
    last_api_time: datetime | None = None
    last_api_cycle: str | None = None
    consecutive_same_stop: int = 0
    cycles_since_api: int = 0


class VehicleStateManager:
    """
    Manages state transitions for tracked vehicles in optimistic mode.

    State transitions:
    - PREDICTED -> VERIFIED: API confirms vehicle near expected position
    - PREDICTED -> DELAYED: API shows >deviation_threshold behind schedule
    - VERIFIED -> PREDICTED: time since last verification exceeds threshold
    - VERIFIED -> STUCK: same stop_id in N consecutive API polls
    - DELAYED/STUCK -> VERIFIED: vehicle catches up or starts moving
    - * -> LOST: not seen in API for lost_threshold cycles when expected
    """

    def __init__(
        self,
        deviation_threshold_s: int = 120,
        stuck_threshold_cycles: int = 3,
        lost_threshold_cycles: int = 5,
    ) -> None:
        self._vehicles: dict[str, TrackedVehicle] = {}
        self._deviation_threshold = deviation_threshold_s
        self._stuck_threshold = stuck_threshold_cycles
        self._lost_threshold = lost_threshold_cycles

    def get_or_create(self, vehicle_key: str) -> TrackedVehicle:
        """Get or create a tracked vehicle."""
        if vehicle_key not in self._vehicles:
            self._vehicles[vehicle_key] = TrackedVehicle(vehicle_key=vehicle_key)
        return self._vehicles[vehicle_key]

    def update_from_api(
        self,
        vehicle_key: str,
        stop_id: int,
        stop_code: str,
        line_number: str | None,
        direction: str | None,
        schedule_deviation_s: int,
        cycle_id: str,
        observed_at: datetime,
        matched_trip_id: str | None = None,
    ) -> TrackedVehicle:
        """
        Update vehicle state from an API observation.

        Performs state transitions based on new API data.
        """
        vehicle = self.get_or_create(vehicle_key)

        # Track consecutive same-stop observations
        if vehicle.last_stop_id == stop_id:
            vehicle.consecutive_same_stop += 1
        else:
            vehicle.consecutive_same_stop = 0

        # Update fields
        vehicle.last_stop_id = stop_id
        vehicle.last_stop_code = stop_code
        vehicle.line_number = line_number
        vehicle.direction = direction
        vehicle.schedule_deviation_s = schedule_deviation_s
        vehicle.last_api_time = observed_at
        vehicle.last_api_cycle = cycle_id
        vehicle.cycles_since_api = 0
        if matched_trip_id:
            vehicle.matched_trip_id = matched_trip_id

        # State transitions based on API data
        if vehicle.consecutive_same_stop >= self._stuck_threshold:
            vehicle.status = VehicleStatus.STUCK
        elif abs(schedule_deviation_s) > self._deviation_threshold:
            vehicle.status = VehicleStatus.DELAYED
        else:
            vehicle.status = VehicleStatus.VERIFIED

        return vehicle

    def update_predicted(self, vehicle_key: str) -> TrackedVehicle:
        """
        Mark vehicle as predicted (no API verification this cycle).

        Called for vehicles that weren't polled via API.
        """
        vehicle = self.get_or_create(vehicle_key)
        vehicle.cycles_since_api += 1

        if vehicle.cycles_since_api >= self._lost_threshold:
            vehicle.status = VehicleStatus.LOST
        elif vehicle.status == VehicleStatus.VERIFIED:
            vehicle.status = VehicleStatus.PREDICTED

        return vehicle

    def mark_unmatched(self, vehicle_key: str) -> TrackedVehicle:
        """Mark an API-observed vehicle that couldn't be matched to a trip."""
        vehicle = self.get_or_create(vehicle_key)
        vehicle.status = VehicleStatus.UNMATCHED
        return vehicle

    def get_escalation_stops(self) -> set[str]:
        """
        Return stop codes that need extra polling due to delayed/stuck vehicles.
        """
        codes: set[str] = set()
        for vehicle in self._vehicles.values():
            if vehicle.status in (VehicleStatus.DELAYED, VehicleStatus.STUCK):
                if vehicle.last_stop_code:
                    codes.add(vehicle.last_stop_code)
        return codes

    def cleanup_lost(self) -> int:
        """Remove LOST vehicles from tracking. Returns count removed."""
        lost_keys = [
            k for k, v in self._vehicles.items()
            if v.status == VehicleStatus.LOST
        ]
        for key in lost_keys:
            del self._vehicles[key]
        return len(lost_keys)

    @property
    def vehicles(self) -> dict[str, TrackedVehicle]:
        return self._vehicles

    def get_stats(self) -> dict[str, int]:
        """Return counts of vehicles by status."""
        stats: dict[str, int] = {}
        for vehicle in self._vehicles.values():
            status = vehicle.status.value
            stats[status] = stats.get(status, 0) + 1
        return stats
