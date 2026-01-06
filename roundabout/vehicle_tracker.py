"""Cross-cycle vehicle tracking for movement detection."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any

from roundabout.utils import haversine_distance


@dataclass
class VehicleState:
    """
    State of a vehicle observed across multiple collection cycles.

    Attributes:
        vehicle_key: Unique vehicle identifier (garage:X or hash:X).
        last_cycle_id: Cycle ID when vehicle was last observed.
        last_observed_at: Timestamp of last observation.
        last_lat: Last observed latitude.
        last_lon: Last observed longitude.
        last_stop_code: Last observed stop code.
        last_line_number: Last observed line/route number.
        first_seen_cycle_id: Cycle ID when vehicle was first seen.
        cycles_seen: Number of cycles this vehicle has been observed.
    """

    vehicle_key: str
    last_cycle_id: str
    last_observed_at: datetime
    last_lat: float | None
    last_lon: float | None
    last_stop_code: str
    last_line_number: str | None
    first_seen_cycle_id: str
    cycles_seen: int


class VehicleTracker:
    """
    Tracks vehicles across collection cycles for movement detection.

    Maintains in-memory state of vehicles seen in recent cycles and provides
    movement detection by comparing current position with previous observations.

    Attributes:
        states: Dictionary mapping vehicle_key to VehicleState.
        ttl_cycles: Number of cycles before removing stale vehicles.
        cycle_counter: Tracks cycle count for cleanup.
        cycle_to_vehicles: Maps cycle_id to set of vehicle_keys for TTL tracking.

    Example:
        >>> tracker = VehicleTracker(ttl_cycles=5)
        >>> # First observation
        >>> prev = tracker.update("garage:P1234", "20260105T160000Z", now, 44.8, 20.5, "1001", "7")
        >>> # prev is None (new vehicle)
        >>> # Second observation at different location
        >>> prev = tracker.update("garage:P1234", "20260105T160045Z", now+45s, 44.81, 20.51, "1002", "7")
        >>> movement = tracker.detect_movement("garage:P1234", "1002", 44.81, 20.51)
        >>> print(f"Moved {movement['distance_km']:.2f} km")
    """

    def __init__(self, ttl_cycles: int = 5):
        """
        Initialize the vehicle tracker.

        Args:
            ttl_cycles: Number of cycles before removing stale vehicles (default: 5).
        """
        if ttl_cycles < 1:
            raise ValueError(f"ttl_cycles must be at least 1, got {ttl_cycles}")

        self.states: dict[str, VehicleState] = {}
        self.ttl_cycles = ttl_cycles
        self.cycle_counter = 0
        self.cycle_to_vehicles: dict[int, set[str]] = {}

    def update(
        self,
        vehicle_key: str,
        cycle_id: str,
        observed_at: datetime,
        lat: float | None,
        lon: float | None,
        stop_code: str,
        line_number: str | None,
    ) -> VehicleState | None:
        """
        Update vehicle state and return previous state if exists.

        Args:
            vehicle_key: Unique vehicle identifier.
            cycle_id: Current cycle identifier.
            observed_at: Current observation timestamp.
            lat: Current latitude.
            lon: Current longitude.
            stop_code: Current stop code.
            line_number: Current line/route number.

        Returns:
            Previous VehicleState if vehicle was seen before, None otherwise.

        Example:
            >>> prev = tracker.update("garage:P1234", "cycle1", now, 44.8, 20.5, "1001", "7")
            >>> if prev is None:
            ...     print("New vehicle")
        """
        previous = self.states.get(vehicle_key)

        self.states[vehicle_key] = VehicleState(
            vehicle_key=vehicle_key,
            last_cycle_id=cycle_id,
            last_observed_at=observed_at,
            last_lat=lat,
            last_lon=lon,
            last_stop_code=stop_code,
            last_line_number=line_number,
            first_seen_cycle_id=previous.first_seen_cycle_id if previous else cycle_id,
            cycles_seen=(previous.cycles_seen + 1) if previous else 1,
        )

        # Track vehicle in current cycle for TTL cleanup
        if self.cycle_counter not in self.cycle_to_vehicles:
            self.cycle_to_vehicles[self.cycle_counter] = set()
        self.cycle_to_vehicles[self.cycle_counter].add(vehicle_key)

        return previous

    def detect_movement(
        self,
        vehicle_key: str,
        current_stop_code: str,
        current_lat: float | None,
        current_lon: float | None,
    ) -> dict[str, Any]:
        """
        Detect if vehicle has moved since last observation.

        Args:
            vehicle_key: Vehicle to check.
            current_stop_code: Current stop code.
            current_lat: Current latitude.
            current_lon: Current longitude.

        Returns:
            Movement metrics dictionary with keys:
                - is_new: bool (True if vehicle not seen before)
                - stop_changed: bool (True if at different stop)
                - distance_km: float | None (haversine distance if coords available)
                - cycles_since_seen: int (always 1 for tracked vehicles)
                - previous_stop_code: str | None (previous stop if exists)
                - previous_lat: float | None
                - previous_lon: float | None

        Example:
            >>> movement = tracker.detect_movement("garage:P1234", "1002", 44.81, 20.51)
            >>> if movement["stop_changed"]:
            ...     print(f"Vehicle moved {movement['distance_km']:.2f} km")
        """
        previous = self.states.get(vehicle_key)

        if not previous:
            return {"is_new": True}

        # Calculate distance if both positions have coordinates
        distance_km = None
        if (
            previous.last_lat is not None
            and previous.last_lon is not None
            and current_lat is not None
            and current_lon is not None
        ):
            distance_km = haversine_distance(
                previous.last_lat, previous.last_lon, current_lat, current_lon
            )

        return {
            "is_new": False,
            "stop_changed": previous.last_stop_code != current_stop_code,
            "distance_km": distance_km,
            "cycles_since_seen": 1,  # Simplified: assume vehicle seen every cycle
            "previous_stop_code": previous.last_stop_code,
            "previous_lat": previous.last_lat,
            "previous_lon": previous.last_lon,
        }

    def cleanup(self) -> int:
        """
        Remove stale vehicles not seen in recent cycles.

        Vehicles are considered stale if they haven't been seen in the last
        ttl_cycles collection cycles.

        Returns:
            Number of vehicles removed from tracker.

        Example:
            >>> removed = tracker.cleanup()
            >>> print(f"Removed {removed} stale vehicles")
        """
        self.cycle_counter += 1

        # Determine which cycles are now stale
        stale_cycle = self.cycle_counter - self.ttl_cycles
        if stale_cycle < 0:
            return 0

        # Collect vehicle keys from stale cycles
        vehicles_to_remove = set()
        cycles_to_remove = []

        for cycle_num, vehicle_keys in self.cycle_to_vehicles.items():
            if cycle_num <= stale_cycle:
                vehicles_to_remove.update(vehicle_keys)
                cycles_to_remove.append(cycle_num)

        # Remove stale vehicles from states
        removed_count = 0
        for vehicle_key in vehicles_to_remove:
            # Only remove if vehicle hasn't been seen recently
            state = self.states.get(vehicle_key)
            if state:
                # Check if this vehicle appeared in more recent cycles
                is_stale = True
                for cycle_num, vehicle_keys in self.cycle_to_vehicles.items():
                    if cycle_num > stale_cycle and vehicle_key in vehicle_keys:
                        is_stale = False
                        break

                if is_stale:
                    del self.states[vehicle_key]
                    removed_count += 1

        # Clean up old cycle tracking
        for cycle_num in cycles_to_remove:
            del self.cycle_to_vehicles[cycle_num]

        return removed_count

    def get_vehicle_count(self) -> int:
        """
        Get the current number of tracked vehicles.

        Returns:
            Number of vehicles in tracker state.

        Example:
            >>> count = tracker.get_vehicle_count()
            >>> print(f"Tracking {count} vehicles")
        """
        return len(self.states)

    def get_vehicle_state(self, vehicle_key: str) -> VehicleState | None:
        """
        Get the current state of a specific vehicle.

        Args:
            vehicle_key: Vehicle to retrieve state for.

        Returns:
            VehicleState if vehicle is tracked, None otherwise.

        Example:
            >>> state = tracker.get_vehicle_state("garage:P1234")
            >>> if state:
            ...     print(f"Last seen at stop {state.last_stop_code}")
        """
        return self.states.get(vehicle_key)
