"""Tests for vehicle state machine."""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from roundabout.optimistic_state import TrackedVehicle, VehicleStateManager, VehicleStatus


class TestVehicleStateManager:
    @pytest.fixture
    def manager(self):
        return VehicleStateManager(
            deviation_threshold_s=120,
            stuck_threshold_cycles=3,
            lost_threshold_cycles=5,
        )

    def test_new_vehicle_from_api_verified(self, manager):
        vehicle = manager.update_from_api(
            vehicle_key="garage:P80276",
            stop_id=100,
            stop_code="1001",
            line_number="7",
            direction="A",
            schedule_deviation_s=30,
            cycle_id="20250228T080000Z",
            observed_at=datetime(2025, 2, 28, 8, 0, 0, tzinfo=timezone.utc),
        )
        assert vehicle.status == VehicleStatus.VERIFIED
        assert vehicle.last_stop_id == 100

    def test_delayed_when_over_threshold(self, manager):
        vehicle = manager.update_from_api(
            vehicle_key="garage:P80276",
            stop_id=100,
            stop_code="1001",
            line_number="7",
            direction="A",
            schedule_deviation_s=200,  # > 120s threshold
            cycle_id="20250228T080000Z",
            observed_at=datetime(2025, 2, 28, 8, 0, 0, tzinfo=timezone.utc),
        )
        assert vehicle.status == VehicleStatus.DELAYED

    def test_stuck_after_consecutive_same_stop(self, manager):
        now = datetime(2025, 2, 28, 8, 0, 0, tzinfo=timezone.utc)
        # Same stop for 4 consecutive cycles (counter: 0, 1, 2, 3 >= threshold 3)
        for i in range(4):
            vehicle = manager.update_from_api(
                vehicle_key="garage:P80276",
                stop_id=100,
                stop_code="1001",
                line_number="7",
                direction="A",
                schedule_deviation_s=0,
                cycle_id=f"cycle_{i}",
                observed_at=now,
            )
        assert vehicle.status == VehicleStatus.STUCK

    def test_predicted_decay_to_lost(self, manager):
        # Create a vehicle
        manager.update_from_api(
            vehicle_key="garage:P80276",
            stop_id=100,
            stop_code="1001",
            line_number="7",
            direction="A",
            schedule_deviation_s=0,
            cycle_id="cycle_0",
            observed_at=datetime(2025, 2, 28, 8, 0, 0, tzinfo=timezone.utc),
        )

        # No API observation for 5 cycles
        for _ in range(5):
            vehicle = manager.update_predicted("garage:P80276")

        assert vehicle.status == VehicleStatus.LOST

    def test_unmatched_status(self, manager):
        vehicle = manager.mark_unmatched("garage:P80276")
        assert vehicle.status == VehicleStatus.UNMATCHED

    def test_escalation_stops(self, manager):
        now = datetime(2025, 2, 28, 8, 0, 0, tzinfo=timezone.utc)
        manager.update_from_api(
            vehicle_key="v1",
            stop_id=100,
            stop_code="1001",
            line_number="7",
            direction="A",
            schedule_deviation_s=200,  # Delayed
            cycle_id="cycle_0",
            observed_at=now,
        )
        codes = manager.get_escalation_stops()
        assert "1001" in codes

    def test_cleanup_lost(self, manager):
        # Create and lose a vehicle
        manager.update_from_api(
            vehicle_key="v1",
            stop_id=100,
            stop_code="1001",
            line_number="7",
            direction="A",
            schedule_deviation_s=0,
            cycle_id="cycle_0",
            observed_at=datetime(2025, 2, 28, 8, 0, 0, tzinfo=timezone.utc),
        )
        for _ in range(5):
            manager.update_predicted("v1")

        assert "v1" in manager.vehicles
        removed = manager.cleanup_lost()
        assert removed == 1
        assert "v1" not in manager.vehicles

    def test_get_stats(self, manager):
        now = datetime(2025, 2, 28, 8, 0, 0, tzinfo=timezone.utc)
        manager.update_from_api(
            vehicle_key="v1", stop_id=100, stop_code="1001",
            line_number="7", direction="A", schedule_deviation_s=0,
            cycle_id="c", observed_at=now,
        )
        manager.update_from_api(
            vehicle_key="v2", stop_id=200, stop_code="1002",
            line_number="7", direction="A", schedule_deviation_s=200,
            cycle_id="c", observed_at=now,
        )
        stats = manager.get_stats()
        assert stats.get("verified", 0) == 1
        assert stats.get("delayed", 0) == 1

    def test_verified_to_predicted_transition(self, manager):
        now = datetime(2025, 2, 28, 8, 0, 0, tzinfo=timezone.utc)
        manager.update_from_api(
            vehicle_key="v1", stop_id=100, stop_code="1001",
            line_number="7", direction="A", schedule_deviation_s=0,
            cycle_id="c", observed_at=now,
        )
        assert manager.vehicles["v1"].status == VehicleStatus.VERIFIED

        # One cycle without API -> PREDICTED
        vehicle = manager.update_predicted("v1")
        assert vehicle.status == VehicleStatus.PREDICTED

    def test_different_stop_resets_consecutive(self, manager):
        now = datetime(2025, 2, 28, 8, 0, 0, tzinfo=timezone.utc)
        # Same stop twice
        manager.update_from_api(
            vehicle_key="v1", stop_id=100, stop_code="1001",
            line_number="7", direction="A", schedule_deviation_s=0,
            cycle_id="c1", observed_at=now,
        )
        manager.update_from_api(
            vehicle_key="v1", stop_id=100, stop_code="1001",
            line_number="7", direction="A", schedule_deviation_s=0,
            cycle_id="c2", observed_at=now,
        )
        assert manager.vehicles["v1"].consecutive_same_stop == 1

        # Different stop resets
        manager.update_from_api(
            vehicle_key="v1", stop_id=200, stop_code="1002",
            line_number="7", direction="A", schedule_deviation_s=0,
            cycle_id="c3", observed_at=now,
        )
        assert manager.vehicles["v1"].consecutive_same_stop == 0
