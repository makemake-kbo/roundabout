"""Optimistic timetable-based collection orchestrator.

Predicts vehicle positions from GTFS timetable data and only polls a
small subset of stops via API to verify predictions and detect delays.
Targets ~85-90% reduction in API calls vs full polling.
"""

from __future__ import annotations

import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

from roundabout.bgpp import FetchResult, fetch_stop
from roundabout.clickhouse import ClickHouseBatchWriter, ClickHouseClient, ClickHouseConfig
from roundabout.config import CollectorConfig, CycleSummary
from roundabout.constants import (
    CLICKHOUSE_TABLE_CYCLES,
    CLICKHOUSE_TABLE_ERRORS,
    CLICKHOUSE_TABLE_PREDICTIONS,
    CLICKHOUSE_TABLE_VEHICLES,
    CYCLE_ID_FORMAT,
)
from roundabout.gtfs import Stop
from roundabout.optimistic_state import VehicleStateManager, VehicleStatus
from roundabout.poll_scheduler import CheckpointSelector, PollScheduler
from roundabout.processor import process_cycle
from roundabout.rate_limiter import TokenBucketRateLimiter
from roundabout.storage import JsonlWriter
from roundabout.timetable import TimetableIndex
from roundabout.transformers import (
    build_error_record,
    build_output_paths,
    build_prediction_record,
    build_timetable_prediction_record,
    build_vehicle_record,
)
from roundabout.trip_matcher import TripMatcher
from roundabout.utils import format_timestamp
from roundabout.vehicle_tracker import VehicleTracker

LOG = logging.getLogger(__name__)


def optimistic_collect_once(
    stops: list[Stop],
    config: CollectorConfig,
    timetable: TimetableIndex,
    poll_scheduler: PollScheduler,
    trip_matcher: TripMatcher,
    state_manager: VehicleStateManager,
    rate_limiter: TokenBucketRateLimiter | None = None,
    vehicle_tracker: VehicleTracker | None = None,
) -> CycleSummary:
    """
    Execute a single optimistic collection cycle.

    Steps:
    1. Get active trips from timetable, estimate all vehicle positions
    2. Build poll plan (subset of stops)
    3. Fetch API data for selected stops
    4. Match API vehicles to trips
    5. Update vehicle states
    6. Emit records (both API and timetable-predicted)
    7. Write to storage
    """
    started_at = datetime.now(timezone.utc)
    cycle_id = started_at.strftime(CYCLE_ID_FORMAT)
    output_paths = build_output_paths(config.output_dir, cycle_id, started_at)
    now_seconds = started_at.hour * 3600 + started_at.minute * 60 + started_at.second

    # 1. Get active trips and estimate positions
    active_trips = timetable.get_active_trips(started_at)
    LOG.info("Active trips: %d", len(active_trips))

    trip_positions: dict[str, dict] = {}
    for trip in active_trips:
        pos = timetable.estimate_position(trip, now_seconds)
        if pos:
            trip_positions[trip.trip_id] = {
                "trip": trip,
                "position": pos,
            }

    # 2. Build poll plan
    escalation_codes = state_manager.get_escalation_stops()
    plan = poll_scheduler.build_plan(escalation_stop_codes=escalation_codes)
    stops_to_poll = plan.all_stops

    LOG.info(
        "Poll plan: checkpoints=%d verification=%d escalation=%d discovery=%d total=%d",
        len(plan.checkpoint_stops),
        len(plan.verification_stops),
        len(plan.escalation_stops),
        len(plan.discovery_stops),
        plan.total,
    )

    # Initialize writers
    predictions_writer = None
    vehicles_writer = None
    errors_writer = None
    if config.jsonl_enabled:
        predictions_writer = JsonlWriter(output_paths["predictions"])
        vehicles_writer = JsonlWriter(output_paths["vehicles"])
        errors_writer = JsonlWriter(output_paths["errors"])

    clickhouse_client = None
    ch_predictions = None
    ch_vehicles = None
    ch_errors = None
    ch_cycles = None

    if config.clickhouse_enabled:
        clickhouse_client = ClickHouseClient(
            ClickHouseConfig(
                url=config.clickhouse_url,
                database=config.clickhouse_database,
                user=config.clickhouse_user,
                password=config.clickhouse_password,
                timeout_s=config.clickhouse_timeout_s,
            )
        )
        ch_predictions = ClickHouseBatchWriter(
            clickhouse_client, CLICKHOUSE_TABLE_PREDICTIONS,
            batch_size=config.clickhouse_batch_size,
        )
        ch_vehicles = ClickHouseBatchWriter(
            clickhouse_client, CLICKHOUSE_TABLE_VEHICLES,
            batch_size=config.clickhouse_batch_size,
        )
        ch_errors = ClickHouseBatchWriter(
            clickhouse_client, CLICKHOUSE_TABLE_ERRORS,
            batch_size=config.clickhouse_batch_size,
        )
        ch_cycles = ClickHouseBatchWriter(
            clickhouse_client, CLICKHOUSE_TABLE_CYCLES,
            batch_size=config.clickhouse_batch_size,
        )

    seen_vehicle_keys: set[str] = set()
    predictions_count = 0
    timetable_predictions_count = 0
    unique_vehicles = 0
    error_count = 0
    response_count = 0

    # Build stop_id -> Stop lookup for the polled stops
    stop_by_code: dict[str, Stop] = {s.stop_code: s for s in stops}
    stop_by_id: dict[int, Stop] = {s.stop_id: s for s in stops}

    # Track which trips were verified via API
    verified_trip_ids: set[str] = set()

    def rate_limited_fetch(stop: Stop) -> FetchResult:
        if rate_limiter:
            rate_limiter.acquire()
        return fetch_stop(
            stop.stop_code,
            base_url=config.base_url,
            timeout_s=config.timeout_s,
            retries=config.retries,
        )

    try:
        # 3. Fetch API data for selected stops
        with ThreadPoolExecutor(max_workers=config.concurrency) as executor:
            future_to_stop = {
                executor.submit(rate_limited_fetch, stop): stop
                for stop in stops_to_poll
            }

            for future in as_completed(future_to_stop):
                stop = future_to_stop[future]

                try:
                    result = future.result()
                except Exception as exc:
                    error_count += 1
                    unexpected_record = {
                        "observed_at": format_timestamp(datetime.now(timezone.utc)),
                        "cycle_id": cycle_id,
                        "stop_id": stop.stop_id,
                        "stop_code": stop.stop_code,
                        "error": f"unexpected:{exc}",
                        "http_status": None,
                        "attempts": 0,
                        "duration_ms": 0,
                    }
                    if errors_writer:
                        errors_writer.write(unexpected_record)
                    if ch_errors:
                        ch_errors.write(unexpected_record)
                    continue

                response_count += 1

                if result.error:
                    error_count += 1
                    error_record = build_error_record(
                        stop=stop, result=result, cycle_id=cycle_id
                    )
                    if errors_writer:
                        errors_writer.write(error_record)
                    if ch_errors:
                        ch_errors.write(error_record)
                    continue

                # Process API vehicle predictions
                vehicles = (
                    result.payload.get("vehicles")
                    if isinstance(result.payload, dict)
                    else None
                )
                if not isinstance(vehicles, list):
                    vehicles = []

                for vehicle in vehicles:
                    prediction = build_prediction_record(
                        stop=stop,
                        result=result,
                        vehicle=vehicle,
                        cycle_id=cycle_id,
                    )
                    if predictions_writer:
                        predictions_writer.write(prediction)
                    if ch_predictions:
                        ch_predictions.write(prediction)
                    predictions_count += 1

                    # 4. Match API vehicle to trip
                    line_number = prediction.get("line_number")
                    direction = prediction.get("direction")
                    if line_number:
                        match = trip_matcher.match(
                            line_number=line_number,
                            direction=direction,
                            stop_id=stop.stop_id,
                            now_seconds=now_seconds,
                            active_trips=active_trips,
                        )

                        # 5. Update vehicle state
                        if match:
                            verified_trip_ids.add(match.trip_id)
                            state_manager.update_from_api(
                                vehicle_key=prediction["vehicle_key"],
                                stop_id=stop.stop_id,
                                stop_code=stop.stop_code,
                                line_number=line_number,
                                direction=direction,
                                schedule_deviation_s=match.time_deviation_s,
                                cycle_id=cycle_id,
                                observed_at=result.observed_at,
                                matched_trip_id=match.trip_id,
                            )
                        else:
                            state_manager.mark_unmatched(prediction["vehicle_key"])

                    # Vehicle tracker update
                    if vehicle_tracker:
                        vehicle_tracker.update(
                            prediction["vehicle_key"],
                            cycle_id,
                            result.observed_at,
                            prediction["vehicle_lat"],
                            prediction["vehicle_lon"],
                            stop.stop_code,
                            prediction["line_number"],
                        )

                    # Deduplicated vehicle record
                    if prediction["vehicle_key"] in seen_vehicle_keys:
                        continue
                    seen_vehicle_keys.add(prediction["vehicle_key"])

                    vehicle_record = build_vehicle_record(
                        stop=stop,
                        result=result,
                        prediction=prediction,
                    )
                    if vehicles_writer:
                        vehicles_writer.write(vehicle_record)
                    if ch_vehicles:
                        ch_vehicles.write(vehicle_record)
                    unique_vehicles += 1

        # 6. Emit timetable-predicted records for unverified active trips
        observed_at_str = format_timestamp(started_at)
        for trip_id, info in trip_positions.items():
            if trip_id in verified_trip_ids:
                # Already verified via API
                state_manager.update_predicted(f"trip:{trip_id}")
                continue

            trip = info["trip"]
            pos = info["position"]

            # Look up stop info for the next stop
            next_stop = stop_by_id.get(pos.next_stop_id)
            next_stop_code = next_stop.stop_code if next_stop else str(pos.next_stop_id)
            next_stop_id = pos.next_stop_id

            vehicle_key = f"trip:{trip_id}"
            tracked = state_manager.update_predicted(vehicle_key)

            timetable_record = build_timetable_prediction_record(
                cycle_id=cycle_id,
                observed_at=observed_at_str,
                stop_id=next_stop_id,
                stop_code=next_stop_code,
                line_number=trip.route_short_name,
                direction="A" if trip.direction_id == 0 else "B" if trip.direction_id == 1 else None,
                trip_id=trip_id,
                vehicle_status=tracked.status.value,
                estimated_seconds_to_next=pos.estimated_seconds_to_next,
                prev_stop_id=pos.prev_stop_id,
                next_stop_id=pos.next_stop_id,
                fraction=pos.fraction,
                schedule_deviation_s=pos.schedule_deviation,
            )

            if predictions_writer:
                predictions_writer.write(timetable_record)
            if ch_predictions:
                ch_predictions.write(timetable_record)
            timetable_predictions_count += 1

    finally:
        if predictions_writer:
            predictions_writer.close()
        if vehicles_writer:
            vehicles_writer.close()
        if errors_writer:
            errors_writer.close()
        if ch_predictions:
            ch_predictions.close()
        if ch_vehicles:
            ch_vehicles.close()
        if ch_errors:
            ch_errors.close()

    # Get state stats
    stats = state_manager.get_stats()

    # Write cycle summary
    finished_at = datetime.now(timezone.utc)
    summary = CycleSummary(
        cycle_id=cycle_id,
        started_at=started_at,
        finished_at=finished_at,
        stops_total=len(stops),
        responses=response_count,
        errors=error_count,
        predictions=predictions_count + timetable_predictions_count,
        unique_vehicles=unique_vehicles,
        stops_polled=plan.total,
        stops_predicted=len(trip_positions) - len(verified_trip_ids),
        vehicles_verified=stats.get("verified", 0),
        vehicles_delayed=stats.get("delayed", 0),
        vehicles_stuck=stats.get("stuck", 0),
    )

    if config.jsonl_enabled:
        cycles_writer = JsonlWriter(output_paths["cycles"])
        cycles_writer.write(summary.as_record())
        cycles_writer.close()

    if ch_cycles:
        ch_cycles.write(summary.as_record())
        ch_cycles.close()

    return summary


def optimistic_collect_forever(stops: list[Stop], config: CollectorConfig) -> None:
    """
    Run optimistic collection cycles continuously.

    Builds timetable index at startup, then loops:
    1. Collect with optimistic predictions
    2. Run ETL processing
    3. Cleanup stale state
    4. Sleep until next cycle
    """
    # Build timetable index (one-time, ~10-15s for large datasets)
    LOG.info("Building timetable index from GTFS data...")
    timetable = TimetableIndex.build(
        calendar_csv=config.calendar_csv,
        calendar_dates_csv=config.calendar_dates_csv,
        routes_csv=config.routes_csv,
        trips_csv=config.trips_csv,
        stop_times_csv=config.stop_times_csv,
    )

    # Initialize checkpoint selector and poll scheduler
    checkpoint_selector = CheckpointSelector(
        timetable, stops, stride=config.checkpoint_stride,
    )
    poll_scheduler = PollScheduler(
        checkpoint_selector,
        stops,
        verification_batch_size=config.verification_batch_size,
        discovery_interval=config.discovery_interval_cycles,
    )

    # Initialize trip matcher and state manager
    trip_matcher = TripMatcher(
        timetable, deviation_threshold_s=config.deviation_threshold_s,
    )
    state_manager = VehicleStateManager(
        deviation_threshold_s=config.deviation_threshold_s,
        stuck_threshold_cycles=config.stuck_threshold_cycles,
        lost_threshold_cycles=config.lost_threshold_cycles,
    )

    # Initialize rate limiter
    rate_limiter = None
    if config.rate_limit_enabled:
        rate_limiter = TokenBucketRateLimiter(
            tokens_per_second=config.rate_limit_rps,
            bucket_capacity=int(config.rate_limit_rps * 2),
        )
        LOG.info("Rate limiter initialized: %.1f rps", config.rate_limit_rps)

    # Initialize vehicle tracker
    vehicle_tracker = None
    if config.vehicle_tracking_enabled:
        vehicle_tracker = VehicleTracker(ttl_cycles=config.vehicle_tracking_ttl_cycles)

    # ETL client
    processor_client = None
    if config.clickhouse_enabled:
        processor_client = ClickHouseClient(
            ClickHouseConfig(
                url=config.clickhouse_url,
                database=config.clickhouse_database,
                user=config.clickhouse_user,
                password=config.clickhouse_password,
                timeout_s=config.clickhouse_timeout_s,
            )
        )

    LOG.info(
        "Optimistic collection ready: %d checkpoints, %d total stops",
        len(checkpoint_selector.checkpoints),
        len(stops),
    )

    while True:
        started = time.monotonic()

        summary = optimistic_collect_once(
            stops=stops,
            config=config,
            timetable=timetable,
            poll_scheduler=poll_scheduler,
            trip_matcher=trip_matcher,
            state_manager=state_manager,
            rate_limiter=rate_limiter,
            vehicle_tracker=vehicle_tracker,
        )

        LOG.info(
            "cycle=%s stops_polled=%s stops_predicted=%s "
            "predictions=%s unique_vehicles=%s errors=%s "
            "verified=%s delayed=%s stuck=%s duration_s=%.2f",
            summary.cycle_id,
            summary.stops_polled,
            summary.stops_predicted,
            summary.predictions,
            summary.unique_vehicles,
            summary.errors,
            summary.vehicles_verified,
            summary.vehicles_delayed,
            summary.vehicles_stuck,
            (summary.finished_at - summary.started_at).total_seconds(),
        )

        # Run ETL
        if processor_client:
            try:
                process_results = process_cycle(processor_client)
                LOG.info(
                    "processed arrivals=%s eta_errors=%s",
                    process_results.get("arrivals", 0),
                    process_results.get("eta_errors", 0),
                )
            except Exception as exc:
                LOG.error("ETL processing failed: %s", exc)

        # Cleanup
        if vehicle_tracker:
            removed = vehicle_tracker.cleanup()
            if removed:
                LOG.debug("Removed %d stale vehicles from tracker", removed)

        lost = state_manager.cleanup_lost()
        if lost:
            LOG.debug("Removed %d lost vehicles from optimistic state", lost)

        if config.interval_s <= 0:
            break

        elapsed = time.monotonic() - started
        sleep_for = max(0.0, config.interval_s - elapsed)
        if sleep_for:
            time.sleep(sleep_for)
