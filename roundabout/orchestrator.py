"""Core orchestration logic for data collection cycles."""

from __future__ import annotations

import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

from roundabout.bgpp import fetch_stop
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
from roundabout.storage import JsonlWriter
from roundabout.transformers import (
    build_error_record,
    build_output_paths,
    build_prediction_record,
    build_vehicle_record,
)
from roundabout.processor import process_cycle
from roundabout.utils import format_timestamp

LOG = logging.getLogger(__name__)


def collect_once(stops: list[Stop], config: CollectorConfig) -> CycleSummary:
    """
    Execute a single collection cycle across all configured stops.

    This function performs one complete pass through all stops, fetching arrival
    predictions concurrently and writing results to both JSONL files and ClickHouse.

    The collection process:
    1. Initialize output files and database writers
    2. Fetch predictions for all stops concurrently
    3. Deduplicate vehicles within the cycle
    4. Write predictions, vehicles, and errors to storage
    5. Generate and store cycle summary

    Args:
        stops: List of stops to query.
        config: Collector configuration.

    Returns:
        CycleSummary with statistics about the collection cycle.

    Raises:
        Any exception during file I/O or critical failures will propagate.
        ClickHouse errors are logged but don't halt collection.
    """
    started_at = datetime.now(timezone.utc)
    cycle_id = started_at.strftime(CYCLE_ID_FORMAT)
    output_paths = build_output_paths(config.output_dir, cycle_id, started_at)

    # Initialize JSONL writers (only if enabled)
    predictions_writer = None
    vehicles_writer = None
    errors_writer = None
    if config.jsonl_enabled:
        predictions_writer = JsonlWriter(output_paths["predictions"])
        vehicles_writer = JsonlWriter(output_paths["vehicles"])
        errors_writer = JsonlWriter(output_paths["errors"])

    # Initialize ClickHouse writers if enabled
    clickhouse_client = None
    clickhouse_predictions = None
    clickhouse_vehicles = None
    clickhouse_errors = None
    clickhouse_cycles = None

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
        clickhouse_predictions = ClickHouseBatchWriter(
            clickhouse_client,
            CLICKHOUSE_TABLE_PREDICTIONS,
            batch_size=config.clickhouse_batch_size,
        )
        clickhouse_vehicles = ClickHouseBatchWriter(
            clickhouse_client,
            CLICKHOUSE_TABLE_VEHICLES,
            batch_size=config.clickhouse_batch_size,
        )
        clickhouse_errors = ClickHouseBatchWriter(
            clickhouse_client,
            CLICKHOUSE_TABLE_ERRORS,
            batch_size=config.clickhouse_batch_size,
        )
        clickhouse_cycles = ClickHouseBatchWriter(
            clickhouse_client,
            CLICKHOUSE_TABLE_CYCLES,
            batch_size=config.clickhouse_batch_size,
        )

    # Deduplication tracking for vehicles within this cycle
    seen_vehicle_keys: set[str] = set()
    predictions_count = 0
    unique_vehicles = 0
    errors = 0
    responses = 0

    try:
        # Fetch predictions concurrently
        with ThreadPoolExecutor(max_workers=config.concurrency) as executor:
            future_to_stop = {
                executor.submit(
                    fetch_stop,
                    stop.stop_code,
                    base_url=config.base_url,
                    timeout_s=config.timeout_s,
                    retries=config.retries,
                ): stop
                for stop in stops
            }

            for future in as_completed(future_to_stop):
                stop = future_to_stop[future]

                try:
                    result = future.result()
                except Exception as exc:  # pragma: no cover
                    # Unexpected exception from executor (not FetchResult error)
                    errors += 1
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
                    if clickhouse_errors:
                        clickhouse_errors.write(unexpected_record)
                    continue

                responses += 1

                # Handle API errors
                if result.error:
                    errors += 1
                    error_record = build_error_record(stop=stop, result=result, cycle_id=cycle_id)
                    if errors_writer:
                        errors_writer.write(error_record)
                    if clickhouse_errors:
                        clickhouse_errors.write(error_record)
                    continue

                # Process vehicle predictions
                vehicles = result.payload.get("vehicles") if isinstance(result.payload, dict) else None
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
                    if clickhouse_predictions:
                        clickhouse_predictions.write(prediction)
                    predictions_count += 1

                    # Write unique vehicle record on first occurrence
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
                    if clickhouse_vehicles:
                        clickhouse_vehicles.write(vehicle_record)
                    unique_vehicles += 1

    finally:
        # Ensure all writers are properly closed
        if predictions_writer:
            predictions_writer.close()
        if vehicles_writer:
            vehicles_writer.close()
        if errors_writer:
            errors_writer.close()

        if clickhouse_predictions:
            clickhouse_predictions.close()
        if clickhouse_vehicles:
            clickhouse_vehicles.close()
        if clickhouse_errors:
            clickhouse_errors.close()

    # Write cycle summary
    finished_at = datetime.now(timezone.utc)
    summary = CycleSummary(
        cycle_id=cycle_id,
        started_at=started_at,
        finished_at=finished_at,
        stops_total=len(stops),
        responses=responses,
        errors=errors,
        predictions=predictions_count,
        unique_vehicles=unique_vehicles,
    )

    if config.jsonl_enabled:
        cycles_writer = JsonlWriter(output_paths["cycles"])
        cycles_writer.write(summary.as_record())
        cycles_writer.close()

    if clickhouse_cycles:
        clickhouse_cycles.write(summary.as_record())
        clickhouse_cycles.close()

    return summary


def collect_forever(stops: list[Stop], config: CollectorConfig) -> None:
    """
    Run collection cycles continuously with configured interval.

    Executes collect_once repeatedly, sleeping between cycles to maintain
    the configured interval. If interval_s is 0, runs once and returns.

    The sleep duration is adjusted to account for collection time, ensuring
    cycles start at regular intervals rather than having fixed gaps between them.

    After each collection cycle, runs ETL processing to populate analytics tables.

    Args:
        stops: List of stops to query.
        config: Collector configuration with interval_s setting.

    Example:
        With interval_s=30:
        - Cycle 1: starts at 0s, takes 5s -> sleeps 25s
        - Cycle 2: starts at 30s, takes 4s -> sleeps 26s
        - Cycle 3: starts at 60s, ...
    """
    # Create a persistent ClickHouse client for ETL processing
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

    while True:
        started = time.monotonic()
        summary = collect_once(stops, config)

        LOG.info(
            "cycle=%s stops=%s predictions=%s unique_vehicles=%s errors=%s duration_s=%.2f",
            summary.cycle_id,
            summary.stops_total,
            summary.predictions,
            summary.unique_vehicles,
            summary.errors,
            (summary.finished_at - summary.started_at).total_seconds(),
        )

        # Run ETL processing to populate analytics tables
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

        # Exit after one cycle if interval is 0
        if config.interval_s <= 0:
            break

        # Sleep to maintain regular interval
        elapsed = time.monotonic() - started
        sleep_for = max(0.0, config.interval_s - elapsed)
        if sleep_for:
            time.sleep(sleep_for)
