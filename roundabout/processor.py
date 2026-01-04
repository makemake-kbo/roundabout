"""ETL processor for deriving analytics tables from raw collection data.

This module processes raw prediction and vehicle data to:
1. Detect actual vehicle arrivals at stops
2. Calculate ETA prediction errors
3. Populate analytics tables (arrivals, eta_errors, etc.)
"""

from __future__ import annotations

import logging
from typing import Any

from roundabout.clickhouse import ClickHouseClient
from roundabout.utils import format_timestamp

LOG = logging.getLogger(__name__)


def process_arrivals(client: ClickHouseClient, lookback_minutes: int = 10) -> int:
    """
    Detect vehicle arrivals from raw predictions and write to arrivals table.

    Arrivals are detected when:
    - stations_between == 0 (vehicle is at the stop)
    - Vehicle has a valid predicted_arrival_at time

    Uses ReplacingMergeTree deduplication to handle multiple observations
    of the same arrival event.

    Args:
        client: ClickHouse client for queries and inserts.
        lookback_minutes: How many minutes of recent data to process.

    Returns:
        Number of arrival records written.
    """
    # Use client's database for table prefixing
    database = client._config.database

    query = f"""
    INSERT INTO {database}.arrivals (
        vehicle_key,
        vehicle_id,
        line_number,
        direction,
        stop_id,
        stop_code,
        arrival_at,
        source_cycle_id,
        source_observed_at
    )
    SELECT
        vehicle_key,
        vehicle_id,
        line_number,
        direction,
        stop_id,
        stop_code,
        predicted_arrival_at as arrival_at,
        cycle_id as source_cycle_id,
        observed_at as source_observed_at
    FROM {database}.raw_stop_predictions
    WHERE observed_at >= now() - INTERVAL {lookback_minutes} MINUTE
      AND stations_between = 0
      AND predicted_arrival_at IS NOT NULL
      AND vehicle_key != ''
    """

    try:
        result = client.execute(query)
        rows_written = result.get("rows_written", 0) if isinstance(result, dict) else 0
        rows_written = int(rows_written) if rows_written else 0
        LOG.info("Processed arrivals: rows_written=%d lookback_minutes=%d", rows_written, lookback_minutes)
        return rows_written
    except Exception as exc:
        LOG.error("Failed to process arrivals: %s", exc)
        return 0


def process_eta_errors(client: ClickHouseClient, lookback_minutes: int = 60) -> int:
    """
    Calculate ETA prediction errors by matching predictions to actual arrivals.

    For each arrival, finds the most recent prediction made for that
    vehicle+stop combination and calculates the error in seconds.

    Args:
        client: ClickHouse client for queries and inserts.
        lookback_minutes: How many minutes of arrival data to process.

    Returns:
        Number of error records written.
    """
    # Use client's database for table prefixing
    database = client._config.database

    query = f"""
    INSERT INTO {database}.eta_errors (
        observed_at,
        predicted_arrival_at,
        actual_arrival_at,
        stop_id,
        stop_code,
        line_number,
        direction,
        vehicle_key,
        error_seconds
    )
    SELECT
        p.observed_at,
        p.predicted_arrival_at,
        a.arrival_at as actual_arrival_at,
        a.stop_id,
        a.stop_code,
        a.line_number,
        a.direction,
        a.vehicle_key,
        toInt32(dateDiff('second', p.predicted_arrival_at, a.arrival_at)) as error_seconds
    FROM {database}.arrivals a
    ASOF LEFT JOIN {database}.raw_stop_predictions p
        ON a.vehicle_key = p.vehicle_key
        AND a.stop_id = p.stop_id
        AND p.observed_at <= a.arrival_at
    WHERE a.arrival_at >= now() - INTERVAL {lookback_minutes} MINUTE
      AND p.predicted_arrival_at IS NOT NULL
      AND p.observed_at IS NOT NULL
      AND abs(dateDiff('second', p.predicted_arrival_at, a.arrival_at)) < 3600
    """

    try:
        result = client.execute(query)
        rows_written = result.get("rows_written", 0) if isinstance(result, dict) else 0
        rows_written = int(rows_written) if rows_written else 0
        LOG.info("Processed ETA errors: rows_written=%d lookback_minutes=%d", rows_written, lookback_minutes)
        return rows_written
    except Exception as exc:
        LOG.error("Failed to process ETA errors: %s", exc)
        return 0


def process_cycle(client: ClickHouseClient) -> dict[str, int]:
    """
    Run a complete processing cycle for all analytics tables.

    This should be called after each data collection cycle to ensure
    analytics tables stay up to date.

    Args:
        client: ClickHouse client for queries and inserts.

    Returns:
        Dictionary with counts of records written to each table.
    """
    results = {}

    # Process arrivals first (required for ETA errors)
    results["arrivals"] = process_arrivals(client, lookback_minutes=10)

    # Process ETA errors (depends on arrivals)
    results["eta_errors"] = process_eta_errors(client, lookback_minutes=60)

    # Future: Add headways, segment_delays, etc.

    return results
