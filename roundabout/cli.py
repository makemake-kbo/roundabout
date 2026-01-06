"""Command-line interface and argument parsing."""

from __future__ import annotations

import argparse
import logging
import os
from pathlib import Path
from typing import Iterable

from roundabout.config import CollectorConfig
from roundabout.constants import (
    DEFAULT_BASE_URL,
    DEFAULT_BBOX_MAX_LAT,
    DEFAULT_BBOX_MAX_LON,
    DEFAULT_BBOX_MIN_LAT,
    DEFAULT_BBOX_MIN_LON,
    DEFAULT_CLICKHOUSE_BATCH_SIZE,
    DEFAULT_CLICKHOUSE_DATABASE,
    DEFAULT_CLICKHOUSE_TIMEOUT_S,
    DEFAULT_CLICKHOUSE_URL,
    DEFAULT_CONCURRENCY,
    DEFAULT_HTTP_RETRIES,
    DEFAULT_HTTP_TIMEOUT_S,
    DEFAULT_INTERVAL_S,
    DEFAULT_LOG_LEVEL,
    DEFAULT_OUTPUT_DIR,
    DEFAULT_RATE_LIMIT_ENABLED,
    DEFAULT_RATE_LIMIT_RPS,
    DEFAULT_ROUTES_CSV,
    DEFAULT_STOP_TIMES_CSV,
    DEFAULT_STOPS_CSV,
    DEFAULT_TRIPS_CSV,
    DEFAULT_VEHICLE_TRACKING_ENABLED,
    DEFAULT_VEHICLE_TRACKING_TTL_CYCLES,
    MIN_BATCH_SIZE,
    MIN_CLICKHOUSE_TIMEOUT_S,
    MIN_CONCURRENCY,
    MIN_INTERVAL_S,
    MIN_RATE_LIMIT_RPS,
    MIN_RETRIES,
)


def parse_stop_codes(values: Iterable[str]) -> set[str]:
    """
    Parse stop codes from command-line arguments.

    Supports comma-separated values and multiple --stop-code arguments.
    Whitespace is trimmed from each code.

    Args:
        values: Iterable of stop code strings (may contain commas).

    Returns:
        Set of unique stop codes with whitespace stripped.

    Examples:
        >>> parse_stop_codes(["1001", "1002"])
        {'1001', '1002'}
        >>> parse_stop_codes(["1001, 1002, 1003"])
        {'1001', '1002', '1003'}
        >>> parse_stop_codes(["1001, 1002", "1003"])
        {'1001', '1002', '1003'}
    """
    stop_codes: set[str] = set()
    for value in values:
        for part in value.split(","):
            trimmed = part.strip()
            if trimmed:
                stop_codes.add(trimmed)
    return stop_codes


def parse_route_names(values: Iterable[str]) -> set[str]:
    """
    Parse route short names from command-line arguments.

    Supports comma-separated values and multiple --route arguments.
    Whitespace is trimmed from each name.

    Args:
        values: Iterable of route name strings (may contain commas).

    Returns:
        Set of unique route short names with whitespace stripped.

    Examples:
        >>> parse_route_names(["7", "84"])
        {'7', '84'}
        >>> parse_route_names(["7, 84, E2"])
        {'7', '84', 'E2'}
        >>> parse_route_names(["7, 84", "E2"])
        {'7', '84', 'E2'}
    """
    route_names: set[str] = set()
    for value in values:
        for part in value.split(","):
            trimmed = part.strip()
            if trimmed:
                route_names.add(trimmed)
    return route_names


def parse_args(argv: list[str] | None = None) -> CollectorConfig:
    """
    Parse command-line arguments and environment variables into configuration.

    Arguments can come from CLI flags or environment variables. CLI flags take
    precedence over environment variables where both are supported.

    Args:
        argv: Command-line arguments (default: sys.argv).

    Returns:
        Parsed and validated CollectorConfig.

    Environment Variables:
        CLICKHOUSE_URL: ClickHouse HTTP interface URL
        CLICKHOUSE_DB: ClickHouse database name
        CLICKHOUSE_USER: ClickHouse username
        CLICKHOUSE_PASSWORD: ClickHouse password
        CLICKHOUSE_BATCH_SIZE: Number of records to batch
        CLICKHOUSE_TIMEOUT: Request timeout in seconds
    """
    parser = argparse.ArgumentParser(
        description="Collect BG++ stop predictions.",
        epilog="Environment variables: CLICKHOUSE_URL, CLICKHOUSE_DB, "
        "CLICKHOUSE_USER, CLICKHOUSE_PASSWORD, CLICKHOUSE_BATCH_SIZE, CLICKHOUSE_TIMEOUT",
    )

    # Input/Output
    parser.add_argument(
        "--stops-csv",
        default=DEFAULT_STOPS_CSV,
        help=f"Path to GTFS stops.csv file (default: {DEFAULT_STOPS_CSV})",
    )
    parser.add_argument(
        "--routes-csv",
        default=DEFAULT_ROUTES_CSV,
        help=f"Path to GTFS routes.csv file (default: {DEFAULT_ROUTES_CSV})",
    )
    parser.add_argument(
        "--trips-csv",
        default=DEFAULT_TRIPS_CSV,
        help=f"Path to GTFS trips.csv file (default: {DEFAULT_TRIPS_CSV})",
    )
    parser.add_argument(
        "--stop-times-csv",
        default=DEFAULT_STOP_TIMES_CSV,
        help=f"Path to GTFS stop_times CSV file (default: {DEFAULT_STOP_TIMES_CSV})",
    )
    parser.add_argument(
        "--output-dir",
        default=DEFAULT_OUTPUT_DIR,
        help=f"Output directory for JSONL files (default: {DEFAULT_OUTPUT_DIR})",
    )

    # API Configuration
    parser.add_argument(
        "--base-url",
        default=DEFAULT_BASE_URL,
        help="BG++ API base URL",
    )
    parser.add_argument(
        "--concurrency",
        type=int,
        default=DEFAULT_CONCURRENCY,
        help=f"Max concurrent requests (default: {DEFAULT_CONCURRENCY})",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=DEFAULT_HTTP_TIMEOUT_S,
        help=f"HTTP request timeout in seconds (default: {DEFAULT_HTTP_TIMEOUT_S})",
    )
    parser.add_argument(
        "--retries",
        type=int,
        default=DEFAULT_HTTP_RETRIES,
        help=f"Number of retry attempts (default: {DEFAULT_HTTP_RETRIES})",
    )

    # Collection Behavior
    parser.add_argument(
        "--interval",
        type=float,
        default=DEFAULT_INTERVAL_S,
        help=f"Seconds between cycles, 0 for single run (default: {DEFAULT_INTERVAL_S})",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Maximum number of stops to process (default: all)",
    )
    parser.add_argument(
        "--stop-code",
        action="append",
        default=[],
        help="Specific stop code(s) to process (repeatable, comma-separated)",
    )
    parser.add_argument(
        "--route",
        action="append",
        default=[],
        help="Priority route short name(s) to collect (repeatable, comma-separated)",
    )
    parser.add_argument(
        "--shuffle",
        action="store_true",
        help="Randomize stop processing order",
    )

    # Logging
    parser.add_argument(
        "--log-level",
        default=DEFAULT_LOG_LEVEL,
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help=f"Logging level (default: {DEFAULT_LOG_LEVEL})",
    )

    # Output Configuration
    parser.add_argument(
        "--jsonl",
        action="store_true",
        help="Enable JSONL file output (default: disabled)",
    )
    parser.add_argument(
        "--no-clickhouse",
        action="store_true",
        help="Disable ClickHouse writes",
    )

    # ClickHouse Configuration
    parser.add_argument(
        "--clickhouse-url",
        default=os.getenv("CLICKHOUSE_URL", DEFAULT_CLICKHOUSE_URL),
        help=f"ClickHouse HTTP interface URL (default: {DEFAULT_CLICKHOUSE_URL})",
    )
    parser.add_argument(
        "--clickhouse-database",
        default=os.getenv("CLICKHOUSE_DB", DEFAULT_CLICKHOUSE_DATABASE),
        help=f"ClickHouse database name (default: {DEFAULT_CLICKHOUSE_DATABASE})",
    )
    parser.add_argument(
        "--clickhouse-user",
        default=os.getenv("CLICKHOUSE_USER"),
        help="ClickHouse username (default: from CLICKHOUSE_USER env)",
    )
    parser.add_argument(
        "--clickhouse-password",
        default=os.getenv("CLICKHOUSE_PASSWORD"),
        help="ClickHouse password (default: from CLICKHOUSE_PASSWORD env)",
    )
    parser.add_argument(
        "--clickhouse-batch-size",
        type=int,
        default=int(os.getenv("CLICKHOUSE_BATCH_SIZE", str(DEFAULT_CLICKHOUSE_BATCH_SIZE))),
        help=f"ClickHouse batch size (default: {DEFAULT_CLICKHOUSE_BATCH_SIZE})",
    )
    parser.add_argument(
        "--clickhouse-timeout",
        type=float,
        default=float(os.getenv("CLICKHOUSE_TIMEOUT", str(DEFAULT_CLICKHOUSE_TIMEOUT_S))),
        help=f"ClickHouse timeout in seconds (default: {DEFAULT_CLICKHOUSE_TIMEOUT_S})",
    )

    # Geographic Bounding Box
    bbox_group = parser.add_argument_group("geographic bounding box")
    bbox_group.add_argument(
        "--bbox-min-lat",
        type=float,
        default=float(os.getenv("BBOX_MIN_LAT")) if os.getenv("BBOX_MIN_LAT") else None,
        help=f"Minimum latitude (default: {DEFAULT_BBOX_MIN_LAT} if any bbox arg set, from BBOX_MIN_LAT env)",
    )
    bbox_group.add_argument(
        "--bbox-max-lat",
        type=float,
        default=float(os.getenv("BBOX_MAX_LAT")) if os.getenv("BBOX_MAX_LAT") else None,
        help=f"Maximum latitude (default: {DEFAULT_BBOX_MAX_LAT} if any bbox arg set, from BBOX_MAX_LAT env)",
    )
    bbox_group.add_argument(
        "--bbox-min-lon",
        type=float,
        default=float(os.getenv("BBOX_MIN_LON")) if os.getenv("BBOX_MIN_LON") else None,
        help=f"Minimum longitude (default: {DEFAULT_BBOX_MIN_LON} if any bbox arg set, from BBOX_MIN_LON env)",
    )
    bbox_group.add_argument(
        "--bbox-max-lon",
        type=float,
        default=float(os.getenv("BBOX_MAX_LON")) if os.getenv("BBOX_MAX_LON") else None,
        help=f"Maximum longitude (default: {DEFAULT_BBOX_MAX_LON} if any bbox arg set, from BBOX_MAX_LON env)",
    )

    # Rate Limiting
    rate_group = parser.add_argument_group("rate limiting")
    rate_group.add_argument(
        "--rate-limit",
        type=float,
        default=DEFAULT_RATE_LIMIT_RPS,
        help=f"Requests per second limit (default: {DEFAULT_RATE_LIMIT_RPS})",
    )
    rate_group.add_argument(
        "--no-rate-limit",
        action="store_true",
        help="Disable rate limiting",
    )

    # Vehicle Tracking
    tracking_group = parser.add_argument_group("cross-cycle vehicle tracking")
    tracking_group.add_argument(
        "--vehicle-tracking",
        action="store_true",
        default=DEFAULT_VEHICLE_TRACKING_ENABLED,
        help=f"Enable cross-cycle vehicle tracking (default: {DEFAULT_VEHICLE_TRACKING_ENABLED})",
    )
    tracking_group.add_argument(
        "--no-vehicle-tracking",
        action="store_true",
        help="Disable cross-cycle vehicle tracking",
    )
    tracking_group.add_argument(
        "--tracking-ttl-cycles",
        type=int,
        default=DEFAULT_VEHICLE_TRACKING_TTL_CYCLES,
        help=f"Number of cycles to track vehicles (default: {DEFAULT_VEHICLE_TRACKING_TTL_CYCLES})",
    )

    args = parser.parse_args(argv)

    # Configure logging
    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(message)s",
    )

    # Parse and validate stop codes
    stop_codes = parse_stop_codes(args.stop_code)
    if not stop_codes:
        stop_codes = None

    # Parse and validate route names from CLI args and environment
    route_args = args.route if args.route else []
    # If no CLI args provided, check environment variable
    if not route_args and os.getenv("PRIORITY_ROUTES"):
        route_args = [os.getenv("PRIORITY_ROUTES")]
    route_short_names = parse_route_names(route_args)
    if not route_short_names:
        route_short_names = None

    # Handle bounding box: if any are set, use defaults for unset values
    bbox_set = any(
        [
            args.bbox_min_lat is not None,
            args.bbox_max_lat is not None,
            args.bbox_min_lon is not None,
            args.bbox_max_lon is not None,
        ]
    )
    bbox_min_lat = args.bbox_min_lat if args.bbox_min_lat is not None else (DEFAULT_BBOX_MIN_LAT if bbox_set else None)
    bbox_max_lat = args.bbox_max_lat if args.bbox_max_lat is not None else (DEFAULT_BBOX_MAX_LAT if bbox_set else None)
    bbox_min_lon = args.bbox_min_lon if args.bbox_min_lon is not None else (DEFAULT_BBOX_MIN_LON if bbox_set else None)
    bbox_max_lon = args.bbox_max_lon if args.bbox_max_lon is not None else (DEFAULT_BBOX_MAX_LON if bbox_set else None)

    # Rate limiting
    rate_limit_enabled = not args.no_rate_limit and DEFAULT_RATE_LIMIT_ENABLED
    rate_limit_rps = max(MIN_RATE_LIMIT_RPS, args.rate_limit)

    # Vehicle tracking
    vehicle_tracking_enabled = not args.no_vehicle_tracking and DEFAULT_VEHICLE_TRACKING_ENABLED

    # Build and validate configuration
    return CollectorConfig(
        base_url=args.base_url,
        stops_csv=Path(args.stops_csv),
        routes_csv=Path(args.routes_csv),
        trips_csv=Path(args.trips_csv),
        stop_times_csv=Path(args.stop_times_csv),
        output_dir=Path(args.output_dir),
        concurrency=max(MIN_CONCURRENCY, args.concurrency),
        timeout_s=args.timeout,
        retries=max(MIN_RETRIES, args.retries),
        interval_s=max(MIN_INTERVAL_S, args.interval),
        limit=args.limit,
        stop_codes=stop_codes,
        route_short_names=route_short_names,
        shuffle=args.shuffle,
        jsonl_enabled=args.jsonl,
        clickhouse_enabled=not args.no_clickhouse,
        clickhouse_url=args.clickhouse_url,
        clickhouse_database=args.clickhouse_database,
        clickhouse_user=args.clickhouse_user,
        clickhouse_password=args.clickhouse_password,
        clickhouse_batch_size=max(MIN_BATCH_SIZE, args.clickhouse_batch_size),
        clickhouse_timeout_s=max(MIN_CLICKHOUSE_TIMEOUT_S, args.clickhouse_timeout),
        bbox_min_lat=bbox_min_lat,
        bbox_max_lat=bbox_max_lat,
        bbox_min_lon=bbox_min_lon,
        bbox_max_lon=bbox_max_lon,
        rate_limit_rps=rate_limit_rps,
        rate_limit_enabled=rate_limit_enabled,
        vehicle_tracking_enabled=vehicle_tracking_enabled,
        vehicle_tracking_ttl_cycles=args.tracking_ttl_cycles,
    )
