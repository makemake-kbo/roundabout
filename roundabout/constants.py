"""Constants used throughout the roundabout application."""

from __future__ import annotations

# HTTP and API Configuration
DEFAULT_BASE_URL = "http://bgpp:3000/api/stations/bg/search"
DEFAULT_USER_AGENT = "roundabout/0.1"
DEFAULT_HTTP_TIMEOUT_S = 10.0
DEFAULT_HTTP_RETRIES = 2

# Retry Configuration
RETRY_BASE_DELAY_S = 0.25
"""Base delay in seconds for exponential backoff between retries."""

# Coordinate Precision
COORDINATE_DECIMAL_PLACES = 5
"""Number of decimal places for lat/lon rounding (approx 1.1m precision)."""

# Vehicle Key Generation
VEHICLE_KEY_PREFIX_GARAGE = "garage"
"""Prefix for vehicle keys when garage number is available."""

VEHICLE_KEY_PREFIX_HASH = "hash"
"""Prefix for vehicle keys when falling back to hash-based deduplication."""

VEHICLE_KEY_HASH_LENGTH = 16
"""Length of SHA256 hash digest used in vehicle key generation."""

# Default CLI Arguments
DEFAULT_STOPS_CSV = "stops-data/stops.csv"
DEFAULT_OUTPUT_DIR = "data/raw"
DEFAULT_CONCURRENCY = 10
DEFAULT_INTERVAL_S = 0.0
DEFAULT_LOG_LEVEL = "INFO"

# ClickHouse Configuration
DEFAULT_CLICKHOUSE_URL = "http://localhost:8123"
DEFAULT_CLICKHOUSE_DATABASE = "roundabout"
DEFAULT_CLICKHOUSE_BATCH_SIZE = 2000
DEFAULT_CLICKHOUSE_TIMEOUT_S = 10.0

# ClickHouse Table Names
CLICKHOUSE_TABLE_PREDICTIONS = "raw_stop_predictions"
CLICKHOUSE_TABLE_VEHICLES = "raw_vehicles"
CLICKHOUSE_TABLE_ERRORS = "raw_errors"
CLICKHOUSE_TABLE_CYCLES = "raw_cycles"

# Date/Time Formats
CYCLE_ID_FORMAT = "%Y%m%dT%H%M%SZ"
"""Format string for cycle ID generation from datetime."""

OUTPUT_DATE_PREFIX_FORMAT = "%Y/%m/%d"
"""Format string for output directory date prefix."""

ISO_TIMESTAMP_TIMESPEC = "milliseconds"
"""Timespec for ISO 8601 timestamp formatting."""

# Minimum Values for Configuration Validation
MIN_CONCURRENCY = 1
MIN_RETRIES = 0
MIN_INTERVAL_S = 0.0
MIN_BATCH_SIZE = 1
MIN_CLICKHOUSE_TIMEOUT_S = 1.0
