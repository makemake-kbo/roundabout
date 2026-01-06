CREATE DATABASE IF NOT EXISTS roundabout;
USE roundabout;

CREATE TABLE IF NOT EXISTS raw_stop_predictions
(
    observed_at DateTime64(3, 'UTC'),
    cycle_id String,
    stop_id UInt32,
    stop_code String,
    api_stop_uid Nullable(UInt32),
    line_number LowCardinality(String),
    line_name LowCardinality(String),
    direction LowCardinality(String),
    seconds_left Nullable(Int32),
    predicted_arrival_at Nullable(DateTime64(3, 'UTC')),
    stations_between Nullable(UInt16),
    vehicle_id Nullable(String),
    vehicle_key String,
    vehicle_lat Nullable(Float64),
    vehicle_lon Nullable(Float64),
    observed_date Date MATERIALIZED toDate(observed_at),
    observed_hour UInt8 MATERIALIZED toHour(observed_at)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(observed_at)
ORDER BY (stop_code, observed_at, line_number, vehicle_key)
TTL observed_at + INTERVAL 1 MONTH DELETE;

CREATE TABLE IF NOT EXISTS raw_vehicles
(
    observed_at DateTime64(3, 'UTC'),
    cycle_id String,
    vehicle_id Nullable(String),
    vehicle_key String,
    line_number LowCardinality(String),
    line_name LowCardinality(String),
    direction LowCardinality(String),
    vehicle_lat Nullable(Float64),
    vehicle_lon Nullable(Float64),
    source_stop_id UInt32,
    source_stop_code String,
    seconds_left Nullable(Int32),
    stations_between Nullable(UInt16),
    observed_date Date MATERIALIZED toDate(observed_at),
    observed_hour UInt8 MATERIALIZED toHour(observed_at)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(observed_at)
ORDER BY (vehicle_key, observed_at)
TTL observed_at + INTERVAL 1 MONTH DELETE;

CREATE TABLE IF NOT EXISTS raw_errors
(
    observed_at DateTime64(3, 'UTC'),
    cycle_id String,
    stop_id UInt32,
    stop_code String,
    error String,
    http_status Nullable(UInt16),
    attempts UInt8,
    duration_ms UInt32,
    observed_date Date MATERIALIZED toDate(observed_at)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(observed_at)
ORDER BY (stop_code, observed_at)
TTL observed_at + INTERVAL 1 MONTH DELETE;

CREATE TABLE IF NOT EXISTS raw_cycles
(
    cycle_id String,
    started_at DateTime64(3, 'UTC'),
    finished_at DateTime64(3, 'UTC'),
    stops_total UInt32,
    responses UInt32,
    errors UInt32,
    predictions UInt32,
    unique_vehicles UInt32,
    started_date Date MATERIALIZED toDate(started_at),
    started_hour UInt8 MATERIALIZED toHour(started_at)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(started_at)
ORDER BY (started_at, cycle_id)
TTL started_at + INTERVAL 1 MONTH DELETE;

CREATE TABLE IF NOT EXISTS gtfs_stops
(
    stop_id UInt32,
    stop_code String,
    stop_name String,
    stop_lat Float64,
    stop_lon Float64,
    zone_id Nullable(String),
    location_type Nullable(UInt8),
    parent_station Nullable(String)
)
ENGINE = MergeTree
ORDER BY stop_id;

CREATE TABLE IF NOT EXISTS gtfs_routes
(
    route_id String,
    agency_id String,
    route_short_name String,
    route_long_name String,
    route_type UInt16,
    route_url Nullable(String),
    route_color Nullable(String),
    route_text_color Nullable(String),
    route_sort_order Nullable(UInt16),
    continuous_pickup Nullable(String),
    continuous_drop_off Nullable(String)
)
ENGINE = MergeTree
ORDER BY route_id;

CREATE TABLE IF NOT EXISTS gtfs_trips
(
    route_id String,
    service_id String,
    trip_id String,
    trip_headsign Nullable(String),
    trip_short_name Nullable(String),
    direction_id Nullable(UInt8),
    block_id Nullable(String),
    shape_id Nullable(String),
    wheelchair_accessible Nullable(UInt8)
)
ENGINE = MergeTree
ORDER BY (trip_id, route_id);

CREATE TABLE IF NOT EXISTS gtfs_stop_times
(
    trip_id String,
    arrival_time String,
    departure_time String,
    stop_id UInt32,
    stop_sequence UInt16,
    pickup_type Nullable(UInt8),
    drop_off_type Nullable(UInt8),
    timepoint Nullable(UInt8),
    arrival_seconds UInt32 MATERIALIZED
        toUInt32OrZero(arrayElement(splitByChar(':', arrival_time), 1)) * 3600 +
        toUInt32OrZero(arrayElement(splitByChar(':', arrival_time), 2)) * 60 +
        toUInt32OrZero(arrayElement(splitByChar(':', arrival_time), 3)),
    departure_seconds UInt32 MATERIALIZED
        toUInt32OrZero(arrayElement(splitByChar(':', departure_time), 1)) * 3600 +
        toUInt32OrZero(arrayElement(splitByChar(':', departure_time), 2)) * 60 +
        toUInt32OrZero(arrayElement(splitByChar(':', departure_time), 3))
)
ENGINE = MergeTree
ORDER BY (trip_id, stop_sequence);

CREATE TABLE IF NOT EXISTS gtfs_shapes
(
    shape_id String,
    shape_pt_lat Float64,
    shape_pt_lon Float64,
    shape_pt_sequence UInt32,
    shape_dist_traveled Nullable(Float64)
)
ENGINE = MergeTree
ORDER BY (shape_id, shape_pt_sequence);

CREATE TABLE IF NOT EXISTS arrivals
(
    vehicle_key String,
    vehicle_id Nullable(String),
    line_number LowCardinality(String),
    direction LowCardinality(String),
    stop_id UInt32,
    stop_code String,
    arrival_at DateTime64(3, 'UTC'),
    source_cycle_id String,
    source_observed_at DateTime64(3, 'UTC'),
    arrival_date Date MATERIALIZED toDate(arrival_at),
    arrival_hour UInt8 MATERIALIZED toHour(arrival_at)
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(arrival_at)
ORDER BY (stop_id, arrival_at, vehicle_key)
TTL arrival_at + INTERVAL 1 MONTH DELETE;

CREATE TABLE IF NOT EXISTS eta_errors
(
    observed_at DateTime64(3, 'UTC'),
    predicted_arrival_at DateTime64(3, 'UTC'),
    actual_arrival_at DateTime64(3, 'UTC'),
    stop_id UInt32,
    stop_code String,
    line_number LowCardinality(String),
    direction LowCardinality(String),
    vehicle_key String,
    error_seconds Int32,
    observed_date Date MATERIALIZED toDate(observed_at),
    observed_hour UInt8 MATERIALIZED toHour(observed_at)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(observed_at)
ORDER BY (line_number, observed_at, stop_id)
TTL observed_at + INTERVAL 1 MONTH DELETE;

CREATE TABLE IF NOT EXISTS headways
(
    line_number LowCardinality(String),
    direction LowCardinality(String),
    stop_id UInt32,
    stop_code String,
    observed_at DateTime64(3, 'UTC'),
    headway_seconds Int32,
    scheduled_headway_seconds Nullable(Int32),
    vehicle_key String,
    prev_vehicle_key String,
    observed_date Date MATERIALIZED toDate(observed_at),
    observed_hour UInt8 MATERIALIZED toHour(observed_at)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(observed_at)
ORDER BY (line_number, stop_id, observed_at)
TTL observed_at + INTERVAL 1 MONTH DELETE;

CREATE TABLE IF NOT EXISTS segment_delays
(
    line_number LowCardinality(String),
    direction LowCardinality(String),
    from_stop_id UInt32,
    to_stop_id UInt32,
    from_stop_code String,
    to_stop_code String,
    observed_at DateTime64(3, 'UTC'),
    actual_travel_seconds Int32,
    scheduled_travel_seconds Nullable(Int32),
    delay_seconds Int32,
    vehicle_key String,
    observed_date Date MATERIALIZED toDate(observed_at),
    observed_weekday UInt8 MATERIALIZED toDayOfWeek(observed_at)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(observed_at)
ORDER BY (line_number, from_stop_id, to_stop_id, observed_at)
TTL observed_at + INTERVAL 1 MONTH DELETE;

CREATE TABLE IF NOT EXISTS eta_error_hourly
(
    line_number LowCardinality(String),
    hour UInt8,
    day Date,
    count_state AggregateFunction(count, UInt64),
    avg_state AggregateFunction(avg, Float64),
    p95_state AggregateFunction(quantileTDigest, Float64)
)
ENGINE = AggregatingMergeTree
PARTITION BY toYYYYMM(day)
ORDER BY (line_number, day, hour)
TTL day + INTERVAL 1 MONTH DELETE;

-- Vehicle movements table for cross-cycle tracking
CREATE TABLE IF NOT EXISTS vehicle_movements
(
    observed_at DateTime64(3, 'UTC'),
    cycle_id String,
    vehicle_key String,
    vehicle_id Nullable(String),
    line_number LowCardinality(String),
    direction LowCardinality(String),
    current_stop_id UInt32,
    current_stop_code String,
    current_lat Nullable(Float64),
    current_lon Nullable(Float64),
    previous_cycle_id String,
    previous_stop_id Nullable(UInt32),
    previous_stop_code String,
    previous_lat Nullable(Float64),
    previous_lon Nullable(Float64),
    distance_km Nullable(Float64),
    stop_changed Bool,
    cycles_since_last_seen UInt8,
    observed_date Date MATERIALIZED toDate(observed_at),
    observed_hour UInt8 MATERIALIZED toHour(observed_at)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(observed_at)
ORDER BY (line_number, observed_at, vehicle_key)
TTL observed_at + INTERVAL 1 MONTH DELETE;

-- Add indexes for line-centric queries
ALTER TABLE raw_stop_predictions ADD INDEX IF NOT EXISTS idx_line_number line_number TYPE set(0) GRANULARITY 4;
ALTER TABLE raw_vehicles ADD INDEX IF NOT EXISTS idx_line_number line_number TYPE set(0) GRANULARITY 4;
