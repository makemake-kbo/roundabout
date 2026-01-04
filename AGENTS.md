# Agent Notes

This repo is for collecting and analyzing Belgrade public transport telemetry. The immediate goal is to capture enough raw data to compute:
- real timetables for every line and stop
- real ETA for every stop
- avg/p95 ETA error by line, by hour
- bunching events/heatmap per corridor
- top delayed segments per week/day

## Inputs

GTFS metadata lives under `stops-data/`:
- `stops.csv`: stop_id, stop_code, name, lat/lon
- `routes.csv`: line ids and short names
- `trips.csv` + `stop_times_*.csv`: scheduled ordering and timetable
- `shapes.csv`: corridor geometry for heatmaps

Live arrivals come from the BG++ proxy:

```
GET https://bgpp.misa.st/api/stations/bg/search?id={stop_code}
```

Observed response shape (fields can be missing):

```
{
  "city": "bg",
  "name": "...",
  "uid": 20001,
  "id": "1",
  "coords": ["44.82", "20.45"],
  "vehicles": [
    {
      "lineNumber": "5",
      "lineName": "...",
      "secondsLeft": 2230,
      "stationsBetween": 18,
      "direction": "...",
      "garageNo": "P80276",
      "coords": ["44.79", "20.51"]
    }
  ]
}
```

Notes:
- `stop_code` from `stops.csv` is the query id.
- `uid` matches `stop_id` in the GTFS dataset.
- `garageNo` (vehicle id) and `coords` are present in practice; treat them as optional.
- `direction` is not always provided.

## Collection flow

1. Load stops from `stops-data/stops.csv` (streaming).
2. For each stop_code, query BG++ and store per-stop predictions.
3. Deduplicate vehicles within a cycle and store a separate vehicle snapshot.

The collector lives in `roundabout/collector.py` and writes JSONL outputs under
`data/raw/YYYY/MM/DD/` for downstream analytics.

## Deduplication strategy

Goal: do not count the same vehicle multiple times when it appears in multiple
stop responses during the same polling cycle.

Current logic (see `_build_vehicle_key` in `roundabout/collector.py`):
- Primary key: `garageNo` (stable vehicle id).
- Fallback: hash of line_number + direction + rounded vehicle coords.
- If coords are missing, the stop_code is added to the hash to avoid collisions.

Limitations: without `garageNo`, dedupe is best-effort. For analytics that need
precise vehicle identity, prioritize `garageNo` and keep the raw per-stop data.

## Stored schema

`stop_predictions_{cycle_id}.jsonl`
- observed_at, cycle_id
- stop_id, stop_code, api_stop_uid
- line_number, line_name, direction
- seconds_left, predicted_arrival_at, stations_between
- vehicle_id, vehicle_key, vehicle_lat, vehicle_lon

`vehicles_{cycle_id}.jsonl` (deduped per cycle)
- observed_at, cycle_id
- vehicle_id, vehicle_key
- line_number, line_name, direction
- vehicle_lat, vehicle_lon
- source_stop_id, source_stop_code
- seconds_left, stations_between

`errors_{cycle_id}.jsonl`
- observed_at, cycle_id, stop_id, stop_code
- error, http_status, attempts, duration_ms

`cycles.jsonl`
- cycle_id, started_at, finished_at
- stops_total, responses, errors, predictions, unique_vehicles

## Analytics plan

Real timetables
- Track each vehicle_id + stop_id over time.
- Detect actual arrival when predicted ETA crosses zero or disappears after
  reaching a low threshold.
- Produce an `arrivals` table keyed by stop_id, line_number, direction.

Real ETA + ETA error
- Compare predicted_arrival_at from earlier snapshots with the eventual actual
  arrival for the same vehicle_id/stop_id.
- Aggregate mean and p95 error by line_number and hour of day.

Bunching events / corridor heatmap
- Use `stop_times_*.csv` to order stops per trip; group by line and direction.
- Compute observed headways (time between consecutive vehicles) at key stops.
- Flag bunching when observed headway is below a threshold or a fraction of
  scheduled headway.
- Use `shapes.csv` to map corridors for heatmap output.

Top delayed segments
- Build segments from ordered stop pairs in `stop_times_*.csv`.
- Estimate observed travel time between consecutive stops using arrival events.
- Compare to scheduled time deltas and aggregate by weekday/week.

## Next steps

- Add ClickHouse schema and ingestion scripts for JSONL outputs.
- Add an arrival-matching job to produce the `arrivals` table.
- Implement analytics queries for each metric.
- Add monitoring and retention policies for raw data.
