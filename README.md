# roundabout

`roundabout` is a service for performing data analytics on the Belgrade public transport system. It queries current bus, tram, and trolleybus data and stores it for downstream analysis. The goals are to infer:
- real timetables for every line and stop
- real ETA for every stop
- avg/p95 ETA error by line, by hour
- bunching events/heatmap per corridor
- top delayed segments per week/day

## Quickstart

Collect a single snapshot across all stops:

```bash
uv run python main.py
```

Collect repeatedly every 30 seconds:

```bash
uv run python main.py --interval 30
```

Target a few stops for a smoke test:

```bash
uv run python main.py --stop-code 1 --stop-code 2 --limit 5
```

## Data sources

- GTFS-style metadata in `stops-data/`.
- BG++ proxy for live arrivals:
  `GET https://bgpp.misa.st/api/stations/bg/search?id={stop_code}`

## Output

JSONL files are written under `data/raw/YYYY/MM/DD/`:

- `stop_predictions_{cycle_id}.jsonl`: per-stop arrival predictions.
- `vehicles_{cycle_id}.jsonl`: deduplicated vehicle snapshots for the cycle.
- `errors_{cycle_id}.jsonl`: request failures.
- `cycles.jsonl`: summary stats per collection cycle.

See `AGENTS.md` for the collection plan, schema notes, and metric definitions.

## ClickHouse

The collector also streams inserts to ClickHouse over HTTP by default.
Configuration options:

- `CLICKHOUSE_URL` (default: `http://localhost:8123`)
- `CLICKHOUSE_DB` (default: `roundabout`)
- `CLICKHOUSE_USER`
- `CLICKHOUSE_PASSWORD`
- `CLICKHOUSE_BATCH_SIZE` (default: `2000`)
- `CLICKHOUSE_TIMEOUT` (default: `10`)

Disable ClickHouse writes with `--no-clickhouse`.
