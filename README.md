# Roundabout

`roundabout` is a data collection and analytics service for the Belgrade public transport system. It queries real-time bus, tram, and trolleybus arrival predictions and stores them for downstream analysis.

## Goals

- Infer real timetables for every line and stop
- Calculate real ETAs with error metrics (avg/p95 by line and hour)
- Detect vehicle bunching events and generate heatmaps per corridor
- Identify top delayed segments by week/day

## Quick Start

### Using Docker (Recommended)

1. **Start all services** (ClickHouse + Collector):
   ```bash
   docker-compose up -d
   ```

2. **View logs**:
   ```bash
   docker-compose logs -f collector
   ```

3. **Stop services**:
   ```bash
   docker-compose down
   ```

### Using Python Directly

1. **Install dependencies**:
   ```bash
   uv sync
   ```

2. **Start ClickHouse** (optional, for database storage):
   ```bash
   docker-compose up -d clickhouse
   ```

3. **Load ClickHouse schema** (first time only):
   ```bash
   docker exec -i roundabout-clickhouse clickhouse-client < schema/clickhouse.sql
   ```

4. **Run collector**:
   ```bash
   # Single collection cycle
   uv run python main.py

   # Continuous collection every 30 seconds
   uv run python main.py --interval 30

   # Without ClickHouse (JSONL only)
   uv run python main.py --no-clickhouse

   # Limited to specific stops
   uv run python main.py --stop-code 1001 --stop-code 1002 --limit 10
   ```

---

## Configuration

### Environment Variables

Copy `.env.example` to `.env` and customize:

```bash
cp .env.example .env
```

Key settings:
- `COLLECTION_INTERVAL`: Seconds between cycles (default: 30)
- `CONCURRENCY`: Max concurrent API requests (default: 10)
- `CLICKHOUSE_URL`: ClickHouse HTTP interface (default: http://localhost:8123)
- `CLICKHOUSE_DB`: Database name (default: roundabout)
- `CLICKHOUSE_BATCH_SIZE`: Records per batch (default: 2000)

See `.env.example` for full documentation.

### Command-Line Arguments

```bash
uv run python main.py --help
```

Common options:
- `--interval 30` - Run every 30 seconds (0 = single run)
- `--concurrency 20` - Use 20 concurrent workers
- `--timeout 15` - HTTP timeout in seconds
- `--retries 3` - Retry failed requests 3 times
- `--shuffle` - Randomize stop processing order
- `--limit 100` - Process only first 100 stops
- `--stop-code 1001` - Process specific stop(s)
- `--no-clickhouse` - Disable database writes
- `--log-level DEBUG` - Set logging level

---

## Contributing

Contributions welcome! Please ensure:
1. All tests pass (`uv run pytest`)
2. New features include tests and docstrings
3. Code follows existing patterns and style
