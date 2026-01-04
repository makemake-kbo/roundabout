# Roundabout

`roundabout` is a data collection and analytics service for the Belgrade public transport system. It queries real-time bus, tram, and trolleybus arrival predictions and stores them for downstream analysis.

## Goals

- Infer real timetables for every line and stop
- Calculate real ETAs with error metrics (avg/p95 by line and hour)
- Detect vehicle bunching events and generate heatmaps per corridor
- Identify top delayed segments by week/day

## Features

- **Real-time Data Collection**: Collects bus/tram/trolleybus predictions every 45 seconds
- **ClickHouse Storage**: High-performance time-series database for analytics
- **Grafana Dashboards**: Pre-configured dashboards with:
  - Vehicle location heatmap
  - On-time performance by transport type
  - Most late/on-time lines and stops
  - Problematic stops with high error rates
  - Large timetable deviations
- **Easy Management**: Makefile commands for common operations

## Quick Start

### Using Docker with Makefile (Recommended)

1. **Start all services** (ClickHouse + Collector + Grafana):
   ```bash
   make up
   ```

2. **View logs**:
   ```bash
   make logs              # All services
   make logs-collector    # Collector only
   make logs-grafana      # Grafana only
   ```

3. **Access Grafana dashboard**:
   - Open http://localhost:3000
   - Login: `admin` / `admin` (change on first login)
   - Dashboard: "Belgrade Public Transport Analytics"

4. **Stop services**:
   ```bash
   make down              # Stop and keep data
   make clean-all         # Stop and remove all data
   ```

5. **View all commands**:
   ```bash
   make help
   ```

### Using Docker Compose Directly

1. **Start all services** (ClickHouse + Collector + Grafana):
   ```bash
   docker-compose up -d
   ```

2. **View logs**:
   ```bash
   docker-compose logs -f collector
   ```

3. **Stop services**:
   ```bash
   docker-compose down         # Keep data volumes
   docker-compose down -v      # Remove data volumes
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

### Grafana Configuration

Grafana is accessible at http://localhost:3000 with default credentials `admin/admin`.

Optional environment variables:
- `GRAFANA_ADMIN_USER`: Admin username (default: admin)
- `GRAFANA_ADMIN_PASSWORD`: Admin password (default: admin)

The ClickHouse datasource and dashboard are automatically provisioned on startup.

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
