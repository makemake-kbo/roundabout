.PHONY: help up down restart logs clean clean-all stop ps

# Default target
help:
	@echo "Roundabout Docker Compose Management"
	@echo ""
	@echo "Available commands:"
	@echo "  make up              - Start all services (with persistent volumes)"
	@echo "  make down            - Stop all services (keep volumes)"
	@echo "  make restart         - Restart all services"
	@echo "  make stop            - Stop all services without removing containers"
	@echo "  make logs            - Follow logs for all services"
	@echo "  make logs-collector  - Follow collector logs only"
	@echo "  make logs-clickhouse - Follow ClickHouse logs only"
	@echo "  make logs-grafana    - Follow Grafana logs only"
	@echo "  make ps              - Show running containers"
	@echo "  make clean           - Stop and remove containers (keep volumes)"
	@echo "  make clean-all       - Stop and remove containers AND volumes (no persistence)"
	@echo ""
	@echo "Quick start:"
	@echo "  1. Start services:    make up"
	@echo "  2. View logs:         make logs"
	@echo "  3. Open Grafana:      http://localhost:3000 (admin/admin)"
	@echo "  4. Stop services:     make down"
	@echo ""

# Start services with persistent volumes
up:
	docker compose up -d
	@echo ""
	@echo "Services started!"
	@echo "  - ClickHouse:  http://localhost:8123"
	@echo "  - Grafana:     http://localhost:3000 (admin/admin)"
	@echo ""
	@echo "Run 'make logs' to view logs"

# Stop services but keep volumes (data persists)
down:
	docker compose down
	@echo "Services stopped. Data volumes preserved."
	@echo "Run 'make up' to restart with existing data."

# Stop services without removing containers
stop:
	docker compose stop
	@echo "Services stopped. Run 'docker-compose start' or 'make restart' to resume."

# Restart all services
restart:
	docker compose restart
	@echo "Services restarted."

# Remove containers and volumes (no persistence)
clean-all:
	docker compose down -v
	@echo "All containers and volumes removed."
	@echo "Next 'make up' will start fresh."

# Remove containers but keep volumes
clean:
	docker compose down
	@echo "Containers removed, volumes preserved."

# Show container status
ps:
	docker compose ps

# Follow logs for all services
logs:
	dockercompose logs -f

# Follow logs for specific services
logs-collector:
	docker compose logs -f collector

logs-clickhouse:
	docker compose logs -f clickhouse

logs-grafana:
	docker compose logs -f grafana

# Build images (useful after code changes)
build:
	docker compose build
	@echo "Images rebuilt. Run 'make up' to start with new images."

# Rebuild and restart
rebuild: build
	docker compose up -d --force-recreate
	@echo "Services rebuilt and restarted."
