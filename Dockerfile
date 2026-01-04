# Multi-stage build for roundabout collector
FROM python:3.13-slim AS builder

# Install uv for faster dependency management
RUN pip install --no-cache-dir uv

WORKDIR /app

# Copy dependency files
COPY pyproject.toml uv.lock ./

# Install dependencies in a virtual environment
RUN uv venv /app/.venv && \
    . /app/.venv/bin/activate && \
    uv sync --frozen --no-dev

# Final stage
FROM python:3.13-slim

WORKDIR /app

# Copy virtual environment from builder
COPY --from=builder /app/.venv /app/.venv

# Copy application code
COPY roundabout/ ./roundabout/
COPY main.py ./
COPY stops-data/ ./stops-data/

# Create data directory for output
RUN mkdir -p /app/data/raw

# Set PATH to use virtual environment
ENV PATH="/app/.venv/bin:$PATH"

# Run as non-root user for security
RUN useradd --create-home --shell /bin/bash appuser && \
    chown -R appuser:appuser /app
USER appuser

# Default command - can be overridden
CMD ["python", "main.py", "--interval", "30"]
