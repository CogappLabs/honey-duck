# Production Dockerfile for honey-duck Dagster pipeline
FROM python:3.12-slim AS base

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    git \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Install uv for fast dependency management
RUN pip install uv

# Set working directory
WORKDIR /app

# Copy dependency files
COPY pyproject.toml .
COPY uv.lock* ./

# Install dependencies
RUN uv sync --no-dev

# Copy application code
COPY src/honey_duck src/honey_duck/
COPY cogapp_deps cogapp_deps/
COPY data/input data/input/
COPY scripts scripts/

# Create necessary directories
RUN mkdir -p /app/data/output/json \
    /app/data/output/storage \
    /app/data/output/dlt \
    /app/dagster_home

# Set Python path (includes src for honey_duck package)
ENV PYTHONPATH=/app:/app/src

# Production stage
FROM base AS production

# Set production environment variables
ENV DAGSTER_HOME=/app/dagster_home
ENV HONEY_DUCK_DB_PATH=/app/data/output/dagster.duckdb
ENV HONEY_DUCK_SALES_OUTPUT=/app/data/output/json/sales_output.json
ENV HONEY_DUCK_ARTWORKS_OUTPUT=/app/data/output/json/artworks_output.json

# Copy dagster configuration
COPY dagster.yaml /app/dagster_home/dagster.yaml
COPY workspace.yaml /app/workspace.yaml

# Expose Dagster webserver port
EXPOSE 3000

# Expose code location gRPC port
EXPOSE 4000

# Health check for code location gRPC server
HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
    CMD uv run dagster api grpc-health-check -p 4000 || exit 1

# Set DAGSTER_CURRENT_IMAGE for run launcher (set at build time or runtime)
ENV DAGSTER_CURRENT_IMAGE=""

# Default command runs the code location server
CMD ["uv", "run", "dagster", "code-server", "start", "-h", "0.0.0.0", "-p", "4000", "-m", "honey_duck.defs"]
