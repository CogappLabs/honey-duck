# Docker Quick Start Guide

Get honey-duck running in Docker containers in 5 minutes.

## Prerequisites

- Docker Desktop or Docker Engine installed
- 4GB+ RAM available
- 20GB+ disk space

Install Docker: https://docs.docker.com/get-docker/

## Quick Start

### 1. Development (Recommended for First Time)

```bash
# Start development environment with hot reload
make dev

# Or manually:
docker-compose -f docker-compose.dev.yml up -d
```

**What happens**:
- Builds Docker image with your code
- Starts single all-in-one Dagster container
- Code changes reload automatically (volumes mounted)
- Uses SQLite for simplicity

**Access**:
- Dagster UI: http://localhost:3000

### 2. Production (Full Stack)

```bash
# Build images
make build

# Start services
make up

# Or manually:
docker-compose build
docker-compose up -d
```

**What happens**:
- Builds production-optimized Docker images
- Starts 4 services:
  - dagster-postgres: Metadata storage
  - dagster-webserver: Web UI (port 3000)
  - dagster-daemon: Schedules and sensors
  - dagster-code-location: Pipeline code (gRPC)

**Access**:
- Dagster UI: http://localhost:3000

### 3. With Elasticsearch (Optional)

```bash
# Development with Elasticsearch
make dev-elastic

# Or production Elasticsearch (separate)
docker-compose -f docker-compose.elasticsearch.yml up -d
```

**Access**:
- Elasticsearch: http://localhost:9200
- Kibana: http://localhost:5601

---

## Common Commands

### Check Status

```bash
# Using Makefile
make ps

# Or directly
docker-compose ps

# Output shows:
# NAME                    STATUS    PORTS
# honey-duck-webserver    Up        0.0.0.0:3000->3000/tcp
# honey-duck-daemon       Up
# honey-duck-postgres     Up
```

### View Logs

```bash
# All services
make logs

# Specific service
docker-compose logs -f dagster-webserver
docker-compose logs -f dagster-daemon

# Last 100 lines
docker-compose logs --tail=100
```

### Stop Services

```bash
# Stop (containers persist)
make down
docker-compose down

# Stop and remove volumes (fresh start)
docker-compose down -v
```

### Restart Services

```bash
make restart
docker-compose restart

# Or restart specific service
docker-compose restart dagster-webserver
```

### Execute Commands in Container

```bash
# Open shell
make shell
docker-compose exec dagster-code-location /bin/bash

# Run tests
docker-compose exec dagster-code-location uv run pytest

# Run specific job
docker-compose exec dagster-code-location uv run dg launch --job polars_pipeline
```

---

## Health Checks

### Check All Services

```bash
make health
```

### Manual Checks

```bash
# Webserver
curl http://localhost:3000/server_info

# PostgreSQL
docker-compose exec dagster-postgres pg_isready -U dagster

# Container stats
docker stats
```

---

## Data Persistence

Data is stored in Docker volumes and persists across container restarts:

```bash
# List volumes
docker volume ls | grep honey-duck

# Inspect volume
docker volume inspect honey-duck_postgres-data
docker volume inspect honey-duck_data-output

# Backup volumes
make backup

# Location of data:
# - postgres-data: Run history, events, schedules
# - dagster-home: Compute logs, artifacts
# - data-output: Pipeline outputs (JSON, Parquet, DuckDB)
```

---

## Troubleshooting

### Port Already in Use

```bash
# Check what's using port 3000
lsof -i :3000

# Kill process
kill -9 <PID>

# Or change port in docker-compose.yml
ports:
  - "3001:3000"  # Use 3001 instead
```

### Build Errors

```bash
# Clean build (no cache)
docker-compose build --no-cache

# Remove old images
docker system prune -a

# Rebuild from scratch
make clean
make build
```

### Container Won't Start

```bash
# Check logs
docker-compose logs <service-name>

# Check container status
docker ps -a

# Remove and recreate
docker-compose down
docker-compose up -d
```

### Database Connection Issues

```bash
# Check PostgreSQL is ready
docker-compose exec dagster-postgres pg_isready

# Restart database
docker-compose restart dagster-postgres

# Check environment variables
docker-compose config
```

### Code Changes Not Reflecting

**Development mode**:
- Code is mounted as volume, changes should reload automatically
- If not, restart: `docker-compose -f docker-compose.dev.yml restart`

**Production mode**:
- Code is baked into image, must rebuild:
  ```bash
  docker-compose build dagster-code-location
  docker-compose up -d dagster-code-location
  ```

---

## Environment Variables

Create `.env` file from template:

```bash
cp .env.example .env

# Edit with your values
nano .env
```

**Required for production**:
```bash
DAGSTER_POSTGRES_PASSWORD=your_secure_password_here
```

**Optional integrations**:
```bash
ELASTICSEARCH_HOST=http://localhost:9200
ANTHROPIC_API_KEY=sk-ant-...
VOYAGE_API_KEY=pa-...
```

---

## Performance Tips

### Allocate More Resources

**Docker Desktop**:
- Settings → Resources
- Increase CPU: 4+ cores
- Increase Memory: 8GB+

### Limit Concurrent Runs

Edit `dagster.yaml`:
```yaml
run_coordinator:
  config:
    max_concurrent_runs: 5  # Reduce from 10
```

### Scale Code Location

```bash
# Run 3 instances
docker-compose up -d --scale dagster-code-location=3
```

---

## Updating

### Update Dagster Version

1. Edit `pyproject.toml`:
   ```toml
   dagster>=1.13  # New version
   ```

2. Rebuild images:
   ```bash
   make build
   make up
   ```

### Update Python Dependencies

```bash
# Rebuild with new dependencies
docker-compose build --no-cache
docker-compose up -d
```

---

## Cleanup

### Remove Everything

```bash
# Stop and remove containers, volumes, networks
make clean

# Or manually
docker-compose down -v
docker system prune -a
```

### Remove Old Images

```bash
# Remove unused images
docker image prune -a

# Remove specific image
docker rmi honey-duck_dagster-webserver
```

---

## Advanced Usage

### Custom Dockerfile

Edit `Dockerfile` to customize:
- Base image
- System dependencies
- Python packages
- Environment variables

### Custom docker-compose

Edit `docker-compose.yml` to customize:
- Port mappings
- Resource limits
- Additional services (Redis, etc.)
- Network configuration

### Deploy to Cloud

See `docs/development/deployment.md` for:
- AWS ECS/Fargate deployment
- Google Cloud Run deployment
- Kubernetes with Helm charts
- Docker Swarm deployment

---

## Resources

- **Full Deployment Guide**: [docs/development/deployment.md](docs/development/deployment.md)
- **Dagster Docs**: https://docs.dagster.io/
- **Docker Docs**: https://docs.docker.com/
- **Troubleshooting**: [docs/user-guide/troubleshooting.md](docs/user-guide/troubleshooting.md)

---

## Quick Reference

```bash
# Development
make dev              # Start dev environment
make dev-logs         # View logs
make dev-down         # Stop dev environment

# Production
make build            # Build images
make up               # Start services
make ps               # Check status
make logs             # View logs
make restart          # Restart services
make down             # Stop services

# Maintenance
make backup           # Backup database
make clean            # Remove everything
make health           # Check service health
make shell            # Open shell in container

# Monitoring
make stats            # Container resource usage
docker-compose logs -f  # Follow all logs
```

---

**Happy Dockering!** 🐳
