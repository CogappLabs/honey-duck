# Production Deployment Guide

Complete guide to deploying honey-duck Dagster pipelines in production using Docker.

## Table of Contents

- [Quick Start](#quick-start)
- [Architecture](#architecture)
- [Configuration](#configuration)
- [Deployment Steps](#deployment-steps)
- [Monitoring](#monitoring)
- [Scaling](#scaling)
- [Troubleshooting](#troubleshooting)
- [Security](#security)

---

## Quick Start

### Prerequisites

- Docker Engine 20.10+
- Docker Compose 2.0+
- 4GB+ RAM available
- 20GB+ disk space

### Deploy in 5 Minutes

```bash
# 1. Clone repository
git clone https://github.com/cogapp/honey-duck.git
cd honey-duck

# 2. Build and start services
docker-compose up -d

# 3. Check status
docker-compose ps

# 4. View logs
docker-compose logs -f dagster-webserver

# 5. Open Dagster UI
open http://localhost:3000
```

**Done!** The Dagster UI is now running at http://localhost:3000

---

## Architecture

### Docker Services

```
┌─────────────────────────────────────────────────────────┐
│                    Production Stack                      │
├─────────────────────────────────────────────────────────┤
│                                                           │
│  ┌─────────────────┐      ┌──────────────────┐          │
│  │  dagster-       │      │  dagster-        │          │
│  │  webserver      │◄─────┤  postgres        │          │
│  │  (UI)           │      │  (metadata)      │          │
│  │  :3000          │      │  :5432           │          │
│  └─────────────────┘      └──────────────────┘          │
│         ▲                                                │
│         │                                                │
│         │                                                │
│  ┌──────┴──────────┐      ┌──────────────────┐          │
│  │  dagster-       │      │  dagster-code-   │          │
│  │  daemon         │◄─────┤  location        │          │
│  │  (scheduler)    │      │  (gRPC :4000)    │          │
│  └─────────────────┘      └──────────────────┘          │
│                                                           │
└─────────────────────────────────────────────────────────┘
```

**Services**:

1. **dagster-webserver**: Web UI for viewing pipelines, runs, and assets
2. **dagster-daemon**: Background process for schedules, sensors, and run queue
3. **dagster-code-location**: gRPC server hosting your pipeline code
4. **dagster-postgres**: PostgreSQL database for run metadata and event logs

### Data Persistence

**Docker Volumes**:
- `postgres-data`: PostgreSQL database files (run history, event logs)
- `dagster-home`: Dagster instance storage (compute logs, artifacts)
- `data-output`: Pipeline output files (JSON, Parquet, DuckDB)

**Data survives container restarts** ✅

---

## Configuration

### Environment Variables

Create a `.env` file in the project root:

```bash
# PostgreSQL credentials
DAGSTER_POSTGRES_USER=dagster
DAGSTER_POSTGRES_PASSWORD=your_secure_password_here
DAGSTER_POSTGRES_DB=dagster
DAGSTER_POSTGRES_HOSTNAME=dagster-postgres
DAGSTER_POSTGRES_PORT=5432

# Optional: Elasticsearch
ELASTICSEARCH_HOST=http://elasticsearch:9200
ELASTICSEARCH_API_KEY=your_api_key

# Optional: API keys for bulk harvesters
ANTHROPIC_API_KEY=sk-ant-...
VOYAGE_API_KEY=pa-...

# Optional: Notifications
SLACK_WEBHOOK_URL=https://hooks.slack.com/services/...
SMTP_HOST=smtp.gmail.com
SMTP_USER=pipeline@company.com
SMTP_PASSWORD=...
```

**Load environment variables**:
```bash
docker-compose --env-file .env up -d
```

### Custom Configuration

**Modify `dagster.yaml`** to customize:
- Max concurrent runs (`max_concurrent_runs`)
- Timeout settings
- Retention policies

**Modify `docker-compose.yml`** to customize:
- Port mappings
- Resource limits
- Additional services (Elasticsearch, etc.)

---

## Deployment Steps

### 1. Development Environment

**For local testing before production**:

```bash
# Use development compose file
docker-compose -f docker-compose.dev.yml up -d

# Hot reload enabled for code changes
# Uses SQLite instead of PostgreSQL for simplicity
```

### 2. Staging Environment

**Test production configuration**:

```bash
# Build images
docker-compose build

# Start services
docker-compose up -d

# Run smoke tests
docker-compose exec dagster-webserver dagster job execute -j full_pipeline

# Check logs
docker-compose logs -f
```

### 3. Production Deployment

**Deploy to production server**:

```bash
# 1. Set production environment variables
export DAGSTER_POSTGRES_PASSWORD=$(openssl rand -base64 32)

# 2. Build production images
docker-compose build --no-cache

# 3. Start services
docker-compose up -d

# 4. Verify health
docker-compose ps
curl http://localhost:3000/server_info

# 5. Run initial materialization
docker-compose exec dagster-webserver dagster job execute -j full_pipeline
```

---

## Monitoring

### Health Checks

**Check service health**:
```bash
# All services
docker-compose ps

# Specific service logs
docker-compose logs -f dagster-webserver
docker-compose logs -f dagster-daemon
docker-compose logs -f dagster-code-location

# Follow all logs
docker-compose logs -f
```

**Health endpoints**:
- **Webserver**: http://localhost:3000/server_info
- **Daemon**: Check logs for "Daemon healthy"
- **PostgreSQL**: `docker-compose exec dagster-postgres pg_isready`

### Dagster UI Monitoring

**View in UI** (http://localhost:3000):

1. **Runs**: See all pipeline executions
2. **Assets**: View asset materialization history
3. **Schedules**: Monitor scheduled runs
4. **Sensors**: Track sensor evaluations
5. **Daemon**: Check daemon status

### Resource Usage

**Monitor Docker resources**:
```bash
# Container stats
docker stats

# Disk usage
docker system df

# Volume sizes
docker volume ls
du -sh /var/lib/docker/volumes/honey-duck_*
```

### Logs

**Access compute logs**:
```bash
# Via Docker
docker-compose exec dagster-webserver ls /app/dagster_home/storage/compute_logs

# Via Dagster UI
# Navigate to Run → Logs tab
```

---

## Scaling

### Horizontal Scaling

**Scale code location workers**:

```yaml
# docker-compose.yml
services:
  dagster-code-location:
    # ... existing config
    deploy:
      replicas: 3  # Run 3 instances
```

```bash
# Scale dynamically
docker-compose up -d --scale dagster-code-location=5
```

### Resource Limits

**Set CPU and memory limits**:

```yaml
# docker-compose.yml
services:
  dagster-webserver:
    # ... existing config
    deploy:
      resources:
        limits:
          cpus: '2'
          memory: 4G
        reservations:
          cpus: '1'
          memory: 2G
```

### Run Concurrency

**Adjust concurrent runs in `dagster.yaml`**:

```yaml
run_coordinator:
  module: dagster.core.run_coordinator
  class: QueuedRunCoordinator
  config:
    max_concurrent_runs: 25  # Increase from 10
```

---

## Troubleshooting

### Issue: Services Won't Start

**Symptoms**: `docker-compose up` fails

**Solutions**:
```bash
# 1. Check logs
docker-compose logs

# 2. Remove old containers
docker-compose down -v
docker-compose up -d

# 3. Rebuild images
docker-compose build --no-cache
docker-compose up -d

# 4. Check port conflicts
lsof -i :3000  # Webserver port
lsof -i :4000  # gRPC port
lsof -i :5432  # PostgreSQL port
```

### Issue: Database Connection Errors

**Symptoms**: "could not connect to server"

**Solutions**:
```bash
# 1. Check PostgreSQL health
docker-compose exec dagster-postgres pg_isready

# 2. Verify environment variables
docker-compose config

# 3. Check network
docker network ls
docker network inspect honey-duck_dagster-network

# 4. Restart PostgreSQL
docker-compose restart dagster-postgres
docker-compose logs dagster-postgres
```

### Issue: Code Changes Not Reflected

**Symptoms**: Updates don't appear in UI

**Solutions**:
```bash
# 1. Rebuild code location
docker-compose build dagster-code-location
docker-compose up -d dagster-code-location

# 2. Reload in UI
# Navigate to Deployment → Reload definitions

# 3. Full restart
docker-compose restart
```

### Issue: Out of Memory

**Symptoms**: Containers crash with OOM errors

**Solutions**:
```bash
# 1. Check Docker memory
docker system info | grep Memory

# 2. Increase Docker memory limit
# Docker Desktop → Settings → Resources → Memory → 8GB

# 3. Add swap space
# Linux: sudo fallocate -l 8G /swapfile

# 4. Reduce concurrent runs in dagster.yaml
```

### Issue: Slow Performance

**Symptoms**: Runs take too long

**Solutions**:
1. **Check Performance Guide**: See [PERFORMANCE_TUNING.md](PERFORMANCE_TUNING.md) (or [Performance Tuning](user-guide/performance.md))
2. **Use lazy evaluation**: Optimize Polars queries
3. **Enable streaming**: For large datasets
4. **Increase resources**: Add more CPU/memory
5. **Profile runs**: Use `track_timing()` helper

---

## Security

### Production Checklist

**Before deploying**:

- [ ] Change default PostgreSQL password
- [ ] Use secrets management (Docker secrets, HashiCorp Vault)
- [ ] Enable HTTPS with reverse proxy (Nginx, Traefik)
- [ ] Restrict network access (firewall rules)
- [ ] Enable Dagster authentication
- [ ] Rotate API keys regularly
- [ ] Use read-only file mounts where possible
- [ ] Enable audit logging
- [ ] Set up backup strategy
- [ ] Configure log rotation

### Secrets Management

**Using Docker Secrets**:

```yaml
# docker-compose.yml
services:
  dagster-webserver:
    secrets:
      - postgres_password
      - api_keys

secrets:
  postgres_password:
    file: ./secrets/postgres_password.txt
  api_keys:
    file: ./secrets/api_keys.txt
```

**Using Environment Files**:
```bash
# Never commit .env to git!
echo ".env" >> .gitignore

# Use separate env files per environment
docker-compose --env-file .env.production up -d
```

### HTTPS with Nginx

**Add reverse proxy**:

```yaml
# docker-compose.yml
services:
  nginx:
    image: nginx:alpine
    ports:
      - "443:443"
      - "80:80"
    volumes:
      - ./nginx.conf:/etc/nginx/nginx.conf
      - ./ssl:/etc/nginx/ssl
    depends_on:
      - dagster-webserver
```

**nginx.conf**:
```nginx
server {
    listen 443 ssl;
    server_name dagster.example.com;

    ssl_certificate /etc/nginx/ssl/cert.pem;
    ssl_certificate_key /etc/nginx/ssl/key.pem;

    location / {
        proxy_pass http://dagster-webserver:3000;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
    }
}
```

### Authentication

**Enable Dagster authentication**:

```yaml
# dagster.yaml
webserver:
  authentication:
    provider: basic
    config:
      username: admin
      password_hash: <bcrypt_hash>
```

---

## Backup and Recovery

### Backup Strategy

**What to backup**:
1. PostgreSQL database (run history, schedules)
2. Pipeline output files (JSON, Parquet)
3. Configuration files (dagster.yaml, workspace.yaml)

**Automated backup**:
```bash
#!/bin/bash
# backup.sh

# Backup PostgreSQL
docker-compose exec -T dagster-postgres pg_dump -U dagster dagster > backup_$(date +%Y%m%d).sql

# Backup volumes
docker run --rm -v honey-duck_data-output:/data -v $(pwd)/backups:/backup alpine tar czf /backup/data-output-$(date +%Y%m%d).tar.gz /data

# Backup to S3 (optional)
aws s3 cp backup_$(date +%Y%m%d).sql s3://my-bucket/backups/
```

**Run daily with cron**:
```bash
# crontab -e
0 2 * * * /path/to/backup.sh
```

### Recovery

**Restore from backup**:
```bash
# 1. Stop services
docker-compose down

# 2. Restore PostgreSQL
cat backup_20240115.sql | docker-compose exec -T dagster-postgres psql -U dagster dagster

# 3. Restore data volumes
docker run --rm -v honey-duck_data-output:/data -v $(pwd)/backups:/backup alpine tar xzf /backup/data-output-20240115.tar.gz -C /

# 4. Restart services
docker-compose up -d
```

---

## Advanced Deployment

### Kubernetes

**For large-scale production**:

See official Dagster Helm chart:
- **Helm Chart**: https://docs.dagster.io/deployment/guides/kubernetes/deploying-with-helm
- **Dagster Cloud**: https://dagster.io/cloud (fully managed)

### Cloud Providers

**AWS ECS/Fargate**:
- Use ECS task definitions from docker-compose
- RDS PostgreSQL for metadata
- EFS for shared storage

**Google Cloud Run**:
- Deploy code location as Cloud Run service
- Cloud SQL for PostgreSQL
- Cloud Storage for artifacts

**Azure Container Instances**:
- Deploy containers to ACI
- Azure Database for PostgreSQL
- Azure Blob Storage for artifacts

---

## Maintenance

### Updates

**Update Dagster version**:

```bash
# 1. Update pyproject.toml
# dagster>=1.13
# dagster-webserver>=1.13

# 2. Rebuild images
docker-compose build --no-cache

# 3. Stop services
docker-compose down

# 4. Backup database (see Backup section)

# 5. Start updated services
docker-compose up -d

# 6. Verify
docker-compose logs -f dagster-webserver
```

### Database Migrations

**Dagster handles migrations automatically**, but to manually migrate:

```bash
# Run migration command
docker-compose exec dagster-webserver dagster instance migrate
```

### Cleanup

**Remove old data**:
```bash
# Remove old runs (via UI or API)
# Deployment → Instance → Runs → Delete old runs

# Prune Docker resources
docker system prune -a --volumes

# Clean compute logs
docker-compose exec dagster-webserver rm -rf /app/dagster_home/storage/compute_logs/*
```

---

## Resources

- **Dagster Deployment Docs**: https://docs.dagster.io/deployment
- **Docker Deployment**: https://docs.dagster.io/deployment/oss/deployment-options/docker
- **Kubernetes Deployment**: https://docs.dagster.io/deployment/guides/kubernetes
- **Dagster Cloud**: https://dagster.io/cloud

---

## Support

**Need help?**
- Dagster Slack: https://dagster.io/slack
- GitHub Issues: https://github.com/cogapp/honey-duck/issues
- Documentation: See [Quick Start](getting-started/quick-start.md) for full guide list

**Production issues?**
- Check [Troubleshooting Guide](TROUBLESHOOTING.md) (or [Troubleshooting](user-guide/troubleshooting.md))
- Review logs: `docker-compose logs -f`
- Dagster status page: https://status.dagster.io/

---

**Deploy with confidence!** 🚀
