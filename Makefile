# Makefile for honey-duck Dagster deployment

.PHONY: help build up down logs restart clean test backup

# Default target
help:
	@echo "honey-duck Dagster Deployment"
	@echo ""
	@echo "Development Commands:"
	@echo "  make dev              Start development environment with hot reload"
	@echo "  make dev-down         Stop development environment"
	@echo "  make dev-logs         Follow development logs"
	@echo ""
	@echo "Production Commands:"
	@echo "  make build            Build production Docker images"
	@echo "  make up               Start production services"
	@echo "  make down             Stop production services"
	@echo "  make restart          Restart production services"
	@echo "  make logs             Follow production logs"
	@echo "  make ps               Show service status"
	@echo ""
	@echo "Elasticsearch:"
	@echo "  make dev-elastic      Start dev environment with Elasticsearch"
	@echo "  make elastic          Start Elasticsearch for production"
	@echo ""
	@echo "Maintenance:"
	@echo "  make clean            Remove all containers and volumes"
	@echo "  make test             Run tests in container"
	@echo "  make backup           Backup database and data"
	@echo "  make shell            Open shell in code location container"
	@echo "  make migrate          Run database migrations"
	@echo ""
	@echo "Monitoring:"
	@echo "  make stats            Show container resource usage"
	@echo "  make health           Check service health"

# Development
dev:
	docker-compose -f docker-compose.dev.yml up -d
	@echo ""
	@echo "✅ Development environment started!"
	@echo "🌐 Dagster UI: http://localhost:3000"
	@echo "📝 Logs: make dev-logs"
	@echo ""

dev-elastic:
	docker-compose -f docker-compose.dev.yml --profile with-elasticsearch up -d
	@echo ""
	@echo "✅ Development environment with Elasticsearch started!"
	@echo "🌐 Dagster UI: http://localhost:3000"
	@echo "🔍 Elasticsearch: http://localhost:9200"
	@echo ""

dev-down:
	docker-compose -f docker-compose.dev.yml down

dev-logs:
	docker-compose -f docker-compose.dev.yml logs -f

# Production
build:
	docker-compose build --no-cache

up:
	docker-compose up -d
	@echo ""
	@echo "✅ Production services started!"
	@echo "🌐 Dagster UI: http://localhost:3000"
	@echo "📊 Status: make ps"
	@echo "📝 Logs: make logs"
	@echo ""

down:
	docker-compose down

restart:
	docker-compose restart

logs:
	docker-compose logs -f

ps:
	docker-compose ps

# Elasticsearch
elastic:
	docker-compose -f docker-compose.elasticsearch.yml up -d
	@echo ""
	@echo "✅ Elasticsearch started!"
	@echo "🔍 Elasticsearch: http://localhost:9200"
	@echo "📊 Kibana: http://localhost:5601"
	@echo ""

# Maintenance
clean:
	@echo "⚠️  This will remove all containers and volumes!"
	@read -p "Are you sure? [y/N] " -n 1 -r; \
	echo; \
	if [[ $$REPLY =~ ^[Yy]$$ ]]; then \
		docker-compose down -v; \
		docker-compose -f docker-compose.dev.yml down -v; \
		docker system prune -f; \
		echo "✅ Cleanup complete!"; \
	fi

test:
	docker-compose exec dagster-code-location uv run pytest -xvs

backup:
	@mkdir -p backups
	@echo "📦 Backing up database..."
	docker-compose exec -T dagster-postgres pg_dump -U dagster dagster > backups/backup_$$(date +%Y%m%d_%H%M%S).sql
	@echo "📦 Backing up data volumes..."
	docker run --rm -v honey-duck_data-output:/data -v $$(pwd)/backups:/backup alpine tar czf /backup/data_$$(date +%Y%m%d_%H%M%S).tar.gz /data
	@echo "✅ Backup complete! Check backups/ directory"

shell:
	docker-compose exec dagster-code-location /bin/bash

migrate:
	docker-compose exec dagster-webserver dagster instance migrate
	@echo "✅ Database migrations complete!"

# Monitoring
stats:
	docker stats

health:
	@echo "🏥 Checking service health..."
	@echo ""
	@echo "Webserver:"
	@curl -s http://localhost:3000/server_info | head -1 || echo "❌ Not responding"
	@echo ""
	@echo "PostgreSQL:"
	@docker-compose exec -T dagster-postgres pg_isready -U dagster || echo "❌ Not ready"
	@echo ""
	@echo "Containers:"
	@docker-compose ps
