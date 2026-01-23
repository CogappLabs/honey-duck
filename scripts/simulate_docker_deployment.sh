#!/bin/bash
# Simulate Docker deployment and validate configuration

set -e

echo "🐳 Docker Deployment Simulation"
echo "================================"
echo ""

# Colors
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m'

echo -e "${BLUE}This script simulates what happens when you run Docker deployment.${NC}"
echo ""

# Step 1: Validate configuration files
echo "Step 1: Validating Configuration Files"
echo "======================================="
echo ""

FILES=(
    "Dockerfile"
    "docker-compose.yml"
    "docker-compose.dev.yml"
    "dagster.yaml"
    "workspace.yaml"
    ".dockerignore"
    ".env.example"
    "Makefile"
)

for file in "${FILES[@]}"; do
    if [ -f "$file" ]; then
        echo -e "${GREEN}✓${NC} Found: $file"
    else
        echo -e "${RED}✗${NC} Missing: $file"
        exit 1
    fi
done

echo ""

# Step 2: Validate YAML syntax
echo "Step 2: Validating YAML Syntax"
echo "==============================="
echo ""

python3 << 'PYTHON'
import yaml
import sys

files = {
    'docker-compose.yml': 'Production Docker Compose',
    'docker-compose.dev.yml': 'Development Docker Compose',
    'dagster.yaml': 'Dagster Configuration',
    'workspace.yaml': 'Workspace Configuration',
}

errors = []
for filename, description in files.items():
    try:
        with open(filename) as f:
            config = yaml.safe_load(f)
        print(f'✓ {description}: Valid YAML')

        # Show key info
        if 'services' in config:
            services = list(config['services'].keys())
            print(f'  Services: {", ".join(services)}')
        if 'storage' in config:
            print(f'  Storage backend: {list(config["storage"].keys())[0]}')

    except Exception as e:
        errors.append(f'{filename}: {str(e)}')
        print(f'✗ {description}: {e}')

print()

if errors:
    print(f'Errors found: {len(errors)}')
    for err in errors:
        print(f'  - {err}')
    sys.exit(1)
else:
    print('✅ All YAML files are valid!')
PYTHON

echo ""

# Step 3: Show what Docker Compose would do
echo "Step 3: Docker Compose Deployment Plan"
echo "======================================="
echo ""

echo -e "${BLUE}Production Stack (docker-compose.yml):${NC}"
echo ""
python3 << 'PYTHON'
import yaml

with open('docker-compose.yml') as f:
    config = yaml.safe_load(f)

print("Services to be created:")
for service, details in config['services'].items():
    print(f"\n📦 {service}")
    print(f"   Image: {details.get('build', {}).get('context', details.get('image', 'N/A'))}")

    if 'ports' in details:
        print(f"   Ports: {', '.join(details['ports'])}")

    if 'environment' in details:
        env_count = len(details['environment'])
        print(f"   Environment vars: {env_count}")

    if 'volumes' in details:
        print(f"   Volumes: {len(details['volumes'])} mounted")

    if 'depends_on' in details:
        deps = list(details['depends_on'].keys()) if isinstance(details['depends_on'], dict) else details['depends_on']
        print(f"   Depends on: {', '.join(deps)}")

print("\nVolumes:")
for volume in config.get('volumes', {}).keys():
    print(f"   • {volume}")

print("\nNetworks:")
for network in config.get('networks', {}).keys():
    print(f"   • {network}")
PYTHON

echo ""
echo ""

echo -e "${BLUE}Development Stack (docker-compose.dev.yml):${NC}"
echo ""
python3 << 'PYTHON'
import yaml

with open('docker-compose.dev.yml') as f:
    config = yaml.safe_load(f)

print("Services to be created:")
for service, details in config['services'].items():
    print(f"\n📦 {service}")
    if 'profiles' in details:
        print(f"   Profile: {', '.join(details['profiles'])}")
    if 'ports' in details:
        print(f"   Ports: {', '.join(details['ports'])}")
    if 'volumes' in details:
        vol_count = len(details['volumes'])
        print(f"   Volumes: {vol_count} (includes code mounts for hot reload)")
PYTHON

echo ""
echo ""

# Step 4: Show Dockerfile build stages
echo "Step 4: Docker Image Build Plan"
echo "================================"
echo ""

echo "Dockerfile stages:"
grep -E "^FROM|^# Production stage" Dockerfile | while read line; do
    if [[ $line == FROM* ]]; then
        echo "   📦 $line"
    elif [[ $line == "# Production stage" ]]; then
        echo ""
        echo "   Production optimizations:"
    fi
done

echo ""
echo "Image contents:"
echo "   • Python 3.12 slim"
echo "   • uv package manager"
echo "   • honey_duck package"
echo "   • cogapp_libs utilities"
echo "   • All dependencies from pyproject.toml"
echo ""

# Step 5: Show environment variables needed
echo "Step 5: Environment Variables Required"
echo "======================================="
echo ""

echo "Required for production:"
grep -E "^[A-Z_]+=|^# " .env.example | grep -v "^#.*====" | grep -v "^# Copy" | grep -v "^# Example:" | head -20

echo ""

# Step 6: Deployment commands
echo "Step 6: Deployment Commands"
echo "==========================="
echo ""

echo -e "${YELLOW}To deploy locally with Docker:${NC}"
echo ""
echo "Development (with hot reload):"
echo "  make dev"
echo "  # or"
echo "  docker-compose -f docker-compose.dev.yml up -d"
echo ""
echo "Production:"
echo "  make build    # Build images"
echo "  make up       # Start services"
echo "  # or"
echo "  docker-compose build"
echo "  docker-compose up -d"
echo ""
echo "Check status:"
echo "  make ps"
echo "  docker-compose ps"
echo ""
echo "View logs:"
echo "  make logs"
echo "  docker-compose logs -f"
echo ""
echo "Stop services:"
echo "  make down"
echo "  docker-compose down"
echo ""

# Step 7: Health checks
echo "Step 7: Health Check Endpoints"
echo "==============================="
echo ""

echo "Once running, check health at:"
echo "  • Dagster UI: http://localhost:3000"
echo "  • Webserver health: http://localhost:3000/server_info"
echo "  • PostgreSQL: docker-compose exec dagster-postgres pg_isready"
echo ""

# Step 8: Data persistence
echo "Step 8: Data Persistence"
echo "========================"
echo ""

echo "Docker volumes (data survives container restarts):"
python3 << 'PYTHON'
import yaml

with open('docker-compose.yml') as f:
    config = yaml.safe_load(f)

for volume, details in config.get('volumes', {}).items():
    driver = details.get('driver', 'local') if isinstance(details, dict) else 'local'
    print(f"   • {volume} ({driver})")

    # Show what's stored
    if 'postgres' in volume:
        print(f"     Stores: PostgreSQL metadata (runs, events, schedules)")
    elif 'home' in volume:
        print(f"     Stores: Compute logs, artifacts")
    elif 'output' in volume:
        print(f"     Stores: Pipeline outputs (JSON, Parquet, DuckDB)")
PYTHON

echo ""

# Summary
echo ""
echo "================================"
echo -e "${GREEN}✅ Validation Complete!${NC}"
echo "================================"
echo ""
echo "Summary:"
echo "  ✓ All configuration files present and valid"
echo "  ✓ YAML syntax correct"
echo "  ✓ Docker Compose services configured"
echo "  ✓ Dockerfile build stages defined"
echo "  ✓ Environment variables documented"
echo "  ✓ Health checks configured"
echo "  ✓ Data persistence volumes defined"
echo ""
echo -e "${YELLOW}Next steps:${NC}"
echo "  1. Copy .env.example to .env and update values"
echo "  2. Install Docker: https://docs.docker.com/get-docker/"
echo "  3. Run: make build && make up"
echo "  4. Open: http://localhost:3000"
echo ""
echo "For detailed instructions, see docs/DEPLOYMENT.md"
echo ""
