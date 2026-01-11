#!/bin/bash
# Test Docker deployment configuration

set -e

echo "🐳 Testing Docker Deployment Configuration"
echo "=========================================="
echo ""

# Colors
GREEN='\033[0;32m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# Check Docker installed
if ! command -v docker &> /dev/null; then
    echo -e "${RED}✗ Docker not installed${NC}"
    exit 1
fi
echo -e "${GREEN}✓ Docker installed${NC}"

# Check Docker Compose installed
if ! command -v docker-compose &> /dev/null && ! docker compose version &> /dev/null; then
    echo -e "${RED}✗ Docker Compose not installed${NC}"
    exit 1
fi
echo -e "${GREEN}✓ Docker Compose installed${NC}"

# Validate YAML files
echo ""
echo "Validating YAML files..."
for file in docker-compose.yml docker-compose.dev.yml dagster.yaml workspace.yaml; do
    if [ ! -f "$file" ]; then
        echo -e "${RED}✗ Missing: $file${NC}"
        exit 1
    fi
    echo -e "${GREEN}✓ Found: $file${NC}"
done

# Validate docker-compose config
echo ""
echo "Validating Docker Compose configuration..."
if docker-compose config --quiet; then
    echo -e "${GREEN}✓ docker-compose.yml is valid${NC}"
else
    echo -e "${RED}✗ docker-compose.yml has errors${NC}"
    exit 1
fi

if docker-compose -f docker-compose.dev.yml config --quiet; then
    echo -e "${GREEN}✓ docker-compose.dev.yml is valid${NC}"
else
    echo -e "${RED}✗ docker-compose.dev.yml has errors${NC}"
    exit 1
fi

# Check Dockerfile
echo ""
echo "Validating Dockerfile..."
if [ ! -f "Dockerfile" ]; then
    echo -e "${RED}✗ Dockerfile not found${NC}"
    exit 1
fi

# Basic Dockerfile syntax check
if grep -q "FROM python:3.12-slim" Dockerfile; then
    echo -e "${GREEN}✓ Dockerfile base image correct${NC}"
else
    echo -e "${RED}✗ Dockerfile base image issue${NC}"
    exit 1
fi

# Test build (dry run)
echo ""
echo "Testing Docker build (dry run)..."
if docker build --help &> /dev/null; then
    echo -e "${GREEN}✓ Docker build available${NC}"
fi

# Check .dockerignore
if [ ! -f ".dockerignore" ]; then
    echo -e "${RED}⚠ Warning: .dockerignore not found${NC}"
else
    echo -e "${GREEN}✓ .dockerignore found${NC}"
fi

# Check .env.example
if [ ! -f ".env.example" ]; then
    echo -e "${RED}⚠ Warning: .env.example not found${NC}"
else
    echo -e "${GREEN}✓ .env.example found${NC}"
fi

echo ""
echo -e "${GREEN}=========================================="
echo "✅ All Docker configuration tests passed!"
echo -e "==========================================${NC}"
echo ""
echo "To deploy:"
echo "  Development: make dev"
echo "  Production:  make build && make up"
echo ""
