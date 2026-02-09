#!/bin/bash
# =============================================================================
# Start Infrastructure Only (Hybrid Development Mode)
# =============================================================================
# This script starts only the infrastructure services (Solace, PostgreSQL)
# Use this for local development where generator runs locally
# =============================================================================

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
DOCKER_DIR="$PROJECT_ROOT/docker"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}CDC POC - Starting Infrastructure${NC}"
echo -e "${GREEN}========================================${NC}"

# Check if docker is running
if ! docker info > /dev/null 2>&1; then
    echo -e "${RED}Error: Docker is not running${NC}"
    exit 1
fi

# Determine which architecture to start
ARCH=${1:-spark}

if [ "$ARCH" == "spark" ]; then
    echo -e "${YELLOW}Starting Architecture A infrastructure (Solace + PostgreSQL + REST API)...${NC}"
    cd "$DOCKER_DIR"
    docker compose -f docker-compose.spark.yml up -d solace postgres rest-api
elif [ "$ARCH" == "flink" ]; then
    echo -e "${YELLOW}Starting Architecture B infrastructure (Solace + PostgreSQL + REST API)...${NC}"
    cd "$DOCKER_DIR"
    docker compose -f docker-compose.flink.yml up -d solace postgres rest-api
else
    echo -e "${RED}Unknown architecture: $ARCH${NC}"
    echo "Usage: $0 [spark|flink]"
    exit 1
fi

echo ""
echo -e "${GREEN}Waiting for services to be healthy...${NC}"

# Wait for Solace to be ready
echo -n "Waiting for Solace..."
timeout=120
counter=0
until curl -s http://localhost:8080/health-check/guaranteed-active > /dev/null 2>&1; do
    sleep 2
    counter=$((counter + 2))
    if [ $counter -ge $timeout ]; then
        echo -e " ${RED}TIMEOUT${NC}"
        echo "Solace did not become healthy in time"
        exit 1
    fi
    echo -n "."
done
echo -e " ${GREEN}READY${NC}"

# Wait for PostgreSQL to be ready (both architectures use dual sink)
echo -n "Waiting for PostgreSQL..."
counter=0
until docker exec postgres-ods pg_isready -U cdc_user -d ods > /dev/null 2>&1; do
    sleep 2
    counter=$((counter + 2))
    if [ $counter -ge $timeout ]; then
        echo -e " ${RED}TIMEOUT${NC}"
        echo "PostgreSQL did not become healthy in time"
        exit 1
    fi
    echo -n "."
done
echo -e " ${GREEN}READY${NC}"

# Wait for REST API to be ready (both architectures use dual sink)
echo -n "Waiting for REST API..."
counter=0
until curl -s http://localhost:8000/health > /dev/null 2>&1; do
    sleep 2
    counter=$((counter + 2))
    if [ $counter -ge $timeout ]; then
        echo -e " ${RED}TIMEOUT${NC}"
        echo "REST API did not become healthy in time"
        exit 1
    fi
    echo -n "."
done
echo -e " ${GREEN}READY${NC}"

echo ""
echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}Infrastructure is ready!${NC}"
echo -e "${GREEN}========================================${NC}"
echo ""
echo "Services running:"
docker compose -f "$DOCKER_DIR/docker-compose.${ARCH}.yml" ps
echo ""
echo "URLs:"
echo "  - Solace Manager: http://localhost:8080 (admin/admin)"
echo "  - PostgreSQL: localhost:5432 (cdc_user/cdc_pass)"
echo "  - REST API Docs: http://localhost:8000/docs"
echo ""
echo -e "${YELLOW}To run generator locally:${NC}"
echo "  cd $PROJECT_ROOT/src/generator"
echo "  python -m venv venv && source venv/bin/activate"
echo "  pip install -r ../../requirements/generator.txt"
echo "  SOLACE_HOST=localhost SOLACE_PORT=55555 python main.py"
echo ""
