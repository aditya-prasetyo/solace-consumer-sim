#!/bin/bash
# =============================================================================
# Start CDC POC Services
# =============================================================================
# Usage:
#   ./start-infra-only.sh spark                  # Infra only (hybrid dev)
#   ./start-infra-only.sh spark with-consumer    # Infra + consumer
#   ./start-infra-only.sh spark with-generator   # Infra + generator
#   ./start-infra-only.sh spark full             # Infra + consumer + generator
#   ./start-infra-only.sh flink full             # Same for flink
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
echo -e "${GREEN}CDC POC - Starting Services${NC}"
echo -e "${GREEN}========================================${NC}"

# Check if docker is running
if ! docker info > /dev/null 2>&1; then
    echo -e "${RED}Error: Docker is not running${NC}"
    exit 1
fi

# Determine which architecture and mode
ARCH=${1:-spark}
MODE=${2:-}

if [ "$ARCH" != "spark" ] && [ "$ARCH" != "flink" ]; then
    echo -e "${RED}Unknown architecture: $ARCH${NC}"
    echo "Usage: $0 [spark|flink] [full|with-consumer|with-generator]"
    exit 1
fi

COMPOSE_FILE="docker-compose.${ARCH}.yml"
cd "$DOCKER_DIR"

if [ -z "$MODE" ]; then
    echo -e "${YELLOW}Mode: Hybrid (infra only — consumer & generator jalan lokal)${NC}"
    docker compose -f "$COMPOSE_FILE" up -d solace postgres rest-api
elif [ "$MODE" == "full" ] || [ "$MODE" == "with-consumer" ] || [ "$MODE" == "with-generator" ]; then
    echo -e "${YELLOW}Mode: ${MODE} (infra + profile services)${NC}"
    docker compose -f "$COMPOSE_FILE" --profile "$MODE" up -d
else
    echo -e "${RED}Unknown mode: $MODE${NC}"
    echo "Usage: $0 [spark|flink] [full|with-consumer|with-generator]"
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
echo -e "${GREEN}Services are ready!${NC}"
echo -e "${GREEN}========================================${NC}"
echo ""
echo "Services running:"
if [ -z "$MODE" ]; then
    docker compose -f "$DOCKER_DIR/docker-compose.${ARCH}.yml" ps
else
    docker compose -f "$DOCKER_DIR/docker-compose.${ARCH}.yml" --profile "$MODE" ps
fi
echo ""
echo "URLs:"
echo "  - Solace Manager: http://localhost:8080 (admin/admin)"
echo "  - PostgreSQL: localhost:5432 (cdc_user/cdc_pass)"
echo "  - REST API Docs: http://localhost:8000/docs"
if [ "$ARCH" == "spark" ] && ([ "$MODE" == "full" ] || [ "$MODE" == "with-consumer" ]); then
    echo "  - Spark UI: http://localhost:4040"
fi
if [ "$ARCH" == "flink" ] && ([ "$MODE" == "full" ] || [ "$MODE" == "with-consumer" ]); then
    echo "  - Flink UI: http://localhost:8081"
fi
echo ""
if [ -z "$MODE" ]; then
    echo -e "${YELLOW}To run consumer locally:${NC}"
    echo "  python -m src.${ARCH}_consumer.main"
    echo ""
    echo -e "${YELLOW}To run generator locally:${NC}"
    echo "  cd $PROJECT_ROOT/src/generator"
    echo "  python -m venv venv && source venv/bin/activate"
    echo "  pip install -r ../../requirements/generator.txt"
    echo "  SOLACE_HOST=localhost SOLACE_PORT=55555 python main.py"
    echo ""
fi
