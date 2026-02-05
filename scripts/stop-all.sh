#!/bin/bash
# =============================================================================
# Stop All Services
# =============================================================================

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
DOCKER_DIR="$PROJECT_ROOT/docker"

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

echo -e "${YELLOW}========================================${NC}"
echo -e "${YELLOW}Stopping all CDC POC services...${NC}"
echo -e "${YELLOW}========================================${NC}"

cd "$DOCKER_DIR"

# Stop Architecture A
if docker compose -f docker-compose.spark.yml ps -q 2>/dev/null | grep -q .; then
    echo "Stopping Architecture A services..."
    docker compose -f docker-compose.spark.yml --profile full down
fi

# Stop Architecture B
if docker compose -f docker-compose.flink.yml ps -q 2>/dev/null | grep -q .; then
    echo "Stopping Architecture B services..."
    docker compose -f docker-compose.flink.yml --profile full down
fi

echo ""
echo -e "${GREEN}All services stopped.${NC}"
echo ""

# Optional: Clean up volumes
if [ "$1" == "--clean" ]; then
    echo -e "${YELLOW}Cleaning up volumes...${NC}"
    docker volume rm solace-data postgres-data spark-checkpoints flink-checkpoints 2>/dev/null || true
    echo -e "${GREEN}Volumes cleaned.${NC}"
fi

echo "To also remove volumes, run: $0 --clean"
