#!/bin/bash
# =============================================================================
# Start Architecture B - PyFlink → REST API (Full Container Mode)
# =============================================================================

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
DOCKER_DIR="$PROJECT_ROOT/docker"

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}CDC POC - Architecture B (PyFlink)${NC}"
echo -e "${GREEN}========================================${NC}"

cd "$DOCKER_DIR"

# Start all services with full profile
echo -e "${YELLOW}Starting all services...${NC}"
docker compose -f docker-compose.flink.yml --profile full up -d

echo ""
echo -e "${GREEN}Waiting for services to be healthy...${NC}"

# Wait for services
sleep 10

echo ""
echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}Architecture B is running!${NC}"
echo -e "${GREEN}========================================${NC}"
echo ""
echo "Services:"
docker compose -f docker-compose.flink.yml ps
echo ""
echo "URLs:"
echo "  - Solace Manager: http://localhost:8080"
echo "  - Flink UI: http://localhost:8081"
echo "  - REST API Docs: http://localhost:8000/docs"
echo ""
echo "Logs:"
echo "  docker compose -f docker/docker-compose.flink.yml logs -f"
echo ""
