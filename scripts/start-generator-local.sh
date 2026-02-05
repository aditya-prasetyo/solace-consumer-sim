#!/bin/bash
# =============================================================================
# Start Generator Locally (Hybrid Development Mode)
# =============================================================================

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
GENERATOR_DIR="$PROJECT_ROOT/src/generator"

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}CDC POC - Local Generator${NC}"
echo -e "${GREEN}========================================${NC}"

# Check if Solace is running
if ! curl -s http://localhost:8080/health-check/guaranteed-active > /dev/null 2>&1; then
    echo -e "${RED}Error: Solace is not running!${NC}"
    echo "Please start infrastructure first:"
    echo "  ./scripts/start-infra-only.sh"
    exit 1
fi

# Check if virtual environment exists
VENV_DIR="$GENERATOR_DIR/venv"
if [ ! -d "$VENV_DIR" ]; then
    echo -e "${YELLOW}Creating virtual environment...${NC}"
    python3 -m venv "$VENV_DIR"
fi

# Activate virtual environment
source "$VENV_DIR/bin/activate"

# Install dependencies
echo -e "${YELLOW}Installing dependencies...${NC}"
pip install -q -r "$PROJECT_ROOT/requirements/generator.txt"

# Set environment variables
export SOLACE_HOST=${SOLACE_HOST:-localhost}
export SOLACE_PORT=${SOLACE_PORT:-55555}
export SOLACE_VPN=${SOLACE_VPN:-default}
export SOLACE_USERNAME=${SOLACE_USERNAME:-admin}
export SOLACE_PASSWORD=${SOLACE_PASSWORD:-admin}
export EVENTS_PER_MINUTE=${EVENTS_PER_MINUTE:-1000}
export LOG_LEVEL=${LOG_LEVEL:-INFO}

echo ""
echo -e "${GREEN}Starting generator with:${NC}"
echo "  SOLACE_HOST: $SOLACE_HOST"
echo "  SOLACE_PORT: $SOLACE_PORT"
echo "  EVENTS_PER_MINUTE: $EVENTS_PER_MINUTE"
echo ""

# Run generator
cd "$PROJECT_ROOT"
python -m src.generator.main
