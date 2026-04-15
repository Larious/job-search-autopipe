#!/usr/bin/env bash
# ============================================================
# Job Search AutoPipe — Quick Start Setup
# ============================================================
# Run this script to set up the pipeline for the first time.
# Usage: chmod +x scripts/setup.sh && ./scripts/setup.sh
# ============================================================

set -euo pipefail

BOLD='\033[1m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

echo -e "${BOLD}╔══════════════════════════════════════╗${NC}"
echo -e "${BOLD}║   Job Search AutoPipe — Setup        ║${NC}"
echo -e "${BOLD}╚══════════════════════════════════════╝${NC}"
echo ""

# 1. Check prerequisites
echo -e "${YELLOW}[1/5]${NC} Checking prerequisites..."

command -v docker >/dev/null 2>&1 || {
    echo -e "${RED}✗ Docker not found. Install: https://docs.docker.com/get-docker/${NC}"
    exit 1
}
echo -e "  ${GREEN}✓${NC} Docker found"

command -v docker-compose >/dev/null 2>&1 || docker compose version >/dev/null 2>&1 || {
    echo -e "${RED}✗ Docker Compose not found.${NC}"
    exit 1
}
echo -e "  ${GREEN}✓${NC} Docker Compose found"

# 2. Create config
echo -e "${YELLOW}[2/5]${NC} Checking configuration..."

if [ ! -f config/config.yaml ]; then
    cp config/config.example.yaml config/config.yaml
    echo -e "  ${YELLOW}!${NC} Created config/config.yaml from template"
    echo -e "  ${YELLOW}!${NC} Please edit config/config.yaml with your API keys before running the pipeline."
    echo ""
    echo -e "  ${BOLD}Required API keys:${NC}"
    echo -e "    - Adzuna: https://developer.adzuna.com (free)"
    echo -e "    - Reed:   https://www.reed.co.uk/developers (free)"
    echo -e "    - Slack:  Create webhook at https://api.slack.com/messaging/webhooks"
    echo ""
else
    echo -e "  ${GREEN}✓${NC} config/config.yaml exists"
fi

# 3. Create logs directory
echo -e "${YELLOW}[3/5]${NC} Creating directories..."
mkdir -p logs
echo -e "  ${GREEN}✓${NC} logs/ directory ready"

# 4. Start infrastructure
echo -e "${YELLOW}[4/5]${NC} Starting Docker containers..."
docker compose up -d pipeline-db airflow-db
echo -e "  ${GREEN}✓${NC} Databases starting..."

# Wait for databases
echo -e "  Waiting for databases to be healthy..."
sleep 5

# 5. Initialize schema and Airflow
echo -e "${YELLOW}[5/5]${NC} Initializing..."
docker compose up schema-init
docker compose up airflow-init
docker compose up -d airflow-webserver airflow-scheduler

echo ""
echo -e "${GREEN}╔══════════════════════════════════════╗${NC}"
echo -e "${GREEN}║   Setup Complete!                    ║${NC}"
echo -e "${GREEN}╚══════════════════════════════════════╝${NC}"
echo ""
echo -e "  ${BOLD}Airflow UI:${NC}    http://localhost:8080"
echo -e "  ${BOLD}Login:${NC}         admin / admin"
echo -e "  ${BOLD}PostgreSQL:${NC}    localhost:5432 (autopipe/autopipe_password)"
echo ""
echo -e "  ${BOLD}Next steps:${NC}"
echo -e "  1. Edit config/config.yaml with your API keys"
echo -e "  2. Edit config/skills_profile.yaml with your skills"
echo -e "  3. Open Airflow UI and trigger 'job_search_autopipe' DAG"
echo -e "  4. Check Slack for your morning digest!"
echo ""
