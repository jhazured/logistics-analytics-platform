#!/bin/bash
# 🚀 Logistics Analytics Platform - Quick Deploy
# This script provides a convenient entry point to the deployment system

set -e

# Get script directory and project root
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Load environment variables from .env file
if [[ -f "$SCRIPT_DIR/.env" ]]; then
    set -a  # automatically export all variables
    source "$SCRIPT_DIR/.env"
    set +a  # stop automatically exporting
    echo "✅ Environment variables loaded from .env file"
fi

echo "🚀 Logistics Analytics Platform - Quick Deploy"
echo "=============================================="
echo ""
echo "This script will run the complete deployment process."
echo "For more control, use: ./scripts/02_deployment/handlers/deploy_all.sh [phase_numbers]"
echo ""
echo "Options:"
echo "  --skip-data    Skip data generation and loading phases"
echo "  --reset        Reset deployment status and run all phases"
echo ""

# Run the main deployment script
exec "$SCRIPT_DIR/scripts/02_deployment/handlers/deploy_all.sh" "$@"
