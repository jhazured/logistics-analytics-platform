#!/bin/bash
# 🚀 Logistics Analytics Platform - Quick Deploy
# This script provides a convenient entry point to the deployment system

set -e

echo "🚀 Logistics Analytics Platform - Quick Deploy"
echo "=============================================="
echo ""
echo "This script will run the complete deployment process."
echo "For more control, use: ./scripts/deployment/deploy_all.sh [phase_numbers]"
echo ""

# Run the main deployment script
exec "$(dirname "$0")/scripts/deployment/deploy_all.sh" "$@"
