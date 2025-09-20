#!/bin/bash
# Build-and-Run Deployment Script
# Optimized for one-time deployment with minimal costs

set -e

echo "🚀 Starting Build-and-Run Deployment"
echo "======================================"

# Configuration
ENVIRONMENT=${1:-dev}
WAREHOUSE_SIZE="X-SMALL"  # Minimal cost warehouse
THREADS=2  # Minimal threads for cost optimization

echo "📋 Deployment Configuration:"
echo "  Environment: $ENVIRONMENT"
echo "  Warehouse: COMPUTE_WH_XS ($WAREHOUSE_SIZE)"
echo "  Threads: $THREADS"
echo ""

# Set environment variables for cost optimization
export DBT_THREADS=$THREADS
export DBT_TARGET=$ENVIRONMENT

echo "🔧 Setting up environment..."
cd dbt

echo "📦 Installing dbt packages..."
dbt deps

echo "🔍 Parsing dbt models..."
dbt parse --target $ENVIRONMENT

echo "🏗️ Building dbt models (using views for cost optimization)..."
dbt run --target $ENVIRONMENT --threads $THREADS

echo "🧪 Running essential tests only..."
dbt test --target $ENVIRONMENT --select tag:critical --threads $THREADS

echo "📊 Generating documentation..."
dbt docs generate --target $ENVIRONMENT

echo "✅ Build-and-Run deployment completed successfully!"
echo ""
echo "📈 Cost Summary:"
echo "  - Warehouse usage: ~$2-5 total"
echo "  - Storage: Minimal (views only)"
echo "  - Total estimated cost: $5-10"
echo ""
echo "🎯 Next steps:"
echo "  1. Review generated documentation"
echo "  2. Test key analytics views"
echo "  3. Archive or delete resources when done"
echo ""
echo "💡 To minimize ongoing costs:"
echo "  - All models use views (no storage costs)"
echo "  - Warehouse auto-suspends after 1 minute"
echo "  - No incremental loading (one-time build)"
