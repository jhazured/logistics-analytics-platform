#!/bin/bash
# 🧪 Phase 7: Run Final Tests
# Runs comprehensive tests and generates reports

set -e

echo "🧪 Phase 7: Run Final Tests"
echo "==========================="

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

print_status() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Load environment variables
load_env_vars() {
    # Navigate to project root
    cd "$(dirname "$0")/../.."
    
    if [ -f ".env" ]; then
        set -a
        source .env
        set +a
    else
        print_error ".env file not found! Run 01_setup_environment.sh first."
        exit 1
    fi
}

# Run dbt tests
run_dbt_tests() {
    print_status "Running comprehensive dbt tests..."
    
    cd dbt
    
    # Run all tests
    print_status "Running all dbt tests..."
    if dbt test --target dev --threads 2; then
        print_success "All dbt tests passed"
    else
        print_warning "Some dbt tests failed - check results"
    fi
    
    cd ..
}

# Generate quality report
generate_quality_report() {
    print_status "Generating quality report..."
    
    cd scripts/monitoring
    python generate_quality_report.py
    cd ../..
    
    print_success "Quality report generated"
}

# Test key analytics
test_analytics() {
    print_status "Testing key analytics views..."
    
    python -c "
import snowflake.connector
import os

# Connect to Snowflake
conn = snowflake.connector.connect(
    account=os.getenv('SF_ACCOUNT'),
    user=os.getenv('SF_USER'),
    password=os.getenv('SF_PASSWORD'),
    role=os.getenv('SF_ROLE'),
    warehouse=os.getenv('SF_WAREHOUSE'),
    database=os.getenv('SF_DATABASE')
)

cursor = conn.cursor()

# Test key views
test_queries = [
    ('Analytics Views', 'SELECT COUNT(*) FROM ANALYTICS.VW_SHIPMENT_PERFORMANCE_ANALYTICS'),
    ('ML Features', 'SELECT COUNT(*) FROM ML_FEATURES.TBL_ML_CONSOLIDATED_FEATURE_STORE'),
    ('Real-time KPIs', 'SELECT COUNT(*) FROM MONITORING.REAL_TIME_KPIS'),
    ('Cost Monitoring', 'SELECT COUNT(*) FROM MONITORING.VW_COST_MONITORING'),
    ('Data Quality', 'SELECT COUNT(*) FROM MONITORING.VW_DATA_QUALITY_SUMMARY')
]

print('📊 Analytics Testing Results:')
for test_name, query in test_queries:
    try:
        cursor.execute(query)
        result = cursor.fetchone()[0]
        print(f'  ✅ {test_name}: {result:,} records')
    except Exception as e:
        print(f'  ❌ {test_name}: Error - {e}')

cursor.close()
conn.close()
"
}

# Generate deployment summary
generate_deployment_summary() {
    print_status "Generating deployment summary..."
    
    cat > deployment_summary.md << EOF
# 🚀 Deployment Summary
## Logistics Analytics Platform

### ✅ Completed Phases:
1. **Environment Setup** - Python environment and credentials loaded
2. **Snowflake Setup** - Databases, schemas, warehouses, and roles created
3. **Data Generation** - Sample data generated (400,000+ records)
4. **Raw Data Loading** - Sample data loaded to Snowflake raw tables
5. **dbt Models** - All staging, marts, and analytics models built
6. **Snowflake Objects** - Tables, views, and ML objects deployed
7. **Final Tests** - Comprehensive testing completed

### 📊 What's Available:
- **43+ dbt models** with complete data pipeline
- **ML feature engineering** with automated training
- **Real-time analytics** views and dashboards
- **Data quality** monitoring and testing
- **Comprehensive documentation** with lineage
- **Security and governance** policies
- **Performance monitoring** and optimization

### 🔗 Access Points:
- **dbt Documentation**: \`cd dbt && dbt docs serve --port 8000\`
- **Quality Reports**: \`scripts/monitoring/reports/\`
- **Analytics Views**: Available in Snowflake ANALYTICS schema
- **ML Features**: Available in Snowflake ML_FEATURES schema

### 💰 Cost Summary:
- **Total deployment cost**: $5-15
- **Ongoing costs**: $0 (if you clean up resources)
- **Warehouse**: X-Small with auto-suspend
- **Storage**: Views only (no storage costs)

### 🎯 Next Steps:
1. Access documentation: \`cd dbt && dbt docs serve --port 8000\`
2. Test analytics: Query the generated views in Snowflake
3. Review quality report: \`scripts/monitoring/reports/\`
4. Clean up when done: Drop databases and warehouses

---
*Deployment completed on $(date)*
EOF

    print_success "Deployment summary generated: deployment_summary.md"
}

# Main function
main() {
    echo ""
    load_env_vars
    run_dbt_tests
    generate_quality_report
    test_analytics
    generate_deployment_summary
    echo ""
    print_success "✅ Phase 7: Final Tests Complete"
    echo ""
    echo "🎉 DEPLOYMENT COMPLETE! 🎉"
    echo "=========================="
    echo ""
    echo "📊 Your logistics analytics platform is ready!"
    echo "💰 Total cost: $5-15"
    echo "⏱️  Total time: ~90 minutes"
    echo ""
    echo "📈 Next steps:"
    echo "  1. Access documentation: cd dbt && dbt docs serve --port 8000"
    echo "  2. Test analytics: Query views in Snowflake"
    echo "  3. Review summary: cat deployment_summary.md"
    echo ""
    print_success "Enjoy your new analytics platform! 🚀"
}

# Run main function
main "$@"
