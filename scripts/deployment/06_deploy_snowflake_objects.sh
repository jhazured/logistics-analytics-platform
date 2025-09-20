#!/bin/bash
# 🏗️ Phase 6: Deploy Snowflake Objects
# Creates tables, views, and other Snowflake objects

set -e

echo "🏗️ Phase 6: Deploy Snowflake Objects"
echo "===================================="

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

# Execute SQL script
execute_sql_script() {
    local script_path="$1"
    local description="$2"
    
    print_status "$description"
    
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

# Read and execute the SQL script
with open('$script_path', 'r') as f:
    sql_content = f.read()

# Execute the script
cursor = conn.cursor()
try:
    # Split by semicolon and execute each statement
    statements = [stmt.strip() for stmt in sql_content.split(';') if stmt.strip()]
    for statement in statements:
        if statement and not statement.startswith('--'):
            cursor.execute(statement)
    print('✅ $description completed successfully!')
except Exception as e:
    print(f'❌ Error during $description: {e}')
    raise
finally:
    cursor.close()
    conn.close()
"
    
    if [ $? -eq 0 ]; then
        print_success "$description completed"
    else
        print_warning "$description had issues - continuing"
    fi
}

# Deploy dimension tables
deploy_dimension_tables() {
    print_status "Deploying dimension tables..."
    
    # Execute all dimension table scripts
    for script in snowflake/tables/dimensions/*.sql; do
        if [ -f "$script" ]; then
            local table_name=$(basename "$script" .sql)
            execute_sql_script "$script" "Creating dimension table $table_name"
        fi
    done
}

# Deploy fact tables
deploy_fact_tables() {
    print_status "Deploying fact tables..."
    
    # Execute all fact table scripts
    for script in snowflake/tables/facts/*.sql; do
        if [ -f "$script" ]; then
            local table_name=$(basename "$script" .sql)
            execute_sql_script "$script" "Creating fact table $table_name"
        fi
    done
}

# Deploy views
deploy_views() {
    print_status "Deploying views..."
    
    # Execute all view scripts
    for script in snowflake/views/*/*.sql; do
        if [ -f "$script" ]; then
            local view_name=$(basename "$script" .sql)
            execute_sql_script "$script" "Creating view $view_name"
        fi
    done
}

# Deploy ML objects
deploy_ml_objects() {
    print_status "Deploying ML objects..."
    
    # Execute all ML object scripts
    for script in snowflake/ml_objects/*/*.sql; do
        if [ -f "$script" ]; then
            local object_name=$(basename "$script" .sql)
            execute_sql_script "$script" "Creating ML object $object_name"
        fi
    done
}

# Deploy monitoring objects
deploy_monitoring_objects() {
    print_status "Deploying monitoring objects..."
    
    # Execute monitoring scripts
    execute_sql_script "scripts/monitoring/real_time/real_time_kpis.sql" "Creating real-time KPIs"
    execute_sql_script "scripts/monitoring/alerting/alert_system.sql" "Setting up alert system"
    execute_sql_script "scripts/monitoring/alerting/email_alerting_system.sql" "Setting up email alerting"
}

# Deploy performance objects
deploy_performance_objects() {
    print_status "Deploying performance objects..."
    
    # Execute performance scripts
    for script in scripts/performance/*/*.sql; do
        if [ -f "$script" ]; then
            local object_name=$(basename "$script" .sql)
            execute_sql_script "$script" "Creating performance object $object_name"
        fi
    done
}

# Deploy security objects
deploy_security_objects() {
    print_status "Deploying security objects..."
    
    # Execute security scripts
    execute_sql_script "scripts/security/audit_logging.sql" "Setting up audit logging"
    execute_sql_script "scripts/security/data_classification.sql" "Setting up data classification"
    execute_sql_script "scripts/security/row_level_security.sql" "Setting up row-level security"
    execute_sql_script "scripts/security/data_masking_policies.sql" "Setting up data masking"
}

# Deploy streaming objects
deploy_streaming_objects() {
    print_status "Deploying streaming objects..."
    
    # Execute streaming scripts
    execute_sql_script "scripts/streaming/streams/create_streams.sql" "Creating streams"
    execute_sql_script "scripts/streaming/tasks/create_tasks.sql" "Creating tasks"
    execute_sql_script "scripts/streaming/tasks/deploy_streams_and_tasks.sql" "Deploying streams and tasks"
}

# Deploy governance objects
deploy_governance_objects() {
    print_status "Deploying governance objects..."
    
    # Execute governance scripts
    execute_sql_script "scripts/governance/advanced_data_lineage.sql" "Setting up data lineage"
}

# Main function
main() {
    echo ""
    load_env_vars
    
    # Deploy objects in dependency order
    deploy_dimension_tables
    deploy_fact_tables
    deploy_views
    deploy_ml_objects
    deploy_monitoring_objects
    deploy_performance_objects
    deploy_security_objects
    deploy_streaming_objects
    deploy_governance_objects
    
    echo ""
    print_success "✅ Phase 6: Snowflake Objects Deployed"
    echo ""
    echo "Next: Run 07_run_final_tests.sh"
}

# Run main function
main "$@"
