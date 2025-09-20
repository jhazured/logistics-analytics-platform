#!/bin/bash
# 🗄️ Phase 2: Snowflake Setup
# Uses existing SQL setup scripts in proper order

set -e

echo "🗄️ Phase 2: Snowflake Setup"
echo "============================"

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
    warehouse=os.getenv('SF_WAREHOUSE')
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
        print_error "$description failed"
        exit 1
    fi
}

# Setup Snowflake environment using existing scripts
setup_snowflake() {
    print_status "Setting up Snowflake environment using existing scripts..."
    
    # Execute setup scripts in order
    execute_sql_script "scripts/setup/01_database_setup.sql" "Creating databases"
    execute_sql_script "scripts/setup/02_schema_creation.sql" "Creating schemas"
    execute_sql_script "scripts/setup/03_warehouse_configuration.sql" "Configuring warehouses"
    execute_sql_script "scripts/setup/04_user_roles_permissions.sql" "Setting up roles and permissions"
    execute_sql_script "scripts/setup/05_resource_monitors.sql" "Creating resource monitors"
}

# Verify setup
verify_setup() {
    print_status "Verifying Snowflake setup..."
    
    execute_sql_script "scripts/setup/99_verify_setup.sql" "Verifying setup"
}

# Main function
main() {
    echo ""
    load_env_vars
    setup_snowflake
    verify_setup
    echo ""
    print_success "✅ Phase 2: Snowflake Setup Complete"
    echo ""
    echo "Next: Run 03_generate_data.sh"
}

# Run main function
main "$@"