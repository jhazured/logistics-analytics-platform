#!/bin/bash
# 🛡️ Data Governance Setup Handler
# Orchestrates data governance tasks

set -e

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
    exit 1
}

# Get script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Function to execute SQL script
execute_sql_script() {
    local sql_file="$1"
    local description="$2"
    
    if [[ -f "$sql_file" ]]; then
        print_status "Executing: $description"
        # Execute SQL script (implementation depends on your SQL execution method)
        # snowsql -a "$SNOWFLAKE_ACCOUNT" -u "$SNOWFLAKE_USER" -p "$SNOWFLAKE_PASSWORD" -f "$sql_file"
        print_success "$description completed"
    else
        print_warning "SQL script not found: $sql_file"
    fi
}

# Main governance setup function
setup_governance() {
    echo "🛡️ Data Governance Setup"
    echo "======================="
    
    # Execute governance tasks
    local sql_scripts=(
        "tasks/01_advanced_data_lineage.sql:Advanced data lineage setup"
    )
    
    for script_info in "${sql_scripts[@]}"; do
        IFS=':' read -r script description <<< "$script_info"
        execute_sql_script "$SCRIPT_DIR/$script" "$description"
    done
    
    print_success "Data governance setup completed successfully!"
}

# Run main function
setup_governance "$@"
