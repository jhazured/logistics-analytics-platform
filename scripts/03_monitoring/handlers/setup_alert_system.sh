#!/bin/bash

# Real-time Alert System Setup Script
# This script sets up the real-time vehicle tracking and alert system

set -e  # Exit on any error

# Configuration
SF_ACCOUNT="${SF_ACCOUNT:-}"
SF_USER="${SF_USER:-}"
SF_PASSWORD="${SF_PASSWORD:-}"
SNOWFLAKE_WAREHOUSE="${SNOWFLAKE_WAREHOUSE:-COMPUTE_WH}"
SNOWFLAKE_DATABASE="${SNOWFLAKE_DATABASE:-LOGISTICS_DW_PROD}"
SNOWFLAKE_SCHEMA="${SNOWFLAKE_SCHEMA:-MONITORING}"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Logging function
log() {
    echo -e "${GREEN}[$(date +'%Y-%m-%d %H:%M:%S')] $1${NC}"
}

error() {
    echo -e "${RED}[$(date +'%Y-%m-%d %H:%M:%S')] ERROR: $1${NC}"
    exit 1
}

warn() {
    echo -e "${YELLOW}[$(date +'%Y-%m-%d %H:%M:%S')] WARNING: $1${NC}"
}

# Check if required environment variables are set
check_env() {
    if [[ -z "$SF_ACCOUNT" || -z "$SF_USER" || -z "$SF_PASSWORD" ]]; then
        error "Required Snowflake environment variables not set. Please set SF_ACCOUNT, SF_USER, and SF_PASSWORD"
    fi
}

# Execute SQL file
execute_sql() {
    local sql_file="$1"
    local description="$2"
    
    log "Executing: $description"
    
    if [[ ! -f "$sql_file" ]]; then
        error "SQL file not found: $sql_file"
    fi
    
    # Execute SQL using snowsql or snowflake CLI
    if command -v snowsql &> /dev/null; then
        snowsql -a "$SF_ACCOUNT" -u "$SF_USER" -p "$SF_PASSWORD" -w "$SNOWFLAKE_WAREHOUSE" -d "$SNOWFLAKE_DATABASE" -s "$SNOWFLAKE_SCHEMA" -f "$sql_file"
    elif command -v snowflake &> /dev/null; then
        snowflake -a "$SF_ACCOUNT" -u "$SF_USER" -p "$SF_PASSWORD" -w "$SNOWFLAKE_WAREHOUSE" -d "$SNOWFLAKE_DATABASE" -s "$SNOWFLAKE_SCHEMA" -f "$sql_file"
    else
        error "Neither snowsql nor snowflake CLI found. Please install one of them."
    fi
    
    if [[ $? -eq 0 ]]; then
        log "Successfully executed: $description"
    else
        error "Failed to execute: $description"
    fi
}

# Main setup function
setup_alert_system() {
    log "Starting alert system setup..."
    
    # Step 1: Create alert tables
    log "Step 1: Creating alert tables..."
    execute_sql "tasks/01_create_alert_tables.sql" "Create alert tables"
    
    # Step 2: Create monitoring tasks
    log "Step 2: Creating monitoring tasks..."
    execute_sql "tasks/02_create_monitoring_tasks.sql" "Create monitoring tasks"
    
    # Step 3: Set up email alerting
    log "Step 3: Setting up email alerting..."
    execute_sql "tasks/03_setup_email_alerting.sql" "Set up email alerting system"
    
    # Step 4: Verify setup
    log "Step 4: Verifying alert system setup..."
    execute_sql "tasks/99_verify_alert_setup.sql" "Verify alert system setup"
    
    log "Alert system setup completed successfully!"
}

# Main execution
main() {
    log "=== Real-time Alert System Setup Script ==="
    
    check_env
    setup_alert_system
    
    log "=== Alert system setup completed successfully ==="
}

# Run main function
main "$@"
