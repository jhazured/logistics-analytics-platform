#!/bin/bash

# Deployment script for streams and tasks
# Run this after dbt models are deployed to create streams and tasks

set -e  # Exit on any error

# Configuration
SNOWFLAKE_ACCOUNT="${SNOWFLAKE_ACCOUNT:-}"
SNOWFLAKE_USER="${SNOWFLAKE_USER:-}"
SNOWFLAKE_PASSWORD="${SNOWFLAKE_PASSWORD:-}"
SNOWFLAKE_WAREHOUSE="${SNOWFLAKE_WAREHOUSE:-COMPUTE_WH}"
SNOWFLAKE_DATABASE="${SNOWFLAKE_DATABASE:-LOGISTICS_ANALYTICS}"
SNOWFLAKE_SCHEMA="${SNOWFLAKE_SCHEMA:-MARTS}"

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
    if [[ -z "$SNOWFLAKE_ACCOUNT" || -z "$SNOWFLAKE_USER" || -z "$SNOWFLAKE_PASSWORD" ]]; then
        error "Required Snowflake environment variables not set. Please set SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, and SNOWFLAKE_PASSWORD"
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
        snowsql -a "$SNOWFLAKE_ACCOUNT" -u "$SNOWFLAKE_USER" -p "$SNOWFLAKE_PASSWORD" -w "$SNOWFLAKE_WAREHOUSE" -d "$SNOWFLAKE_DATABASE" -s "$SNOWFLAKE_SCHEMA" -f "$sql_file"
    elif command -v snowflake &> /dev/null; then
        snowflake -a "$SNOWFLAKE_ACCOUNT" -u "$SNOWFLAKE_USER" -p "$SNOWFLAKE_PASSWORD" -w "$SNOWFLAKE_WAREHOUSE" -d "$SNOWFLAKE_DATABASE" -s "$SNOWFLAKE_SCHEMA" -f "$sql_file"
    else
        error "Neither snowsql nor snowflake CLI found. Please install one of them."
    fi
    
    if [[ $? -eq 0 ]]; then
        log "Successfully executed: $description"
    else
        error "Failed to execute: $description"
    fi
}

# Main deployment function
deploy_streams_and_tasks() {
    log "Starting streams and tasks deployment..."
    
    # Step 1: Create streams
    log "Step 1: Creating streams on fact tables..."
    execute_sql "tasks/01_create_streams.sql" "Create streams on fact tables"
    
    # Step 2: Create monitoring tables
    log "Step 2: Creating monitoring tables..."
    execute_sql "tasks/02_create_monitoring_tables.sql" "Create monitoring tables"
    
    # Step 3: Create tasks
    log "Step 3: Creating and enabling tasks..."
    execute_sql "tasks/03_create_tasks.sql" "Create and enable tasks"
    
    # Step 4: Verify deployment
    log "Step 4: Verifying deployment..."
    execute_sql "tasks/99_verify_deployment.sql" "Verify streams and tasks deployment"
    
    log "Streams and tasks deployment completed successfully!"
}

# Cleanup function
cleanup() {
    log "Cleaning up temporary files..."
    # Add any cleanup logic here
}

# Trap to ensure cleanup on exit
trap cleanup EXIT

# Main execution
main() {
    log "=== Streams and Tasks Deployment Script ==="
    
    check_env
    deploy_streams_and_tasks
    
    log "=== Deployment completed successfully ==="
}

# Run main function
main "$@"
