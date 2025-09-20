#!/bin/bash

# =====================================================
# Parameterized SQL Execution Script
# =====================================================
# This script executes SQL files with environment variables
# Usage: ./execute_sql.sh <sql_file> [environment]
# Example: ./execute_sql.sh 01_database_setup.sql dev

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to print colored output
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

# Check if SQL file is provided
if [ $# -lt 1 ]; then
    print_error "Usage: $0 <sql_file> [environment]"
    print_error "Example: $0 01_database_setup.sql dev"
    exit 1
fi

SQL_FILE="$1"
ENVIRONMENT="${2:-dev}"

# Check if SQL file exists
if [ ! -f "$SQL_FILE" ]; then
    print_error "SQL file not found: $SQL_FILE"
    exit 1
fi

print_status "Executing SQL file: $SQL_FILE"
print_status "Environment: $ENVIRONMENT"

# Load environment configuration
if [ -f "scripts/01_setup/handlers/configure_environment.sh" ]; then
    print_status "Loading environment configuration..."
    source scripts/01_setup/handlers/configure_environment.sh "$ENVIRONMENT"
else
    print_warning "Environment configuration script not found, using current environment variables"
fi

# Check if required environment variables are set
if [ -z "$SF_ACCOUNT" ] || [ -z "$SF_USER" ] || [ -z "$SF_PASSWORD" ]; then
    print_error "Required Snowflake environment variables not set:"
    print_error "  SF_ACCOUNT: ${SF_ACCOUNT:-'NOT SET'}"
    print_error "  SF_USER: ${SF_USER:-'NOT SET'}"
    print_error "  SF_PASSWORD: ${SF_PASSWORD:-'NOT SET'}"
    print_error "Please set these variables or run configure_environment.sh first"
    exit 1
fi

# Set default values if not provided
export SF_DATABASE="${SF_DATABASE:-LOGISTICS_DW_DEV}"
export SF_WAREHOUSE="${SF_WAREHOUSE:-COMPUTE_WH_XS}"
export SF_SCHEMA="${SF_SCHEMA:-ANALYTICS}"
export SF_ROLE="${SF_ROLE:-ACCOUNTADMIN}"

print_status "Using configuration:"
print_status "  Database: $SF_DATABASE"
print_status "  Warehouse: $SF_WAREHOUSE"
print_status "  Schema: $SF_SCHEMA"
print_status "  Role: $SF_ROLE"

# Create temporary SQL file with environment variables substituted
TEMP_SQL_FILE="/tmp/$(basename "$SQL_FILE")_$(date +%s).sql"

# Check if snowsql is available
if ! command -v snowsql &> /dev/null; then
    print_error "snowsql command not found. Please install SnowSQL or use Python connector."
    print_status "Attempting to use Python connector instead..."
    
    # Use Python script for execution
    python3 << EOF
import snowflake.connector
import os
import sys

# Get environment variables
account = os.getenv('SF_ACCOUNT')
user = os.getenv('SF_USER')
password = os.getenv('SF_PASSWORD')
role = os.getenv('SF_ROLE', 'ACCOUNTADMIN')
warehouse = os.getenv('SF_WAREHOUSE', 'COMPUTE_WH_XS')
database = os.getenv('SF_DATABASE', 'LOGISTICS_DW_DEV')
schema = os.getenv('SF_SCHEMA', 'ANALYTICS')

try:
    # Connect to Snowflake
    conn = snowflake.connector.connect(
        account=account,
        user=user,
        password=password,
        role=role,
        warehouse=warehouse,
        database=database,
        schema=schema
    )
    
    # Read and execute SQL file
    with open('$SQL_FILE', 'r') as f:
        sql_content = f.read()
    
    # Split by semicolon and execute each statement
    statements = [stmt.strip() for stmt in sql_content.split(';') if stmt.strip()]
    
    cursor = conn.cursor()
    for statement in statements:
        if statement:
            print(f"Executing: {statement[:100]}...")
            cursor.execute(statement)
    
    cursor.close()
    conn.close()
    print("✅ SQL execution completed successfully!")
    
except Exception as e:
    print(f"❌ Error executing SQL: {e}")
    sys.exit(1)
EOF
else
    # Use snowsql for execution
    print_status "Using SnowSQL for execution..."
    
    # Execute SQL file with snowsql
    snowsql -a "$SF_ACCOUNT" -u "$SF_USER" -p "$SF_PASSWORD" -r "$SF_ROLE" -w "$SF_WAREHOUSE" -d "$SF_DATABASE" -s "$SF_SCHEMA" -f "$SQL_FILE"
fi

if [ $? -eq 0 ]; then
    print_success "SQL file executed successfully: $SQL_FILE"
else
    print_error "Failed to execute SQL file: $SQL_FILE"
    exit 1
fi
