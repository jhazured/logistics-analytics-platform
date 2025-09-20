#!/bin/bash
# Data Loading Script for Logistics Analytics Platform
# ===================================================
# This script provides easy access to data loading functionality

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
}

# Get script directory and project root
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"

# Load environment variables from .env file
if [[ -f "$PROJECT_ROOT/.env" ]]; then
    set -a  # automatically export all variables
    source "$PROJECT_ROOT/.env"
    set +a  # stop automatically exporting
    print_status "Environment variables loaded from .env file"
else
    print_warning "No .env file found. Make sure environment variables are set."
fi

# Function to show usage
show_usage() {
    echo "Data Loading Script for Logistics Analytics Platform"
    echo "===================================================="
    echo ""
    echo "Usage: $0 [COMMAND] [OPTIONS]"
    echo ""
    echo "Commands:"
    echo "  load-csv FILE TABLE          Load CSV file into Snowflake table"
    echo "  load-json FILE TABLE         Load JSON file into Snowflake table"
    echo "  generate-sample [COUNT]      Generate sample data (default: 1000 records)"
    echo "  generate-table TABLE COUNT   Generate sample data for specific table"
    echo "  table-info TABLE             Show table information"
    echo "  list-tables                  List all tables in RAW schema"
    echo "  help                         Show this help message"
    echo ""
    echo "Examples:"
    echo "  $0 load-csv data/customers.csv RAW.CUSTOMERS"
    echo "  $0 load-json data/shipments.json RAW.SHIPMENTS"
    echo "  $0 generate-sample 5000"
    echo "  $0 generate-table customers 1000"
    echo "  $0 table-info RAW.CUSTOMERS"
    echo "  $0 list-tables"
    echo ""
    echo "Environment Variables Required:"
    echo "  SF_ACCOUNT, SF_USER, SF_PASSWORD, SF_ROLE, SF_WAREHOUSE, SF_DATABASE, SF_SCHEMA"
}

# Function to check if Python dependencies are installed
check_dependencies() {
    print_status "Checking Python dependencies..."
    
    # Check if required packages are installed
    python3 -c "import pandas, snowflake.connector" 2>/dev/null || {
        print_error "Missing required Python packages. Installing..."
        pip3 install pandas snowflake-connector-python
    }
    
    print_success "Dependencies check completed"
}

# Function to load CSV file
load_csv() {
    local file_path="$1"
    local table_name="$2"
    
    if [[ -z "$file_path" || -z "$table_name" ]]; then
        print_error "Usage: $0 load-csv FILE TABLE"
        exit 1
    fi
    
    if [[ ! -f "$file_path" ]]; then
        print_error "File not found: $file_path"
        exit 1
    fi
    
    print_status "Loading CSV file: $file_path -> $table_name"
    python3 "$SCRIPT_DIR/data_loader.py" load-csv --file "$file_path" --table "$table_name"
    
    if [[ $? -eq 0 ]]; then
        print_success "CSV file loaded successfully"
    else
        print_error "Failed to load CSV file"
        exit 1
    fi
}

# Function to load JSON file
load_json() {
    local file_path="$1"
    local table_name="$2"
    
    if [[ -z "$file_path" || -z "$table_name" ]]; then
        print_error "Usage: $0 load-json FILE TABLE"
        exit 1
    fi
    
    if [[ ! -f "$file_path" ]]; then
        print_error "File not found: $file_path"
        exit 1
    fi
    
    print_status "Loading JSON file: $file_path -> $table_name"
    python3 "$SCRIPT_DIR/data_loader.py" load-json --file "$file_path" --table "$table_name"
    
    if [[ $? -eq 0 ]]; then
        print_success "JSON file loaded successfully"
    else
        print_error "Failed to load JSON file"
        exit 1
    fi
}

# Function to generate sample data
generate_sample() {
    local count="${1:-1000}"
    
    print_status "Generating $count sample records for all tables"
    python3 "$SCRIPT_DIR/sample_data_generator.py" --count "$count"
    
    if [[ $? -eq 0 ]]; then
        print_success "Sample data generation completed"
    else
        print_error "Failed to generate sample data"
        exit 1
    fi
}

# Function to generate data for specific table
generate_table() {
    local table_name="$1"
    local count="${2:-1000}"
    
    if [[ -z "$table_name" ]]; then
        print_error "Usage: $0 generate-table TABLE [COUNT]"
        exit 1
    fi
    
    print_status "Generating $count sample records for table: $table_name"
    python3 "$SCRIPT_DIR/sample_data_generator.py" --table "$table_name" --count "$count"
    
    if [[ $? -eq 0 ]]; then
        print_success "Sample data generation completed for $table_name"
    else
        print_error "Failed to generate sample data for $table_name"
        exit 1
    fi
}

# Function to show table information
table_info() {
    local table_name="$1"
    
    if [[ -z "$table_name" ]]; then
        print_error "Usage: $0 table-info TABLE"
        exit 1
    fi
    
    print_status "Getting information for table: $table_name"
    python3 "$SCRIPT_DIR/data_loader.py" table-info --table "$table_name"
}

# Function to list tables
list_tables() {
    print_status "Listing all tables in RAW schema"
    python3 -c "
import os
import snowflake.connector

# Connect to Snowflake
conn = snowflake.connector.connect(
    account=os.getenv('SF_ACCOUNT'),
    user=os.getenv('SF_USER'),
    password=os.getenv('SF_PASSWORD'),
    role=os.getenv('SF_ROLE', 'ACCOUNTADMIN'),
    warehouse=os.getenv('SF_WAREHOUSE', 'COMPUTE_WH_XS'),
    database=os.getenv('SF_DATABASE', 'LOGISTICS_DW_DEV'),
    schema=os.getenv('SF_SCHEMA', 'RAW')
)

cursor = conn.cursor()
cursor.execute('SHOW TABLES IN SCHEMA RAW')
tables = cursor.fetchall()

print('Tables in RAW schema:')
for table in tables:
    print(f'  - {table[1]}')

cursor.close()
conn.close()
"
}

# Main script logic
main() {
    # Check dependencies
    check_dependencies
    
    # Parse command
    case "${1:-help}" in
        "load-csv")
            load_csv "$2" "$3"
            ;;
        "load-json")
            load_json "$2" "$3"
            ;;
        "generate-sample")
            generate_sample "$2"
            ;;
        "generate-table")
            generate_table "$2" "$3"
            ;;
        "table-info")
            table_info "$2"
            ;;
        "list-tables")
            list_tables
            ;;
        "help"|"--help"|"-h")
            show_usage
            ;;
        *)
            print_error "Unknown command: $1"
            echo ""
            show_usage
            exit 1
            ;;
    esac
}

# Run main function
main "$@"
