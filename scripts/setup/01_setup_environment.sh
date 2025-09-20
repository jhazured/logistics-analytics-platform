#!/bin/bash
# 🔧 Phase 1: Environment Setup
# Sets up Python environment and loads credentials

set -e

echo "🔧 Phase 1: Environment Setup"
echo "============================="

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

# Load environment variables from .env file
load_env_vars() {
    print_status "Loading environment variables from .env file..."
    
    # Navigate to project root
    cd "$(dirname "$0")/../.."
    
    if [ -f ".env" ]; then
        # Load environment variables from .env file
        set -a  # automatically export all variables
        source .env
        set +a  # stop automatically exporting
        print_success "Environment variables loaded from .env file"
    else
        print_error ".env file not found!"
        echo "Please create a .env file with your Snowflake credentials:"
        echo "1. Copy .env.example to .env"
        echo "2. Fill in your actual Snowflake credentials"
        echo "3. Run this script again"
        echo ""
        echo "Example:"
        echo "  cp .env.example .env"
        echo "  # Edit .env with your credentials"
        echo "  ./scripts/deployment/01_setup_environment.sh"
        exit 1
    fi
    
    # Validate required variables
    if [ -z "$SF_ACCOUNT" ] || [ -z "$SF_USER" ] || [ -z "$SF_PASSWORD" ]; then
        print_error "Required environment variables are missing!"
        echo "Please check your .env file and ensure SF_ACCOUNT, SF_USER, and SF_PASSWORD are set."
        exit 1
    fi
    
    print_success "Environment variables validated"
}

# Setup Python environment
setup_python_env() {
    print_status "Setting up Python environment..."
    
    # Create virtual environment if it doesn't exist
    if [ ! -d "venv" ]; then
        python -m venv venv
        print_success "Virtual environment created"
    fi
    
    # Activate virtual environment
    source venv/bin/activate
    
    # Install required packages
    pip install -q dbt-snowflake scikit-learn pandas numpy joblib flask python-dotenv snowflake-connector-python
    
    print_success "Python environment ready"
}

# Test Snowflake connection
test_snowflake_connection() {
    print_status "Testing Snowflake connection..."
    
    python -c "
import snowflake.connector
import os

try:
    conn = snowflake.connector.connect(
        account=os.getenv('SF_ACCOUNT'),
        user=os.getenv('SF_USER'),
        password=os.getenv('SF_PASSWORD'),
        role=os.getenv('SF_ROLE'),
        warehouse=os.getenv('SF_WAREHOUSE')
    )
    print('✅ Snowflake connection successful!')
    
    # Test basic query
    cursor = conn.cursor()
    cursor.execute('SELECT CURRENT_VERSION()')
    version = cursor.fetchone()[0]
    print(f'📊 Snowflake version: {version}')
    
    cursor.close()
    conn.close()
    
except Exception as e:
    print(f'❌ Connection failed: {e}')
    exit(1)
"
    
    if [ $? -eq 0 ]; then
        print_success "Snowflake connection verified"
    else
        print_error "Snowflake connection failed"
        exit 1
    fi
}

# Main function
main() {
    echo ""
    load_env_vars
    setup_python_env
    test_snowflake_connection
    echo ""
    print_success "✅ Phase 1: Environment Setup Complete"
    echo ""
    echo "Next: Run 02_setup_snowflake.sh"
}

# Run main function
main "$@"
