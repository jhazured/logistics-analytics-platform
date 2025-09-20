#!/bin/bash
# 📊 Phase 3: Generate Sample Data
# Generates realistic sample data for all tables

set -e

echo "📊 Phase 3: Generate Sample Data"
echo "================================"

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

# Generate sample data
generate_data() {
    print_status "Generating sample data..."
    
    cd data
    python generate_sample_data.py
    cd ..
    
    print_success "Sample data generated"
}

# Verify data generation
verify_data() {
    print_status "Verifying generated data..."
    
    if [ -d "logistics_sample_data" ]; then
        echo "📁 Generated data files:"
        ls -la logistics_sample_data/*.csv | wc -l | xargs echo "  Total files:"
        ls -la logistics_sample_data/*.csv | head -5
        echo "  ..."
        print_success "Data files verified"
    else
        print_error "Data directory not found"
        exit 1
    fi
}

# Main function
main() {
    echo ""
    load_env_vars
    generate_data
    verify_data
    echo ""
    print_success "✅ Phase 3: Data Generation Complete"
    echo ""
    echo "Next: Run 04_load_raw_data.sh"
}

# Run main function
main "$@"
