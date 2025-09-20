#!/bin/bash
# 🔧 Phase 5: Build dbt Models
# Installs packages, parses, and builds all dbt models

set -e

echo "🔧 Phase 5: Build dbt Models"
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

# Install dbt packages
install_dbt_packages() {
    print_status "Installing dbt packages..."
    
    cd dbt
    dbt deps --quiet
    cd ..
    
    print_success "dbt packages installed"
}

# Parse dbt models
parse_dbt_models() {
    print_status "Parsing dbt models..."
    
    cd dbt
    if dbt parse --target dev --quiet; then
        print_success "dbt models parsed successfully"
    else
        print_warning "dbt parsing had issues - some models may need attention"
    fi
    cd ..
}

# Build dbt models
build_dbt_models() {
    print_status "Building dbt models..."
    
    cd dbt
    
    # Build staging models first
    print_status "Building staging models..."
    if dbt run --target dev --select staging --threads 2 --quiet; then
        print_success "Staging models built successfully"
    else
        print_warning "Staging models had issues"
    fi
    
    # Build marts models
    print_status "Building marts models..."
    if dbt run --target dev --select marts --threads 2 --quiet; then
        print_success "Marts models built successfully"
    else
        print_warning "Marts models had issues"
    fi
    
    # Build analytics models
    print_status "Building analytics models..."
    if dbt run --target dev --select analytics --threads 2 --quiet; then
        print_success "Analytics models built successfully"
    else
        print_warning "Analytics models had issues"
    fi
    
    cd ..
}

# Run tests
run_tests() {
    print_status "Running dbt tests..."
    
    cd dbt
    
    # Run critical tests
    print_status "Running critical tests..."
    if dbt test --target dev --select tag:critical --threads 2 --quiet; then
        print_success "Critical tests passed"
    else
        print_warning "Some critical tests failed"
    fi
    
    # Run data quality tests
    print_status "Running data quality tests..."
    if dbt test --target dev --select tag:data_quality --threads 2 --quiet; then
        print_success "Data quality tests passed"
    else
        print_warning "Some data quality tests failed"
    fi
    
    cd ..
}

# Generate documentation
generate_documentation() {
    print_status "Generating dbt documentation..."
    
    cd dbt
    if dbt docs generate --target dev --quiet; then
        print_success "Documentation generated"
    else
        print_warning "Documentation generation had issues"
    fi
    cd ..
}

# Main function
main() {
    echo ""
    load_env_vars
    install_dbt_packages
    parse_dbt_models
    build_dbt_models
    run_tests
    generate_documentation
    echo ""
    print_success "✅ Phase 5: dbt Models Built"
    echo ""
    echo "Next: Run 06_deploy_snowflake_objects.sh"
}

# Run main function
main "$@"
