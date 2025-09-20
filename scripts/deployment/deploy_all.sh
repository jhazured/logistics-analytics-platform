#!/bin/bash
# 🚀 Master Deployment Script
# Orchestrates the complete deployment process

set -e

echo "🚀 Logistics Analytics Platform - Complete Deployment"
echo "====================================================="

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

# Get script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Function to run a deployment phase
run_phase() {
    local phase_script="$1"
    local phase_name="$2"
    local script_path=""
    
    print_status "Starting $phase_name..."
    
    # Check if it's a setup script (moved to setup folder)
    if [[ "$phase_script" == "01_setup_environment.sh" || "$phase_script" == "02_setup_snowflake.sh" ]]; then
        script_path="$(dirname "$SCRIPT_DIR")/setup/$phase_script"
    else
        script_path="$SCRIPT_DIR/$phase_script"
    fi
    
    if [ -f "$script_path" ]; then
        if bash "$script_path"; then
            print_success "$phase_name completed successfully"
            return 0
        else
            print_error "$phase_name failed"
            return 1
        fi
    else
        print_error "Phase script $script_path not found"
        return 1
    fi
}

# Main deployment function
main() {
    echo ""
    print_status "Starting complete deployment process..."
    echo ""
    
    # Phase 1: Environment Setup
    if ! run_phase "01_setup_environment.sh" "Phase 1: Environment Setup"; then
        print_error "Deployment failed at Phase 1"
        exit 1
    fi
    echo ""
    
    # Phase 2: Snowflake Setup
    if ! run_phase "02_setup_snowflake.sh" "Phase 2: Snowflake Setup"; then
        print_error "Deployment failed at Phase 2"
        exit 1
    fi
    echo ""
    
    # Phase 3: Generate Data
    if ! run_phase "03_generate_data.sh" "Phase 3: Generate Data"; then
        print_error "Deployment failed at Phase 3"
        exit 1
    fi
    echo ""
    
    # Phase 4: Load Raw Data
    if ! run_phase "04_load_raw_data.sh" "Phase 4: Load Raw Data"; then
        print_error "Deployment failed at Phase 4"
        exit 1
    fi
    echo ""
    
    # Phase 5: Build dbt Models
    if ! run_phase "05_build_dbt_models.sh" "Phase 5: Build dbt Models"; then
        print_error "Deployment failed at Phase 5"
        exit 1
    fi
    echo ""
    
    # Phase 6: Deploy Snowflake Objects
    if ! run_phase "06_deploy_snowflake_objects.sh" "Phase 6: Deploy Snowflake Objects"; then
        print_error "Deployment failed at Phase 6"
        exit 1
    fi
    echo ""
    
    # Phase 7: Run Final Tests
    if ! run_phase "07_run_final_tests.sh" "Phase 7: Run Final Tests"; then
        print_error "Deployment failed at Phase 7"
        exit 1
    fi
    echo ""
    
    # Success message
    echo "======================================================"
    print_success "🎉 COMPLETE DEPLOYMENT SUCCESSFUL! 🎉"
    echo "======================================================"
    echo ""
    echo "📊 Your logistics analytics platform is ready!"
    echo "💰 Total cost: $5-15"
    echo "⏱️  Total time: ~90 minutes"
    echo ""
    echo "📈 Quick Start:"
    echo "  1. Access documentation: cd dbt && dbt docs serve --port 8000"
    echo "  2. Test analytics: Query views in Snowflake"
    echo "  3. Review summary: cat deployment_summary.md"
    echo ""
    print_success "Enjoy your new analytics platform! 🚀"
}

# Check if running individual phase
if [ $# -eq 1 ]; then
    case "$1" in
        "1"|"env"|"environment")
            run_phase "01_setup_environment.sh" "Phase 1: Environment Setup"
            ;;
        "2"|"snowflake"|"setup")
            run_phase "02_setup_snowflake.sh" "Phase 2: Snowflake Setup"
            ;;
        "3"|"data"|"generate")
            run_phase "03_generate_data.sh" "Phase 3: Generate Data"
            ;;
        "4"|"load"|"raw")
            run_phase "04_load_raw_data.sh" "Phase 4: Load Raw Data"
            ;;
        "5"|"dbt"|"models")
            run_phase "05_build_dbt_models.sh" "Phase 5: Build dbt Models"
            ;;
        "6"|"objects"|"deploy")
            run_phase "06_deploy_snowflake_objects.sh" "Phase 6: Deploy Snowflake Objects"
            ;;
        "7"|"tests"|"final")
            run_phase "07_run_final_tests.sh" "Phase 7: Run Final Tests"
            ;;
        "help"|"-h"|"--help")
            echo "🚀 Logistics Analytics Platform Deployment"
            echo ""
            echo "Usage: $0 [phase]"
            echo ""
            echo "Phases:"
            echo "  1, env, environment     - Setup environment and credentials"
            echo "  2, snowflake, setup     - Setup Snowflake infrastructure"
            echo "  3, data, generate       - Generate sample data"
            echo "  4, load, raw           - Load raw data to Snowflake"
            echo "  5, dbt, models         - Build dbt models"
            echo "  6, objects, deploy     - Deploy Snowflake objects"
            echo "  7, tests, final        - Run final tests"
            echo "  (no args)              - Run all phases"
            echo ""
            echo "Examples:"
            echo "  $0                     # Run complete deployment"
            echo "  $0 1                   # Run only environment setup"
            echo "  $0 dbt                 # Run only dbt models"
            echo "  $0 help                # Show this help"
            ;;
        *)
            print_error "Unknown phase: $1"
            echo "Use '$0 help' for usage information."
            exit 1
            ;;
    esac
else
    # Run complete deployment
    main
fi
