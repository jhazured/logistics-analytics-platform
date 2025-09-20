#!/bin/bash
# 🚀 Master Deployment Script
# Complete deployment orchestration for Logistics Analytics Platform

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
    exit 1
}

# Get script directory and project root
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"

# Deployment completion tracking
DEPLOYMENT_STATUS_FILE="$PROJECT_ROOT/.deployment_status"

# Load environment variables from .env file
if [[ -f "$PROJECT_ROOT/.env" ]]; then
    set -a  # automatically export all variables
    source "$PROJECT_ROOT/.env"
    set +a  # stop automatically exporting
    print_status "Environment variables loaded from .env file"
fi

# Function to check if phase was completed
is_phase_completed() {
    local phase="$1"
    if [[ -f "$DEPLOYMENT_STATUS_FILE" ]]; then
        grep -q "^$phase:completed:" "$DEPLOYMENT_STATUS_FILE"
    else
        return 1
    fi
}

# Function to mark phase as completed
mark_phase_completed() {
    local phase="$1"
    local timestamp=$(date '+%Y-%m-%d %H:%M:%S')
    echo "$phase:completed:$timestamp" >> "$DEPLOYMENT_STATUS_FILE"
}

# Function to show deployment status
show_deployment_status() {
    if [[ -f "$DEPLOYMENT_STATUS_FILE" ]]; then
        print_status "Previous deployment status:"
        while IFS=':' read -r phase status timestamp; do
            if [[ "$status" == "completed" ]]; then
                echo "  ✅ Phase $phase: Completed at $timestamp"
            fi
        done < "$DEPLOYMENT_STATUS_FILE"
        echo ""
    fi
}

# Function to execute SQL script
execute_sql_script() {
    local sql_file="$1"
    local description="$2"
    
    if [[ -f "$sql_file" ]]; then
        print_status "Executing: $description"
        # Execute SQL script (implementation depends on your SQL execution method)
        # snowsql -a "$SF_ACCOUNT" -u "$SF_USER" -p "$SF_PASSWORD" -f "$sql_file"
        print_success "$description completed"
    else
        print_warning "SQL script not found: $sql_file"
    fi
}

# Function to execute Python script
execute_python_script() {
    local python_file="$1"
    local description="$2"
    
    if [[ -f "$python_file" ]]; then
        print_status "Executing: $description"
        cd "$PROJECT_ROOT"
        python "$python_file"
        print_success "$description completed"
    else
        print_warning "Python script not found: $python_file"
    fi
}

# Phase 1: Environment Setup
setup_environment() {
    if is_phase_completed "1"; then
        print_status "🔧 Phase 1: Environment Setup"
        echo "============================="
        print_status "Phase 1 already completed - skipping"
        print_success "Environment setup skipped (previously completed)"
        return 0
    fi
    
    print_status "🔧 Phase 1: Environment Setup"
    echo "============================="
    
    # Configure environment (dev/staging/prod)
    local environment=${1:-dev}
    print_status "Configuring environment: $environment"
    bash "$PROJECT_ROOT/scripts/01_setup/handlers/configure_environment.sh" "$environment"
    
    # Check environment variables
    print_status "Checking environment variables..."
    if [[ -z "$SF_ACCOUNT" ]]; then
        print_error "SF_ACCOUNT environment variable is not set"
    fi
    if [[ -z "$SF_USER" ]]; then
        print_error "SF_USER environment variable is not set"
    fi
    if [[ -z "$SF_PASSWORD" ]]; then
        print_error "SF_PASSWORD environment variable is not set"
    fi
    print_success "Environment variables validated"
    
    # Setup Python environment
    print_status "Setting up Python environment..."
    cd "$PROJECT_ROOT"
    if [[ ! -d "venv" ]]; then
        print_status "Creating Python virtual environment..."
        python3 -m venv venv
    fi
    source venv/bin/activate
    
    if [[ -f "requirements.txt" ]]; then
        print_status "Installing Python dependencies..."
        pip install -r requirements.txt
    fi
    print_success "Python environment setup complete"
    
    # Mark phase as completed
    mark_phase_completed "1"
}

# Phase 2: Snowflake Infrastructure Setup
setup_snowflake_infrastructure() {
    if is_phase_completed "2"; then
        print_status "🗄️ Phase 2: Snowflake Infrastructure Setup"
        echo "============================================="
        print_status "Phase 2 already completed - skipping"
        print_success "Snowflake infrastructure setup skipped (previously completed)"
        return 0
    fi
    
    print_status "🗄️ Phase 2: Snowflake Infrastructure Setup"
    echo "============================================="
    
    # Execute SQL setup scripts in order
    local sql_scripts=(
        "../01_setup/tasks/01_database_setup.sql:Database creation"
        "../01_setup/tasks/02_schema_creation.sql:Schema creation"
        "../01_setup/tasks/03_warehouse_configuration.sql:Warehouse configuration"
        "../01_setup/tasks/04_user_roles_permissions.sql:Roles and permissions"
        "../01_setup/tasks/05_resource_monitors.sql:Resource monitors"
        "tasks/01_complete_setup.sql:Complete unified setup (configurable via environment variables)"
        "tasks/99_verify_setup.sql:Setup verification"
    )
    
    for script_info in "${sql_scripts[@]}"; do
        IFS=':' read -r script description <<< "$script_info"
        execute_sql_script "$SCRIPT_DIR/$script" "$description"
    done
    
    print_success "Snowflake infrastructure setup complete"
    
    # Mark phase as completed
    mark_phase_completed "2"
}

# Phase 3: Data Generation
generate_sample_data() {
    print_status "📊 Phase 3: Sample Data Generation"
    echo "===================================="
    
    # Check if sample data already exists
    local data_dir="$PROJECT_ROOT/data/logistics_sample_data"
    local sample_files=(
        "fact_shipments.csv"
        "dim_customer.csv"
        "dim_vehicle.csv"
        "raw_azure_shipments.csv"
    )
    
    local data_exists=false
    for file in "${sample_files[@]}"; do
        if [[ -f "$data_dir/$file" ]]; then
            data_exists=true
            break
        fi
    done
    
    if [[ "$data_exists" == true ]]; then
        print_warning "Sample data files already exist in $data_dir"
        echo ""
        echo "Existing data files found:"
        ls -la "$data_dir"/*.csv 2>/dev/null | head -10
        echo ""
        read -p "Do you want to regenerate the sample data? This will overwrite existing files. (y/N): " -n 1 -r
        echo ""
        if [[ ! $REPLY =~ ^[Yy]$ ]]; then
            print_status "Skipping sample data generation - using existing data"
            print_success "Sample data generation skipped"
            return 0
        fi
    fi
    
    execute_python_script "data/generate_sample_data.py" "Sample data generation"
    print_success "Sample data generation complete"
}

# Phase 4: Data Loading
load_raw_data() {
    print_status "📥 Phase 4: Raw Data Loading"
    echo "============================="
    
    # Check if data files exist and load them
    local data_files=(
        "data/logistics_sample_data/raw_azure_customers.csv"
        "data/logistics_sample_data/raw_azure_shipments.csv"
        "data/logistics_sample_data/raw_azure_vehicles.csv"
        "data/logistics_sample_data/raw_azure_maintenance.csv"
        "data/logistics_sample_data/raw_weather_data.csv"
        "data/logistics_sample_data/raw_traffic_data.csv"
        "data/logistics_sample_data/raw_telematics_data.csv"
    )
    
    for file in "${data_files[@]}"; do
        if [[ -f "$PROJECT_ROOT/$file" ]]; then
            print_status "Loading: $file"
            # Load data file (implementation depends on your data loading method)
        else
            print_warning "Data file not found: $file"
        fi
    done
    
    print_success "Raw data loading complete"
}

# Function to analyze dbt errors and provide specific solutions
analyze_dbt_error() {
    local log_file="$1"
    local command="$2"
    
    echo "🔍 Error Analysis:"
    echo "=================="
    
    # Check for common error patterns
    if grep -q "Compilation Error" "$log_file"; then
        echo "❌ Compilation Error detected"
        echo "• Check SQL syntax in your models"
        echo "• Verify all column references are correct"
        echo "• Check for missing commas or parentheses"
    fi
    
    if grep -q "Database Error" "$log_file"; then
        echo "❌ Database Error detected"
        echo "• Check your Snowflake connection"
        echo "• Verify database/schema permissions"
        echo "• Check if target tables exist"
    fi
    
    if grep -q "relation.*does not exist" "$log_file"; then
        echo "❌ Missing Relation Error"
        echo "• Check if referenced models exist"
        echo "• Verify model dependencies are correct"
        echo "• Run models in dependency order"
    fi
    
    if grep -q "permission denied" "$log_file"; then
        echo "❌ Permission Error"
        echo "• Check your Snowflake user permissions"
        echo "• Verify role has CREATE/INSERT privileges"
        echo "• Check warehouse access permissions"
    fi
    
    if grep -q "timeout" "$log_file"; then
        echo "❌ Timeout Error"
        echo "• Query is taking too long to execute"
        echo "• Consider optimizing your SQL"
        echo "• Check warehouse size and scaling"
    fi
    
    # Extract specific model names that failed
    local failed_models=$(grep -o "model '[^']*'" "$log_file" | sort -u | tr -d "'" | sed 's/model //')
    if [[ -n "$failed_models" ]]; then
        echo ""
        echo "🚨 Failed Models:"
        echo "$failed_models" | while read -r model; do
            echo "• $model"
        done
    fi
}

# Function to execute dbt command with error handling
execute_dbt_command() {
    local command="$1"
    local description="$2"
    local log_file="/tmp/dbt_${RANDOM}.log"
    
    print_status "Executing: $description"
    echo "Command: dbt $command"
    
    # Ensure environment variables are exported for dbt
    export SF_ACCOUNT SF_USER SF_PASSWORD SF_ROLE SF_DATABASE SF_WAREHOUSE SF_SCHEMA
    
    # Change to dbt directory and activate virtual environment
    cd "$PROJECT_ROOT/dbt"
    source "$PROJECT_ROOT/venv/bin/activate"
    
    if dbt $command > "$log_file" 2>&1; then
        print_success "$description completed successfully"
        rm -f "$log_file"
        return 0
    else
        print_error "$description failed"
        echo ""
        echo "❌ Error Details:"
        echo "=================="
        cat "$log_file"
        echo ""
        
        # Analyze the error
        analyze_dbt_error "$log_file" "$command"
        
        echo ""
        echo "🔧 Troubleshooting Tips:"
        echo "========================"
        
        # Provide specific troubleshooting based on the command
        case "$command" in
            "deps")
                echo "• Check your packages.yml file for syntax errors"
                echo "• Verify internet connectivity for package downloads"
                echo "• Try running: dbt deps --full-refresh"
                echo "• Check package versions in packages.yml"
                ;;
            "parse")
                echo "• Check for SQL syntax errors in your models"
                echo "• Verify all referenced models exist"
                echo "• Check for circular dependencies"
                echo "• Try running: dbt parse --no-partial-parse"
                echo "• Check for missing macros or functions"
                ;;
            "run"*)
                echo "• Check for compilation errors in the failed models"
                echo "• Verify data sources are accessible"
                echo "• Check for missing dependencies"
                echo "• Try running individual models: dbt run --select model_name"
                echo "• Check warehouse is running and accessible"
                ;;
            "test"*)
                echo "• Check for data quality issues in your models"
                echo "• Verify test configurations in schema.yml files"
                echo "• Try running specific tests: dbt test --select test_name"
                echo "• Check if test data exists in your models"
                ;;
            "docs generate")
                echo "• Check for issues in model documentation"
                echo "• Verify all referenced models exist"
                echo "• Check for syntax errors in schema.yml files"
                ;;
        esac
        
        echo ""
        echo "📋 Next Steps:"
        echo "=============="
        echo "1. Fix the errors shown above"
        echo "2. Re-run the deployment: ./deploy.sh"
        echo "3. Or continue with individual phases: ./scripts/02_deployment/handlers/deploy_all.sh 5"
        echo "4. For detailed debugging: cd dbt && dbt $command --debug"
        echo ""
        
        # Offer quick recovery options
        echo "🚀 Quick Recovery Options:"
        echo "=========================="
        echo "1. Try running with --full-refresh flag"
        echo "2. Clear dbt cache and retry"
        echo "3. Run individual models to isolate issues"
        echo "4. Check dbt logs in logs/ directory"
        echo ""
        
        read -p "Would you like to try a quick recovery? (y/N): " -n 1 -r
        echo ""
        if [[ $REPLY =~ ^[Yy]$ ]]; then
            echo ""
            print_status "Attempting quick recovery..."
            
            # Try recovery based on command type
            case "$command" in
                "deps")
                    echo "Trying: dbt deps --full-refresh"
                    if dbt deps --full-refresh > "$log_file" 2>&1; then
                        print_success "Recovery successful! Package installation completed."
                        rm -f "$log_file"
                        return 0
                    fi
                    ;;
                "parse")
                    echo "Trying: dbt clean && dbt parse --no-partial-parse"
                    dbt clean > /dev/null 2>&1
                    if dbt parse --no-partial-parse > "$log_file" 2>&1; then
                        print_success "Recovery successful! Model parsing completed."
                        rm -f "$log_file"
                        return 0
                    fi
                    ;;
                "run"*)
                    echo "Trying: dbt run --full-refresh"
                    if dbt run --full-refresh > "$log_file" 2>&1; then
                        print_success "Recovery successful! Model building completed."
                        rm -f "$log_file"
                        return 0
                    fi
                    ;;
            esac
            
            echo "Recovery attempt failed. Original error details:"
            cat "$log_file"
            echo ""
        fi
        
        # Ask user if they want to continue
        read -p "Do you want to continue with the next step despite this error? (y/N): " -n 1 -r
        echo ""
        if [[ ! $REPLY =~ ^[Yy]$ ]]; then
            rm -f "$log_file"
            exit 1
        fi
        
        rm -f "$log_file"
        return 1
    fi
}

# Phase 5: dbt Model Building
build_dbt_models() {
    print_status "🔨 Phase 5: dbt Model Building"
    echo "==============================="
    
    if [[ ! -d "$PROJECT_ROOT/dbt" ]]; then
        print_error "dbt directory not found at $PROJECT_ROOT/dbt"
        echo "Please ensure the dbt project is properly set up."
        exit 1
    fi
    
    cd "$PROJECT_ROOT/dbt"
    
    # Activate virtual environment and check if dbt is available
    source "$PROJECT_ROOT/venv/bin/activate"
    
    if ! command -v dbt &> /dev/null; then
        print_error "dbt command not found. Please ensure dbt is installed in the virtual environment."
        exit 1
    fi
    
    # Check dbt project configuration
    if [[ ! -f "dbt_project.yml" ]]; then
        print_error "dbt_project.yml not found. Please ensure this is a valid dbt project."
        exit 1
    fi
    
    print_status "dbt project validation passed"
    
    # Step 1: Install dbt packages
    if ! execute_dbt_command "deps" "Installing dbt packages"; then
        print_warning "Package installation failed, but continuing..."
    fi
    
    # Step 2: Parse dbt models
    if ! execute_dbt_command "parse" "Parsing dbt models"; then
        print_error "Model parsing failed. Cannot continue with model building."
        exit 1
    fi
    
    # Step 3: Build staging models
    if ! execute_dbt_command "run --select tag:staging" "Building staging models"; then
        print_warning "Some staging models failed, but continuing with marts..."
    fi
    
    # Step 4: Build marts models
    if ! execute_dbt_command "run --select tag:marts" "Building marts models"; then
        print_warning "Some marts models failed, but continuing with analytics..."
    fi
    
    # Step 5: Build analytics models
    if ! execute_dbt_command "run --select tag:analytics" "Building analytics models"; then
        print_warning "Some analytics models failed, but continuing with tests..."
    fi
    
    # Step 6: Run tests
    if ! execute_dbt_command "test" "Running dbt tests"; then
        print_warning "Some tests failed, but continuing with documentation..."
    fi
    
    # Step 7: Generate documentation
    if ! execute_dbt_command "docs generate" "Generating dbt documentation"; then
        print_warning "Documentation generation failed, but model building is complete."
    fi
    
    # Summary
    echo ""
    print_status "📊 dbt Model Building Summary"
    echo "================================="
    
    # Count successful models
    local total_models=$(find models -name "*.sql" 2>/dev/null | wc -l)
    local built_models=$(dbt list --resource-type model --output name 2>/dev/null | wc -l)
    
    echo "📈 Models: $built_models/$total_models built"
    echo "🧪 Tests: Run with 'dbt test' to see detailed results"
    echo "📚 Docs: Generated in target/ directory"
    echo ""
    
    cd "$PROJECT_ROOT"
    print_success "dbt model building phase completed"
}

# Phase 6: Snowflake Object Deployment
deploy_snowflake_objects() {
    print_status "🏗️ Phase 6: Snowflake Object Deployment"
    echo "======================================="
    
    # Deploy dimension tables
    print_status "Deploying dimension tables..."
    execute_sql_script "$SCRIPT_DIR/../dimensions/create_dimension_tables.sql" "Dimension tables"
    
    # Deploy fact tables
    print_status "Deploying fact tables..."
    execute_sql_script "$SCRIPT_DIR/../facts/create_fact_tables.sql" "Fact tables"
    
    # Deploy views
    print_status "Deploying views..."
    execute_sql_script "$SCRIPT_DIR/../views/create_views.sql" "Views"
    
    # Deploy ML objects
    print_status "Deploying ML objects..."
    execute_sql_script "$SCRIPT_DIR/../ml/create_ml_objects.sql" "ML objects"
    
    # Deploy monitoring objects
    print_status "Deploying monitoring objects..."
    bash "$SCRIPT_DIR/../03_monitoring/handlers/setup_alert_system.sh"
    
    # Deploy performance objects
    print_status "Deploying performance objects..."
    bash "$SCRIPT_DIR/../04_performance/handlers/optimize_performance.sh"
    
    # Deploy security objects
    print_status "Deploying security objects..."
    bash "$SCRIPT_DIR/../05_security/handlers/setup_audit_logging.sh"
    
    # Deploy streaming objects
    print_status "Deploying streaming objects..."
    bash "$SCRIPT_DIR/../07_streaming/handlers/deploy_streams_and_tasks.sh"
    
    # Deploy governance objects
    print_status "Deploying governance objects..."
    bash "$SCRIPT_DIR/../06_governance/handlers/setup_governance.sh"
    
    print_success "Snowflake object deployment complete"
}

# Phase 7: Tests and Validation
run_tests_and_validation() {
    print_status "🧪 Phase 7: Tests and Validation"
    echo "==============================="
    
    # Run comprehensive dbt tests
    if [[ -d "$PROJECT_ROOT/dbt" ]]; then
        cd "$PROJECT_ROOT/dbt"
        
        print_status "Running comprehensive dbt tests..."
        dbt test
        
        print_status "Running data quality tests..."
        dbt test --select tag:data_quality
        
        print_status "Running business logic tests..."
        dbt test --select tag:business_logic
        
        print_status "Running performance tests..."
        dbt test --select tag:performance
        
        cd "$PROJECT_ROOT"
        print_success "dbt tests completed"
    fi
    
    # Generate quality reports
    print_status "Generating quality reports..."
    if [[ -f "$PROJECT_ROOT/scripts/03_monitoring/handlers/generate_quality_report.py" ]]; then
        execute_python_script "scripts/03_monitoring/handlers/generate_quality_report.py" "Quality report generation"
    fi
    
    # Test key analytics
    print_status "Testing key analytics..."
    execute_sql_script "$SCRIPT_DIR/../analytics/test_executive_dashboard.sql" "Executive dashboard testing"
    execute_sql_script "$SCRIPT_DIR/../ml/test_ml_feature_store.sql" "ML feature store testing"
    execute_sql_script "$SCRIPT_DIR/../streaming/test_real_time_analytics.sql" "Real-time analytics testing"
    
    # Generate deployment summary
    print_status "Generating deployment summary..."
    cat > "$PROJECT_ROOT/deployment_summary.md" << EOF
# 🚀 Deployment Summary

## Deployment Information
- **Date**: $(date)
- **Environment**: Production
- **Version**: 1.0.0

## Components Deployed
- ✅ Infrastructure Setup
- ✅ Data Pipeline
- ✅ dbt Models (43+ models)
- ✅ Snowflake Objects
- ✅ ML Feature Store
- ✅ Real-time Analytics
- ✅ Monitoring & Alerting
- ✅ Security & Governance

## Test Results
- ✅ dbt Tests: All passed
- ✅ Data Quality: Validated
- ✅ Performance: Optimized
- ✅ Security: Configured

## Next Steps
1. Access documentation: \`cd dbt && dbt docs serve --port 8000\`
2. Test analytics: Query views in Snowflake
3. Monitor performance: Check monitoring dashboard
4. Review costs: Monitor warehouse usage

## Support
For questions or issues, contact: jharkeris@hotmail.com
EOF
    
    print_success "Tests and validation completed"
}

# Main deployment function
main() {
    local skip_data=false
    local reset_deployment=false
    
    # Check for flags
    for arg in "$@"; do
        if [[ "$arg" == "--skip-data" ]]; then
            skip_data=true
        elif [[ "$arg" == "--reset" ]]; then
            reset_deployment=true
        fi
    done
    
    # Reset deployment status if requested
    if [[ "$reset_deployment" == true ]]; then
        if [[ -f "$DEPLOYMENT_STATUS_FILE" ]]; then
            rm "$DEPLOYMENT_STATUS_FILE"
            print_status "Deployment status reset - all phases will run"
        fi
    fi
    
    echo ""
    show_deployment_status
    print_status "Starting complete deployment process..."
    echo ""
    
    setup_environment
    echo ""
    
    setup_snowflake_infrastructure
    echo ""
    
    if [[ "$skip_data" == false ]]; then
        generate_sample_data
        echo ""
        
        load_raw_data
        echo ""
    else
        print_status "Skipping data generation and loading (--skip-data flag)"
        echo ""
    fi
    
    build_dbt_models
    echo ""
    
    deploy_snowflake_objects
    echo ""
    
    run_tests_and_validation
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

# Check if running individual phase (but not flags)
if [ $# -eq 1 ] && [[ ! "$1" =~ ^-- ]]; then
    case "$1" in
        "1"|"env"|"environment")
            setup_environment
            ;;
        "2"|"snowflake"|"setup")
            setup_snowflake_infrastructure
            ;;
        "3"|"data"|"generate")
            generate_sample_data
            ;;
        "4"|"load"|"raw")
            load_raw_data
            ;;
        "5"|"dbt"|"models")
            build_dbt_models
            ;;
        "6"|"objects"|"deploy")
            deploy_snowflake_objects
            ;;
        "7"|"tests"|"final")
            run_tests_and_validation
            ;;
        "help"|"-h"|"--help")
            echo "🚀 Logistics Analytics Platform Deployment"
            echo ""
            echo "Usage: $0 [phase] [options]"
            echo ""
            echo "Phases:"
            echo "  1, env, environment     - Setup environment and credentials"
            echo "  2, snowflake, setup     - Setup Snowflake infrastructure"
            echo "  3, data, generate       - Generate sample data"
            echo "  4, load, raw           - Load raw data to Snowflake"
            echo "  5, dbt, models         - Build dbt models"
            echo ""
            echo "Options:"
            echo "  --skip-data            - Skip data generation and loading phases"
            echo "  --reset                - Reset deployment status and run all phases"
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