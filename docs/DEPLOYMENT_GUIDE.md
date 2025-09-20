# 🚀 Deployment Guide
## Logistics Analytics Platform

### **Overview**
The deployment process is now organized into 7 phases, each with its own script that can be run independently or as part of the complete deployment.

### **Deployment Scripts**

#### **Master Orchestration**
- **`scripts/deployment/deploy_all.sh`** - Main deployment script with all phases
- **`deploy.sh`** - Convenient entry point from root directory

#### **Setup Scripts** (in `scripts/setup/`)
1. **`01_setup_environment.sh`** - Environment setup and credentials
2. **`02_setup_snowflake.sh`** - Snowflake infrastructure setup
3. **`configure_environment.sh`** - Environment configuration

#### **Setup SQL Scripts** (in `scripts/setup/`)
- **`00_build_and_run_setup.sql`** - Complete build-and-run setup
- **`00_complete_setup.sql`** - Complete setup orchestration
- **`01_database_setup.sql`** - Database creation
- **`02_schema_creation.sql`** - Schema creation
- **`03_warehouse_configuration.sql`** - Warehouse setup
- **`04_user_roles_permissions.sql`** - User roles and permissions
- **`05_resource_monitors.sql`** - Resource monitors
- **`99_verify_setup.sql`** - Setup verification

#### **Deployment Scripts** (in `scripts/deployment/`)
3. **`03_generate_data.sh`** - Sample data generation
4. **`04_load_raw_data.sh`** - Load raw data to Snowflake
5. **`05_build_dbt_models.sh`** - Build dbt models
6. **`06_deploy_snowflake_objects.sh`** - Deploy Snowflake objects
7. **`07_run_final_tests.sh`** - Run tests and generate reports

### **How to Deploy**

#### **Complete Deployment**
```bash
# From root directory
./deploy.sh

# Or directly
./scripts/deployment/deploy_all.sh
```

#### **Individual Phases**
```bash
# Run specific phase
./scripts/deployment/deploy_all.sh 1    # Environment setup
./scripts/deployment/deploy_all.sh 2    # Snowflake setup
./scripts/deployment/deploy_all.sh 3    # Generate data
./scripts/deployment/deploy_all.sh 4    # Load raw data
./scripts/deployment/deploy_all.sh 5    # Build dbt models
./scripts/deployment/deploy_all.sh 6    # Deploy Snowflake objects
./scripts/deployment/deploy_all.sh 7    # Run final tests

# Or use phase names
./scripts/deployment/deploy_all.sh env      # Environment setup
./scripts/deployment/deploy_all.sh snowflake # Snowflake setup
./scripts/deployment/deploy_all.sh data     # Generate data
./scripts/deployment/deploy_all.sh load     # Load raw data
./scripts/deployment/deploy_all.sh dbt      # Build dbt models
./scripts/deployment/deploy_all.sh objects  # Deploy Snowflake objects
./scripts/deployment/deploy_all.sh tests    # Run final tests
```

#### **Help**
```bash
./scripts/deployment/deploy_all.sh help
```

### **What Each Phase Does**

#### **Phase 1: Environment Setup**
- Loads credentials from `.env` file
- Sets up Python virtual environment
- Installs required packages
- Tests Snowflake connection

#### **Phase 2: Snowflake Setup**
- Uses existing SQL setup scripts:
  - `scripts/setup/01_database_setup.sql`
  - `scripts/setup/02_schema_creation.sql`
  - `scripts/setup/03_warehouse_configuration.sql`
  - `scripts/setup/04_user_roles_permissions.sql`
  - `scripts/setup/05_resource_monitors.sql`
  - `scripts/setup/99_verify_setup.sql`

#### **Phase 3: Generate Data**
- Runs `data/generate_sample_data.py`
- Creates 400,000+ realistic records
- Generates all required CSV files

#### **Phase 4: Load Raw Data**
- Creates raw tables in Snowflake
- Loads sample data from CSV files
- Verifies data loading

#### **Phase 5: Build dbt Models**
- Installs dbt packages
- Parses dbt models
- Builds staging, marts, and analytics models
- Runs tests
- Generates documentation

#### **Phase 6: Deploy Snowflake Objects**
- Deploys dimension tables
- Deploys fact tables
- Deploys views
- Deploys ML objects
- Deploys monitoring objects
- Deploys performance objects
- Deploys security objects
- Deploys streaming objects
- Deploys governance objects

#### **Phase 7: Run Final Tests**
- Runs comprehensive dbt tests
- Generates quality reports
- Tests key analytics
- Generates deployment summary

### **Prerequisites**
- Snowflake account with admin access
- Python 3.8+ installed
- `.env` file with Snowflake credentials

### **Expected Results**
- Complete analytics platform
- 43+ dbt models
- ML feature engineering
- Real-time analytics
- Data quality monitoring
- Comprehensive documentation
- Total cost: $5-15
- Total time: ~90 minutes

### **Troubleshooting**
- Each phase can be run independently
- Check logs for specific error messages
- Verify `.env` file has correct credentials
- Ensure Snowflake connection is working

### **Next Steps After Deployment**
1. Access documentation: `cd dbt && dbt docs serve --port 8000`
2. Test analytics: Query views in Snowflake
3. Review summary: `cat deployment_summary.md`
4. Clean up when done: Drop databases and warehouses
