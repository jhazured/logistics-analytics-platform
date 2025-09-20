# 🚀 Deployment Guide
## Logistics Analytics Platform

### **Overview**
The deployment process is now organized into 7 phases, each with its own script that can be run independently or as part of the complete deployment.

### **Deployment Scripts**

#### **Master Orchestration**
- **`scripts/deployment/handlers/deploy_all.sh`** - Main deployment script with all phases
- **`deploy.sh`** - Convenient entry point from root directory

#### **Deployment Structure** (Ansible-like organization)
The deployment is organized into two main categories:

**01_setup** (`scripts/01_setup/`) - Infrastructure setup and configuration:
- **Handlers**: `configure_environment.sh` - Environment configuration (dev/staging/prod)
- **Tasks**: 
  - `01_database_setup.sql` - Database creation
  - `02_schema_creation.sql` - Schema creation
  - `03_warehouse_configuration.sql` - Warehouse configuration
  - `04_user_roles_permissions.sql` - Roles and permissions
  - `05_resource_monitors.sql` - Resource monitors

**02_deployment** (`scripts/02_deployment/`) - Complete deployment orchestration:
- **Handlers**: `deploy_all.sh` - Complete deployment orchestration (all phases)
- **Tasks**:
  - `00_build_and_run_setup.sql` - Complete build-and-run setup
  - `00_complete_setup.sql` - Complete setup orchestration
  - `99_verify_setup.sql` - Setup verification

**03_monitoring** (`scripts/03_monitoring/`) - Monitoring and alerting:
- **Handlers**: `setup_alert_system.sh`, `generate_quality_report.py`
- **Tasks**: 8 numbered tasks from `01_create_alert_tables.sql` to `99_verify_alert_setup.sql`

**04_performance** (`scripts/04_performance/`) - Performance optimization:
- **Handlers**: `optimize_performance.sh`
- **Tasks**: 6 numbered tasks from `01_cost_monitoring.sql` to `06_automated_tasks.sql`

**05_security** (`scripts/05_security/`) - Security and audit:
- **Handlers**: `setup_audit_logging.sh`
- **Tasks**: 9 numbered tasks from `01_configure_account_audit.sql` to `99_verify_audit_setup.sql`

**06_governance** (`scripts/06_governance/`) - Data governance:
- **Handlers**: `setup_governance.sh`
- **Tasks**: `01_advanced_data_lineage.sql`

**07_streaming** (`scripts/07_streaming/`) - Real-time streaming:
- **Handlers**: `deploy_streams_and_tasks.sh`
- **Tasks**: 5 numbered tasks from `01_create_streams.sql` to `99_verify_deployment.sql`

**08_automation** (`scripts/08_automation/`) - Automation framework:
- **Handlers**: 6 Python automation scripts
- **Tasks**: Templates and configuration files

### **How to Deploy**

#### **Complete Deployment**
```bash
# From root directory
./deploy.sh

# Or directly
./scripts/02_deployment/handlers/deploy_all.sh
```

#### **Individual Phases**
```bash
# Run specific phase
./scripts/02_deployment/handlers/deploy_all.sh 1    # Environment setup
./scripts/02_deployment/handlers/deploy_all.sh 2    # Snowflake infrastructure
./scripts/02_deployment/handlers/deploy_all.sh 3    # Data generation
./scripts/02_deployment/handlers/deploy_all.sh 4    # Data loading
./scripts/02_deployment/handlers/deploy_all.sh 5    # dbt models
./scripts/02_deployment/handlers/deploy_all.sh 6    # Snowflake objects
./scripts/02_deployment/handlers/deploy_all.sh 7    # Tests and validation
```

#### **Help**
```bash
./scripts/02_deployment/handlers/deploy_all.sh help
```

### **What Each Phase Does**

#### **Phase 1: Environment Setup**
- **Environment Configuration**: Sets up dev/staging/prod environment variables and configuration files
- **Credential Validation**: Validates Snowflake account, user, and password environment variables
- **Python Environment**: Creates virtual environment and installs required packages

#### **Phase 2: Snowflake Infrastructure Setup**
- **Database Creation**: Creates databases, schemas, warehouses, roles, and permissions
- **Resource Configuration**: Sets up resource monitors and warehouse configurations
- **Setup Verification**: Validates all infrastructure components

#### **Phase 3: Data Generation**
- **Sample Data Creation**: Runs `data/generate_sample_data.py` to create 400,000+ realistic records
- **CSV File Generation**: Creates all required sample data files

#### **Phase 4: Data Loading**
- **Raw Table Creation**: Creates raw tables in Snowflake
- **Data Import**: Loads sample data from CSV files into Snowflake

#### **Phase 5: dbt Model Building**
- **Package Installation**: Installs dbt packages and dependencies
- **Model Building**: Builds staging, marts, and analytics models
- **Testing**: Runs comprehensive dbt tests
- **Documentation**: Generates dbt documentation

#### **Phase 6: Snowflake Object Deployment**
- **Dimension Tables**: Deploys all dimension tables
- **Fact Tables**: Deploys all fact tables
- **Views**: Deploys analytical views
- **ML Objects**: Deploys machine learning objects
- **Monitoring**: Sets up alert systems and monitoring
- **Performance**: Deploys cost monitoring and optimization
- **Security**: Sets up audit logging and security
- **Streaming**: Deploys streams and tasks
- **Governance**: Sets up data lineage

#### **Phase 7: Tests and Validation**
- **Comprehensive Testing**: Runs all dbt tests, data quality tests, business logic tests, performance tests
- **Quality Reports**: Generates data quality, performance, and cost analysis reports
- **Analytics Testing**: Tests executive dashboard, ML feature store, and real-time analytics
- **Deployment Summary**: Generates comprehensive deployment summary with next steps

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
