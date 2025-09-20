# Setup Instructions

## Prerequisites

Before setting up the platform, ensure you have:

- **Snowflake Account**: Trial or production account with appropriate permissions
- **Python 3.8+**: For data generation and dbt execution
- **Git**: For version control
- **dbt Core 1.6+**: Data transformation tool

## Quick Start

1. **Clone Repository**
   ```bash
   git clone https://github.com/jhazured/logistics-analytics-platform.git
   cd logistics-analytics-platform
   ```

2. **Initial Setup (Full Refresh)**
   ```bash
   dbt run --full-refresh --select tag:raw
   ```

3. **Incremental Updates (Cost-Optimized)**
   ```bash
   dbt run --select tag:incremental
   ```

> **💡 Cost Optimization**: This project uses incremental loading to minimize Fivetran costs by 70-90%. See [INCREMENTAL_LOADING_STRATEGY.md](INCREMENTAL_LOADING_STRATEGY.md) for details.

4. **Environment Setup**
   ```bash
   # Configure environment
   ./scripts/setup/configure_environment.sh dev
   
   # Install dependencies
   pip install -r requirements.txt
   
   # Install dbt packages
   dbt deps
   ```

5. **Generate Sample Data**
   ```bash
   # Create virtual environment
   python3 -m venv venv
   source venv/bin/activate
   
   # Install dependencies
   pip install -r requirements.txt
   
   # Generate sample data
   python3 data/generate_sample_data.py
   ```

6. **Run dbt Models**
   ```bash
   # Run all models
   dbt run
   
   # Run specific model
   dbt run --select tbl_fact_shipments
   
   # Run with tests
   dbt run --select +tbl_fact_shipments
   ```

7. **Generate Documentation**
   ```bash
   dbt docs generate
   dbt docs serve
   ```

## Environment Configuration

### Development Environment

1. **Configure dbt Profile**
   ```yaml
   # ~/.dbt/profiles.yml
   logistics_analytics_platform:
     target: dev
     outputs:
       dev:
         type: snowflake
         account: your_account
         user: your_username
         password: your_password
         role: DATA_ENGINEER
         database: LOGISTICS_DW_DEV
         warehouse: COMPUTE_WH_XS
         schema: dbt_dev
         threads: 4
         client_session_keep_alive: False
   ```

2. **Set Environment Variables**
   ```bash
   export DBT_PROFILES_DIR=~/.dbt
   export SNOWFLAKE_ACCOUNT=your_account
   export SNOWFLAKE_USER=your_username
   export SNOWFLAKE_PASSWORD=your_password
   ```

3. **Initialize dbt Project**
   ```bash
   dbt init logistics_analytics_platform
   cd logistics_analytics_platform
   ```

### Staging Environment

1. **Configure Staging Profile**
   ```yaml
   logistics_analytics_platform:
     target: staging
     outputs:
       staging:
         type: snowflake
         account: your_account
         user: your_username
         password: your_password
         role: DATA_ENGINEER
         database: LOGISTICS_DW_STAGING
         warehouse: COMPUTE_WH_SMALL
         schema: dbt_staging
         threads: 8
         client_session_keep_alive: False
   ```

2. **Deploy to Staging**
   ```bash
   dbt run --target staging
   dbt test --target staging
   ```

### Production Environment

1. **Configure Production Profile**
   ```yaml
   logistics_analytics_platform:
     target: prod
     outputs:
       prod:
         type: snowflake
         account: your_account
         user: your_username
         password: your_password
         role: DATA_ENGINEER
         database: LOGISTICS_DW_PROD
         warehouse: COMPUTE_WH_MEDIUM
         schema: dbt_prod
         threads: 16
         client_session_keep_alive: True
   ```

2. **Deploy to Production**
   ```bash
   dbt run --target prod
   dbt test --target prod
   ```

## Snowflake Setup

### 1. Database and Schema Creation

```sql
-- Create database
CREATE DATABASE IF NOT EXISTS LOGISTICS_DW_PROD;

-- Create schemas
CREATE SCHEMA IF NOT EXISTS LOGISTICS_DW_PROD.RAW;
CREATE SCHEMA IF NOT EXISTS LOGISTICS_DW_PROD.STAGING;
CREATE SCHEMA IF NOT EXISTS LOGISTICS_DW_PROD.MARTS;
CREATE SCHEMA IF NOT EXISTS LOGISTICS_DW_PROD.ML_FEATURES;
CREATE SCHEMA IF NOT EXISTS LOGISTICS_DW_PROD.ANALYTICS;
CREATE SCHEMA IF NOT EXISTS LOGISTICS_DW_PROD.MONITORING;
CREATE SCHEMA IF NOT EXISTS LOGISTICS_DW_PROD.SNAPSHOTS;
```

### 2. Warehouse Configuration

```sql
-- Create warehouses
CREATE WAREHOUSE IF NOT EXISTS COMPUTE_WH_XS
  WAREHOUSE_SIZE = 'X-SMALL'
  AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE
  INITIALLY_SUSPENDED = TRUE;

CREATE WAREHOUSE IF NOT EXISTS COMPUTE_WH_SMALL
  WAREHOUSE_SIZE = 'SMALL'
  AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE
  INITIALLY_SUSPENDED = TRUE;

CREATE WAREHOUSE IF NOT EXISTS COMPUTE_WH_MEDIUM
  WAREHOUSE_SIZE = 'MEDIUM'
  AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE
  INITIALLY_SUSPENDED = TRUE;
```

### 3. User Roles and Permissions

```sql
-- Create roles
CREATE ROLE IF NOT EXISTS DATA_ENGINEER;
CREATE ROLE IF NOT EXISTS DATA_ANALYST;
CREATE ROLE IF NOT EXISTS ML_ENGINEER;
CREATE ROLE IF NOT EXISTS DATA_SCIENTIST;

-- Grant permissions
GRANT USAGE ON DATABASE LOGISTICS_DW_PROD TO ROLE DATA_ENGINEER;
GRANT USAGE ON DATABASE LOGISTICS_DW_PROD TO ROLE DATA_ANALYST;
GRANT USAGE ON DATABASE LOGISTICS_DW_PROD TO ROLE ML_ENGINEER;
GRANT USAGE ON DATABASE LOGISTICS_DW_PROD TO ROLE DATA_SCIENTIST;

-- Grant schema permissions
GRANT USAGE ON SCHEMA LOGISTICS_DW_PROD.RAW TO ROLE DATA_ENGINEER;
GRANT USAGE ON SCHEMA LOGISTICS_DW_PROD.STAGING TO ROLE DATA_ENGINEER;
GRANT USAGE ON SCHEMA LOGISTICS_DW_PROD.MARTS TO ROLE DATA_ANALYST;
GRANT USAGE ON SCHEMA LOGISTICS_DW_PROD.ML_FEATURES TO ROLE ML_ENGINEER;
GRANT USAGE ON SCHEMA LOGISTICS_DW_PROD.ANALYTICS TO ROLE DATA_ANALYST;
```

## Fivetran Setup

### 1. Create Fivetran Account

1. Sign up for Fivetran account
2. Connect to your Snowflake account
3. Create destination in Snowflake

### 2. Configure Connectors

1. **Azure SQL Database**
   - Source: Your Azure SQL Database
   - Destination: `LOGISTICS_DW_PROD.RAW`
   - Tables: customers, shipments, vehicles, maintenance

2. **External APIs**
   - Weather API: `LOGISTICS_DW_PROD.RAW.weather_data`
   - Traffic API: `LOGISTICS_DW_PROD.RAW.traffic_data`
   - Telematics API: `LOGISTICS_DW_PROD.RAW.telematics_data`

### 3. Configure Incremental Loading

```sql
-- Configure incremental loading for each connector
-- This will be handled by Fivetran automatically
-- Ensure _loaded_at column is included in all tables
```

## CI/CD Setup

### 1. GitHub Actions Configuration

1. **Set up GitHub Repository**
   ```bash
   git remote add origin https://github.com/your-org/logistics-analytics-platform.git
   git push -u origin main
   ```

2. **Configure Secrets**
   - `SNOWFLAKE_ACCOUNT`
   - `SNOWFLAKE_USER`
   - `SNOWFLAKE_PASSWORD`
   - `SNOWFLAKE_ROLE`
   - `SNOWFLAKE_DATABASE`
   - `SNOWFLAKE_WAREHOUSE`

3. **Enable GitHub Actions**
   - Go to repository Settings > Actions
   - Enable GitHub Actions
   - Workflows will run automatically on push/PR

### 2. dbt Cloud Integration (Optional)

1. **Connect dbt Cloud to GitHub**
   - Link your GitHub repository
   - Configure Snowflake connection
   - Set up deployment environments

2. **Configure Jobs**
   - Development: Run on every commit
   - Staging: Run on merge to staging branch
   - Production: Run on merge to main branch

## Production Deployment

### 1. Pre-deployment Checklist

- [ ] All tests passing
- [ ] Documentation updated
- [ ] Performance benchmarks met
- [ ] Security review completed
- [ ] Backup strategy in place

### 2. Deployment Steps

1. **Deploy Snowflake Objects**
   ```bash
   # Run setup scripts
   snowsql -f snowflake/setup/01_database_setup.sql
   snowsql -f snowflake/setup/02_schema_creation.sql
   snowsql -f snowflake/setup/03_warehouse_configuration.sql
   snowsql -f snowflake/setup/04_user_roles_permissions.sql
   snowsql -f snowflake/setup/05_resource_monitors.sql
   ```

2. **Deploy dbt Models**
   ```bash
   dbt run --target prod
   dbt test --target prod
   dbt docs generate --target prod
   ```

3. **Deploy Snowflake Views and Procedures**
   ```bash
   snowsql -f snowflake/views/monitoring/vw_data_quality_summary.sql
   snowsql -f snowflake/views/monitoring/vw_performance_monitoring.sql
   snowsql -f snowflake/streaming/email_alerting_system.sql
   ```

4. **Set up Monitoring**
   ```bash
   # Deploy monitoring scripts
   python scripts/monitoring/data_quality_monitor.py
   python scripts/monitoring/performance_monitor.py
   python scripts/monitoring/cost_monitor.py
   ```

### 3. Post-deployment Validation

1. **Data Quality Checks**
   ```bash
   dbt test --target prod
   ```

2. **Performance Validation**
   ```sql
   -- Check query performance
   SELECT * FROM vw_performance_monitoring
   WHERE query_date >= CURRENT_DATE();
   ```

3. **Cost Monitoring**
   ```sql
   -- Check warehouse usage
   SELECT * FROM vw_cost_monitoring
   WHERE usage_date >= CURRENT_DATE();
   ```

## Troubleshooting

### Common Issues

1. **Connection Issues**
   ```bash
   # Test Snowflake connection
   dbt debug
   ```

2. **Permission Issues**
   ```sql
   -- Check user permissions
   SHOW GRANTS TO USER your_username;
   ```

3. **Model Failures**
   ```bash
   # Run specific model with debug
   dbt run --select failing_model --debug
   ```

4. **Test Failures**
   ```bash
   # Run tests with store failures
   dbt test --store-failures
   ```

### Support

For additional support:
- Check the [dbt documentation](https://docs.getdbt.com/)
- Review [Snowflake documentation](https://docs.snowflake.com/)
- Open an issue in the GitHub repository
