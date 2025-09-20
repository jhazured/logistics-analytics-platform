# Parameterization Guide - Logistics Analytics Platform

This guide explains how to use the parameterized configuration system for flexible deployment across different environments.

## Overview

The Logistics Analytics Platform has been fully parameterized to support flexible deployment across different environments without requiring code changes. All database references, schema names, and configuration values can be controlled through environment variables.

## Key Benefits

- **🔧 Flexibility**: Deploy to any database name without code changes
- **🌍 Environment Management**: Easy switching between dev/staging/prod
- **🚀 CI/CD Ready**: Simple environment variable overrides for deployment pipelines
- **🔒 Security**: Sensitive values in environment variables, not code
- **📈 Maintainability**: Single source of truth for configuration

## Environment Variables

### Core Snowflake Configuration

| Variable | Description | Default | Example |
|----------|-------------|---------|---------|
| `SF_ACCOUNT` | Snowflake account identifier | Required | `XXXXXXX-XXXXXXX` |
| `SF_USER` | Snowflake username | Required | `XXXXXXX` |
| `SF_PASSWORD` | Snowflake password | Required | `your_password` |
| `SF_ROLE` | Snowflake role | `ACCOUNTADMIN` | `ACCOUNTADMIN` |
| `SF_WAREHOUSE` | Snowflake warehouse | `COMPUTE_WH_XS` | `COMPUTE_WH_XS` |
| `SF_DATABASE` | Target database name | `LOGISTICS_DW_DEV` | `LOGISTICS_DW_DEV` |
| `SF_SCHEMA` | Default schema | `ANALYTICS` | `ANALYTICS` |

### dbt Configuration

| Variable | Description | Default | Example |
|----------|-------------|---------|---------|
| `DBT_TARGET` | dbt target environment | `dev` | `dev`, `staging`, `prod` |
| `DBT_THREADS` | Number of parallel threads | `4` | `4`, `8`, `12` |

### Unified Setup Configuration

| Variable | Description | Default | Example |
|----------|-------------|---------|---------|
| `SETUP_MODE` | Setup mode: 'minimal' or 'complete' | `complete` | `minimal`, `complete` |
| `SKIP_WAREHOUSES` | Skip warehouse creation | `false` | `true`, `false` |
| `SKIP_RESOURCE_MONITORS` | Skip resource monitor creation | `false` | `true`, `false` |

## Usage Examples

### Basic Setup

```bash
# Set core environment variables
export SF_ACCOUNT="your-account.snowflakecomputing.com"
export SF_USER="your-username"
export SF_PASSWORD="your-password"
export SF_ROLE="ACCOUNTADMIN"
export SF_WAREHOUSE="COMPUTE_WH_XS"
export SF_DATABASE="LOGISTICS_DW_DEV"
export SF_SCHEMA="ANALYTICS"

# Execute parameterized setup
python3 scripts/01_setup/handlers/execute_sql_python.py scripts/01_setup/tasks/01_database_setup.sql
python3 scripts/01_setup/handlers/execute_sql_python.py scripts/01_setup/tasks/02_schema_creation.sql
python3 scripts/01_setup/handlers/execute_sql_python.py scripts/01_setup/tasks/04_user_roles_permissions.sql

# Run dbt models
dbt run --full-refresh --select tag:raw
dbt run --select tag:incremental
```

### Environment-Specific Deployment

#### Development Environment
```bash
export SF_DATABASE="LOGISTICS_DW_DEV"
export SF_WAREHOUSE="COMPUTE_WH_XS"
export DBT_THREADS="4"
export DBT_TARGET="dev"
```

#### Staging Environment
```bash
export SF_DATABASE="LOGISTICS_DW_STAGING"
export SF_WAREHOUSE="COMPUTE_WH_SMALL"
export DBT_THREADS="8"
export DBT_TARGET="staging"
```

#### Production Environment
```bash
export SF_DATABASE="LOGISTICS_DW_PROD"
export SF_WAREHOUSE="COMPUTE_WH_MEDIUM"
export DBT_THREADS="12"
export DBT_TARGET="prod"
```

### Custom Database Names

```bash
# Deploy to a custom database
export SF_DATABASE="MY_CUSTOM_LOGISTICS_DB"
export SF_SCHEMA="ANALYTICS"

# All scripts will use MY_CUSTOM_LOGISTICS_DB instead of LOGISTICS_DW_DEV
python3 scripts/01_setup/handlers/execute_sql_python.py scripts/01_setup/tasks/01_database_setup.sql
```

### Unified Setup Examples

#### Minimal Setup (Build-and-Run)
```bash
export SF_DATABASE="LOGISTICS_DW_DEV"
export SF_SCHEMA="ANALYTICS"
export SETUP_MODE="minimal"
export SKIP_WAREHOUSES="false"
export SKIP_RESOURCE_MONITORS="false"

# Creates minimal environment with X-Small warehouse and $50 credit limit
python3 scripts/01_setup/handlers/execute_sql_python.py scripts/02_deployment/tasks/01_complete_setup.sql
```

#### Complete Setup (Production)
```bash
export SF_DATABASE="LOGISTICS_DW_PROD"
export SF_SCHEMA="ANALYTICS"
export SETUP_MODE="complete"
export SKIP_WAREHOUSES="false"
export SKIP_RESOURCE_MONITORS="false"

# Creates complete environment with all warehouses and resource monitors
python3 scripts/01_setup/handlers/execute_sql_python.py scripts/02_deployment/tasks/01_complete_setup.sql
```

#### Schema-Only Setup
```bash
export SF_DATABASE="LOGISTICS_DW_DEV"
export SF_SCHEMA="ANALYTICS"
export SETUP_MODE="complete"
export SKIP_WAREHOUSES="true"
export SKIP_RESOURCE_MONITORS="true"

# Creates only databases and schemas, skips warehouses and resource monitors
python3 scripts/01_setup/handlers/execute_sql_python.py scripts/02_deployment/tasks/01_complete_setup.sql
```

## Parameterized Files

### SQL Scripts

The following SQL scripts are fully parameterized:

- `scripts/01_setup/tasks/01_database_setup.sql` - Database creation
- `scripts/01_setup/tasks/02_schema_creation.sql` - Schema creation
- `scripts/01_setup/tasks/04_user_roles_permissions.sql` - Roles and permissions
- `scripts/02_deployment/tasks/01_complete_setup.sql` - Unified setup (replaces old separate setup files)

### dbt Configuration

- `dbt/profiles.yml` - Connection profiles with environment variable support
- `dbt/dbt_project.yml` - Project variables and configuration

### Execution Scripts

- `scripts/01_setup/handlers/execute_sql.sh` - Shell wrapper for SQL execution
- `scripts/01_setup/handlers/execute_sql_python.py` - Python executor with variable substitution
- `scripts/01_setup/handlers/configure_environment.sh` - Environment configuration

## Variable Substitution

### SQL Scripts

SQL scripts use Snowflake session variables with fallback defaults:

```sql
-- Get database name from environment variable or use default
SET DATABASE_NAME = IFNULL($SF_DATABASE, 'LOGISTICS_DW_DEV');

-- Use the variable in DDL statements
CREATE DATABASE IF NOT EXISTS IDENTIFIER($DATABASE_NAME);
```

### Python Executor

The Python executor automatically substitutes environment variables:

```python
# Converts: IFNULL($SF_DATABASE, 'LOGISTICS_DW_DEV')
# To: 'LOGISTICS_DW_DEV' (if SF_DATABASE is set)
# Or: 'LOGISTICS_DW_DEV' (if SF_DATABASE is not set)
```

### dbt Profiles

dbt profiles use Jinja templating with environment variables:

```yaml
dev:
  type: snowflake
  account: "{{ env_var('SF_ACCOUNT') }}"
  user: "{{ env_var('SF_USER') }}"
  password: "{{ env_var('SF_PASSWORD') }}"
  database: "{{ env_var('SF_DATABASE', 'LOGISTICS_DW_DEV') }}"
  warehouse: "{{ env_var('SF_WAREHOUSE', 'COMPUTE_WH_XS') }}"
  schema: "{{ env_var('SF_SCHEMA', 'ANALYTICS') }}"
  threads: "{{ env_var('DBT_THREADS', 4) | int }}"
```

## CI/CD Integration

### GitHub Actions

```yaml
env:
  SF_ACCOUNT: ${{ secrets.SF_ACCOUNT }}
  SF_USER: ${{ secrets.SF_USER }}
  SF_PASSWORD: ${{ secrets.SF_PASSWORD }}
  SF_DATABASE: ${{ vars.SF_DATABASE }}
  SF_WAREHOUSE: ${{ vars.SF_WAREHOUSE }}
  SF_SCHEMA: ${{ vars.SF_SCHEMA }}
  DBT_THREADS: ${{ vars.DBT_THREADS }}
```

### Docker

```dockerfile
ENV SF_DATABASE=LOGISTICS_DW_DEV
ENV SF_WAREHOUSE=COMPUTE_WH_XS
ENV SF_SCHEMA=ANALYTICS
ENV DBT_THREADS=4
```

### Kubernetes

```yaml
env:
- name: SF_DATABASE
  value: "LOGISTICS_DW_DEV"
- name: SF_WAREHOUSE
  value: "COMPUTE_WH_XS"
- name: SF_SCHEMA
  value: "ANALYTICS"
```

## Best Practices

### 1. Environment Variable Naming

- Use consistent prefixes (`SF_` for Snowflake, `DBT_` for dbt)
- Use uppercase with underscores
- Be descriptive and clear

### 2. Default Values

- Always provide sensible defaults
- Use environment-specific defaults when possible
- Document default values in comments

### 3. Security

- Never commit sensitive values to code
- Use environment variables for all secrets
- Use different credentials per environment

### 4. Validation

- Validate required environment variables before execution
- Provide clear error messages for missing variables
- Test with different environment configurations

## Troubleshooting

### Common Issues

#### 1. Environment Variables Not Set
```bash
# Error: Environment variable SF_ACCOUNT not found
# Solution: Set the required environment variable
export SF_ACCOUNT="your-account.snowflakecomputing.com"
```

#### 2. Database Does Not Exist
```bash
# Error: Database 'MY_DB' does not exist
# Solution: Run the database setup script first
python3 scripts/01_setup/handlers/execute_sql_python.py scripts/01_setup/tasks/01_database_setup.sql
```

#### 3. Permission Denied
```bash
# Error: Insufficient privileges to create database
# Solution: Ensure you're using a role with appropriate permissions
export SF_ROLE="ACCOUNTADMIN"
```

### Debug Mode

Enable debug mode to see variable substitution:

```bash
# Set debug environment variable
export DEBUG=1

# Run with verbose output
python3 scripts/01_setup/handlers/execute_sql_python.py scripts/01_setup/tasks/01_database_setup.sql
```

## Migration Guide

### From Hardcoded to Parameterized

If you have existing hardcoded configurations:

1. **Identify hardcoded values** in your scripts
2. **Replace with environment variables** using the patterns shown above
3. **Update execution scripts** to use the new parameterized versions
4. **Test with different environments** to ensure flexibility
5. **Update documentation** to reflect the new parameterized approach

### Example Migration

```sql
-- Before (hardcoded)
CREATE DATABASE IF NOT EXISTS LOGISTICS_DW_DEV;

-- After (parameterized)
SET DATABASE_NAME = IFNULL($SF_DATABASE, 'LOGISTICS_DW_DEV');
CREATE DATABASE IF NOT EXISTS IDENTIFIER($DATABASE_NAME);
```

## Support

For questions or issues with parameterization:

1. Check the [Troubleshooting Guide](12_TROUBLESHOOTING_GUIDES.md)
2. Review the [Setup Instructions](02_SETUP.md)
3. Examine the example configurations in `.env.example`
4. Test with the provided execution scripts

The parameterization system is designed to be flexible and maintainable while providing clear error messages and comprehensive documentation.
