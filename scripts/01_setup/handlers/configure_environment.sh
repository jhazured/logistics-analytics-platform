# Environment Configuration Script
# scripts/setup/configure_environment.sh
#!/bin/bash

set -e

ENVIRONMENT=${1:-dev}
echo "🚀 Configuring environment: $ENVIRONMENT"

# Load existing environment variables if .env file exists
if [[ -f ".env" ]]; then
    set -a  # automatically export all variables
    source .env
    set +a  # stop automatically exporting
fi

# Environment-specific variables (override defaults from .env if needed)
# Default database is LOGISTICS_DW_DEV for all environments (can be overridden)
case $ENVIRONMENT in
  dev)
    export SF_DATABASE="${SF_DATABASE:-LOGISTICS_DW_DEV}"
    export SF_WAREHOUSE="${SF_WAREHOUSE:-COMPUTE_WH_XS}"  
    export SF_SCHEMA="${SF_SCHEMA:-ANALYTICS}"
    export SF_ROLE="${SF_ROLE:-DBT_DEV_ROLE}"
    export DBT_THREADS="${DBT_THREADS:-4}"
    export DBT_TARGET="${DBT_TARGET:-dev}"
    ;;
  staging)
    export SF_DATABASE="${SF_DATABASE:-LOGISTICS_DW_DEV}"
    export SF_WAREHOUSE="${SF_WAREHOUSE:-COMPUTE_WH_SMALL}"
    export SF_SCHEMA="${SF_SCHEMA:-ANALYTICS}"  
    export SF_ROLE="${SF_ROLE:-DBT_STAGING_ROLE}"
    export DBT_THREADS="${DBT_THREADS:-8}"
    export DBT_TARGET="${DBT_TARGET:-staging}"
    ;;
  prod)
    export SF_DATABASE="${SF_DATABASE:-LOGISTICS_DW_DEV}"
    export SF_WAREHOUSE="${SF_WAREHOUSE:-COMPUTE_WH_MEDIUM}"
    export SF_SCHEMA="${SF_SCHEMA:-ANALYTICS}"
    export SF_ROLE="${SF_ROLE:-DBT_PROD_ROLE}" 
    export DBT_THREADS="${DBT_THREADS:-12}"
    export DBT_TARGET="${DBT_TARGET:-prod}"
    ;;
  *)
    echo "❌ Invalid environment. Use: dev, staging, or prod"
    exit 1
    ;;
esac

# Create environment-specific directories
mkdir -p logs/$ENVIRONMENT
mkdir -p target/$ENVIRONMENT

# Set environment file
cat > .env.$ENVIRONMENT << EOF
# Snowflake Configuration for $ENVIRONMENT
SF_ACCOUNT=$SF_ACCOUNT
SF_USER=$SF_USER
SF_PASSWORD=$SF_PASSWORD
SF_ROLE=$SF_ROLE
SF_DATABASE=$SF_DATABASE
SF_WAREHOUSE=$SF_WAREHOUSE
SF_SCHEMA=$SF_SCHEMA

# dbt Configuration
DBT_PROFILES_DIR=./dbt
DBT_TARGET=$ENVIRONMENT
DBT_THREADS=$DBT_THREADS

# Environment-specific settings
ENVIRONMENT=$ENVIRONMENT
LOG_LEVEL=INFO
EOF

echo "✅ Environment $ENVIRONMENT configured successfully!"
echo "📁 Configuration saved to .env.$ENVIRONMENT"