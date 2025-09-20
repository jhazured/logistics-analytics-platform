#!/bin/bash
# 📤 Phase 4: Load Raw Data to Snowflake
# Creates raw tables and loads sample data

set -e

echo "📤 Phase 4: Load Raw Data to Snowflake"
echo "======================================"

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

# Create raw tables
create_raw_tables() {
    print_status "Creating raw tables in Snowflake..."
    
    python -c "
import snowflake.connector
import os
import pandas as pd

# Connect to Snowflake
conn = snowflake.connector.connect(
    account=os.getenv('SF_ACCOUNT'),
    user=os.getenv('SF_USER'),
    password=os.getenv('SF_PASSWORD'),
    role=os.getenv('SF_ROLE'),
    warehouse=os.getenv('SF_WAREHOUSE'),
    database=os.getenv('SF_DATABASE'),
    schema='RAW'
)

cursor = conn.cursor()

# Raw table definitions
raw_tables = {
    'raw_azure_customers': '''
        CREATE OR REPLACE TABLE raw_azure_customers (
            customer_id NUMBER,
            customer_name VARCHAR(255),
            customer_type VARCHAR(50),
            industry_code VARCHAR(10),
            customer_tier VARCHAR(20),
            account_manager VARCHAR(255),
            billing_address VARCHAR(500),
            shipping_address VARCHAR(500),
            phone VARCHAR(20),
            email VARCHAR(255),
            credit_limit NUMBER(15,2),
            payment_terms VARCHAR(50),
            created_date DATE,
            last_updated TIMESTAMP,
            is_active BOOLEAN,
            notes VARCHAR(1000)
        )
    ''',
    'raw_azure_shipments': '''
        CREATE OR REPLACE TABLE raw_azure_shipments (
            shipment_id NUMBER,
            customer_id NUMBER,
            vehicle_id VARCHAR(50),
            route_id NUMBER,
            origin_location_id NUMBER,
            destination_location_id NUMBER,
            shipment_date DATE,
            planned_departure TIMESTAMP,
            actual_departure TIMESTAMP,
            planned_arrival TIMESTAMP,
            actual_arrival TIMESTAMP,
            weight_kg NUMBER(10,2),
            volume_m3 NUMBER(10,2),
            distance_km NUMBER(10,2),
            fuel_cost NUMBER(10,2),
            delivery_cost NUMBER(10,2),
            revenue NUMBER(15,2),
            customer_rating NUMBER(2,1),
            is_on_time BOOLEAN,
            delivery_notes VARCHAR(1000),
            weather_conditions VARCHAR(100),
            traffic_conditions VARCHAR(100),
            route_efficiency_score NUMBER(3,2),
            created_date TIMESTAMP,
            last_updated TIMESTAMP,
            status VARCHAR(50)
        )
    ''',
    'raw_azure_vehicles': '''
        CREATE OR REPLACE TABLE raw_azure_vehicles (
            vehicle_id VARCHAR(50),
            vehicle_type VARCHAR(50),
            make VARCHAR(50),
            model VARCHAR(50),
            year NUMBER(4),
            capacity_kg NUMBER(10,2),
            capacity_m3 NUMBER(10,2),
            fuel_type VARCHAR(20),
            fuel_efficiency_kmpl NUMBER(5,2),
            purchase_date DATE,
            last_maintenance_date DATE,
            next_maintenance_date DATE,
            maintenance_cost NUMBER(10,2),
            insurance_cost NUMBER(10,2),
            depreciation_cost NUMBER(10,2),
            total_cost NUMBER(15,2),
            is_active BOOLEAN,
            current_location VARCHAR(255),
            current_driver VARCHAR(255),
            odometer_reading NUMBER(10),
            created_date TIMESTAMP,
            last_updated TIMESTAMP,
            notes VARCHAR(1000)
        )
    ''',
    'raw_azure_maintenance': '''
        CREATE OR REPLACE TABLE raw_azure_maintenance (
            maintenance_id NUMBER,
            vehicle_id VARCHAR(50),
            maintenance_type VARCHAR(50),
            maintenance_date DATE,
            cost NUMBER(10,2),
            description VARCHAR(1000),
            parts_replaced VARCHAR(1000),
            labor_hours NUMBER(5,2),
            mechanic_name VARCHAR(255),
            next_maintenance_date DATE,
            mileage_at_service NUMBER(10),
            created_date TIMESTAMP,
            last_updated TIMESTAMP,
            status VARCHAR(50),
            priority VARCHAR(20)
        )
    ''',
    'raw_weather_data': '''
        CREATE OR REPLACE TABLE raw_weather_data (
            weather_id NUMBER,
            location_id NUMBER,
            date DATE,
            temperature_celsius NUMBER(5,2),
            humidity_percent NUMBER(5,2),
            wind_speed_kmh NUMBER(5,2),
            wind_direction VARCHAR(10),
            precipitation_mm NUMBER(8,2),
            visibility_km NUMBER(5,2),
            pressure_hpa NUMBER(8,2),
            weather_condition VARCHAR(50),
            uv_index NUMBER(3,1),
            created_date TIMESTAMP,
            last_updated TIMESTAMP,
            source VARCHAR(50),
            reliability_score NUMBER(3,2)
        )
    ''',
    'raw_traffic_data': '''
        CREATE OR REPLACE TABLE raw_traffic_data (
            traffic_id NUMBER,
            route_id NUMBER,
            date DATE,
            time_period VARCHAR(20),
            traffic_density VARCHAR(20),
            average_speed_kmh NUMBER(5,2),
            delay_minutes NUMBER(5,2),
            incident_count NUMBER(3),
            road_conditions VARCHAR(50),
            weather_impact VARCHAR(50),
            created_date TIMESTAMP,
            last_updated TIMESTAMP,
            source VARCHAR(50),
            reliability_score NUMBER(3,2)
        )
    ''',
    'raw_telematics_data': '''
        CREATE OR REPLACE TABLE raw_telematics_data (
            telemetry_id NUMBER,
            vehicle_id VARCHAR(50),
            timestamp TIMESTAMP,
            latitude NUMBER(10,8),
            longitude NUMBER(11,8),
            speed_kmh NUMBER(5,2),
            fuel_level_percent NUMBER(5,2),
            engine_rpm NUMBER(6),
            engine_temperature_celsius NUMBER(5,2),
            brake_usage_count NUMBER(5),
            acceleration_count NUMBER(5),
            harsh_braking_count NUMBER(3),
            harsh_acceleration_count NUMBER(3),
            idling_time_minutes NUMBER(8,2),
            distance_traveled_km NUMBER(10,2),
            created_date TIMESTAMP,
            last_updated TIMESTAMP
        )
    '''
}

# Create tables
for table_name, create_sql in raw_tables.items():
    try:
        cursor.execute(create_sql)
        print(f'✅ Created table {table_name}')
    except Exception as e:
        print(f'❌ Error creating {table_name}: {e}')

cursor.close()
conn.close()
print('✅ Raw tables created successfully!')
"
}

# Load sample data
load_sample_data() {
    print_status "Loading sample data to Snowflake..."
    
    python -c "
import snowflake.connector
import os
import pandas as pd
import glob

# Connect to Snowflake
conn = snowflake.connector.connect(
    account=os.getenv('SF_ACCOUNT'),
    user=os.getenv('SF_USER'),
    password=os.getenv('SF_PASSWORD'),
    role=os.getenv('SF_ROLE'),
    warehouse=os.getenv('SF_WAREHOUSE'),
    database=os.getenv('SF_DATABASE'),
    schema='RAW'
)

cursor = conn.cursor()

# Load data from CSV files
csv_files = glob.glob('logistics_sample_data/raw_*.csv')

for csv_file in csv_files:
    table_name = os.path.basename(csv_file).replace('.csv', '')
    print(f'Loading {table_name}...')
    
    try:
        # Read CSV file
        df = pd.read_csv(csv_file)
        
        # Convert DataFrame to list of tuples
        data = [tuple(row) for row in df.values]
        
        # Create INSERT statement
        columns = ', '.join(df.columns)
        placeholders = ', '.join(['%s'] * len(df.columns))
        insert_sql = f'INSERT INTO {table_name} ({columns}) VALUES ({placeholders})'
        
        # Execute batch insert
        cursor.executemany(insert_sql, data)
        print(f'✅ Loaded {len(data)} rows into {table_name}')
        
    except Exception as e:
        print(f'❌ Error loading {table_name}: {e}')

# Commit all changes
conn.commit()
cursor.close()
conn.close()
print('✅ Sample data loaded successfully!')
"
}

# Verify data loading
verify_data_loading() {
    print_status "Verifying data loading..."
    
    python -c "
import snowflake.connector
import os

# Connect to Snowflake
conn = snowflake.connector.connect(
    account=os.getenv('SF_ACCOUNT'),
    user=os.getenv('SF_USER'),
    password=os.getenv('SF_PASSWORD'),
    role=os.getenv('SF_ROLE'),
    warehouse=os.getenv('SF_WAREHOUSE'),
    database=os.getenv('SF_DATABASE'),
    schema='RAW'
)

cursor = conn.cursor()

# Check row counts for each table
tables = ['raw_azure_customers', 'raw_azure_shipments', 'raw_azure_vehicles', 
          'raw_azure_maintenance', 'raw_weather_data', 'raw_traffic_data', 'raw_telematics_data']

print('📊 Data loading verification:')
for table in tables:
    try:
        cursor.execute(f'SELECT COUNT(*) FROM {table}')
        count = cursor.fetchone()[0]
        print(f'  {table}: {count:,} rows')
    except Exception as e:
        print(f'  {table}: Error - {e}')

cursor.close()
conn.close()
"
}

# Main function
main() {
    echo ""
    load_env_vars
    create_raw_tables
    load_sample_data
    verify_data_loading
    echo ""
    print_success "✅ Phase 4: Raw Data Loading Complete"
    echo ""
    echo "Next: Run 05_build_dbt_models.sh"
}

# Run main function
main "$@"
