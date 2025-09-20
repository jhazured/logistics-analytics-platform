# Data Loading Scripts - Logistics Analytics Platform

This directory contains comprehensive data loading tools for the Logistics Analytics Platform, providing both manual data loading capabilities and automated sample data generation.

## 📁 Directory Structure

```
scripts/04_data_loading/
├── handlers/
│   ├── data_loader.py           # Main data loading functionality
│   ├── sample_data_generator.py # Sample data generation
│   └── load_data.sh            # Shell script wrapper
├── requirements.txt            # Python dependencies
└── README.md                  # This documentation
```

## 🚀 Quick Start

### 1. Install Dependencies

```bash
# Install Python dependencies
pip3 install -r scripts/04_data_loading/requirements.txt

# Or install individually
pip3 install pandas snowflake-connector-python
```

### 2. Set Environment Variables

Make sure your `.env` file contains the required Snowflake connection details:

```bash
export SF_ACCOUNT="your-account.snowflakecomputing.com"
export SF_USER="your-username"
export SF_PASSWORD="your-password"
export SF_ROLE="ACCOUNTADMIN"
export SF_WAREHOUSE="COMPUTE_WH_XS"
export SF_DATABASE="LOGISTICS_DW_DEV"
export SF_SCHEMA="RAW"
```

### 3. Generate Sample Data

```bash
# Generate 1000 records for all tables
./scripts/04_data_loading/handlers/load_data.sh generate-sample 1000

# Generate data for specific table
./scripts/04_data_loading/handlers/load_data.sh generate-table customers 500
```

## 📊 Available Commands

### Shell Script Interface (`load_data.sh`)

```bash
# Load CSV file
./scripts/04_data_loading/handlers/load_data.sh load-csv data/customers.csv RAW.CUSTOMERS

# Load JSON file
./scripts/04_data_loading/handlers/load_data.sh load-json data/shipments.json RAW.SHIPMENTS

# Generate sample data
./scripts/04_data_loading/handlers/load_data.sh generate-sample 5000

# Generate data for specific table
./scripts/04_data_loading/handlers/load_data.sh generate-table vehicles 1000

# Show table information
./scripts/04_data_loading/handlers/load_data.sh table-info RAW.CUSTOMERS

# List all tables
./scripts/04_data_loading/handlers/load_data.sh list-tables
```

### Python Script Interface

```bash
# Direct Python usage
python3 scripts/04_data_loading/handlers/data_loader.py load-csv --file data.csv --table RAW.CUSTOMERS
python3 scripts/04_data_loading/handlers/sample_data_generator.py --count 1000 --table customers
```

## 🗄️ Supported Data Sources

### 1. CSV Files
- **Format**: Standard CSV with headers
- **Encoding**: UTF-8
- **Size**: No limit (chunked processing for large files)
- **Auto-detection**: Column types and table creation

### 2. JSON Files
- **Format**: JSON array or single object
- **Structure**: Flat or nested (flattened automatically)
- **Encoding**: UTF-8

### 3. Sample Data Generation
- **Realistic data**: Business-logic compliant
- **Relationships**: Proper foreign key relationships
- **Volume**: Configurable record counts
- **Variety**: Multiple data types and patterns

## 📋 Sample Data Tables

The sample data generator creates realistic data for these tables:

### Core Tables
- **`RAW.CUSTOMERS`** - Customer information and contact details
- **`RAW.VEHICLES`** - Vehicle fleet information and specifications
- **`RAW.ROUTES`** - Route definitions and characteristics
- **`RAW.SHIPMENTS`** - Shipment records and tracking information

### Supporting Tables
- **`RAW.WEATHER`** - Weather conditions and forecasts
- **`RAW.TRAFFIC`** - Traffic conditions and incidents
- **`RAW.MAINTENANCE`** - Vehicle maintenance records

### Sample Data Characteristics

#### Customers (1000 records)
```sql
customer_id: CUST_000001
company_name: Acme Corp
contact_name: Contact 1
email: contact1@acmecorp.com
phone: 555-123-4567
address: 123 Main St
city: New York
state: NY
zip_code: 10001
customer_type: Enterprise
```

#### Vehicles (1000 records)
```sql
vehicle_id: VEH_000001
make: Ford
model: F-150
year: 2022
vehicle_type: Truck
capacity_kg: 25000
fuel_type: Diesel
license_plate: NY123AB
vin: 1HGBH41JXMN123456
```

#### Shipments (1000 records)
```sql
shipment_id: SHIP_000001
customer_id: CUST_000001
vehicle_id: VEH_000001
route_id: ROUTE_000001
pickup_date: 2024-01-15
delivery_date: 2024-01-17
weight_kg: 1500
volume_cubic_meters: 5.2
shipment_value: 2500
status: Delivered
priority: High
```

## 🔧 Advanced Usage

### Custom Data Loading

```python
from scripts.04_data_loading.handlers.data_loader import SnowflakeDataLoader
import pandas as pd

# Initialize loader
loader = SnowflakeDataLoader()
loader.connect()

# Load custom DataFrame
df = pd.DataFrame({
    'id': [1, 2, 3],
    'name': ['A', 'B', 'C'],
    'value': [100, 200, 300]
})

success = loader.load_dataframe(df, 'RAW.CUSTOM_TABLE')
loader.disconnect()
```

### Batch Processing

```python
# Process multiple files
files = ['data1.csv', 'data2.csv', 'data3.csv']
tables = ['RAW.TABLE1', 'RAW.TABLE2', 'RAW.TABLE3']

for file, table in zip(files, tables):
    success = loader.load_csv_file(file, table)
    if success:
        print(f"Loaded {file} successfully")
    else:
        print(f"Failed to load {file}")
```

### Data Validation

```python
# Check table exists and get info
if loader.validate_table_exists('RAW.CUSTOMERS'):
    info = loader.get_table_info('RAW.CUSTOMERS')
    print(f"Table has {info['row_count']} rows")
    print(f"Columns: {[col[0] for col in info['columns']]}")
```

## 📈 Performance Considerations

### Large File Processing
- **Chunking**: Files are processed in configurable chunks (default: 10,000 rows)
- **Memory**: Efficient memory usage with streaming processing
- **Parallel**: Multiple files can be processed simultaneously

### Snowflake Optimization
- **Warehouse**: Uses configured warehouse for processing
- **Auto-suspend**: Warehouses auto-suspend after processing
- **Compression**: Data is compressed during transfer

### Error Handling
- **Retry logic**: Automatic retry for transient failures
- **Logging**: Comprehensive logging for debugging
- **Rollback**: Failed transactions are rolled back

## 🐛 Troubleshooting

### Common Issues

#### 1. Connection Errors
```
Error: Failed to connect to Snowflake
```
**Solution**: Check environment variables and network connectivity

#### 2. Permission Errors
```
Error: Insufficient privileges
```
**Solution**: Ensure user has CREATE TABLE and INSERT permissions

#### 3. Data Type Errors
```
Error: Invalid data type conversion
```
**Solution**: Check data format and column types

#### 4. Memory Errors
```
Error: Out of memory
```
**Solution**: Reduce chunk size or process smaller files

### Debug Mode

```bash
# Enable debug logging
export PYTHONPATH="${PYTHONPATH}:$(pwd)"
python3 -c "
import logging
logging.basicConfig(level=logging.DEBUG)
from scripts.04_data_loading.handlers.data_loader import SnowflakeDataLoader
loader = SnowflakeDataLoader()
loader.connect()
"
```

## 📚 Examples

### Example 1: Load Customer Data

```bash
# Create sample CSV
echo "customer_id,company_name,email,phone
CUST_001,Acme Corp,contact@acme.com,555-123-4567
CUST_002,Global Inc,info@global.com,555-987-6543" > customers.csv

# Load into Snowflake
./scripts/04_data_loading/handlers/load_data.sh load-csv customers.csv RAW.CUSTOMERS
```

### Example 2: Generate Test Data

```bash
# Generate 5000 records for testing
./scripts/04_data_loading/handlers/load_data.sh generate-sample 5000

# Verify data was loaded
./scripts/04_data_loading/handlers/load_data.sh table-info RAW.CUSTOMERS
```

### Example 3: Load JSON Data

```bash
# Create sample JSON
echo '[
  {"shipment_id": "SHIP_001", "status": "Delivered", "value": 1000},
  {"shipment_id": "SHIP_002", "status": "In Transit", "value": 2000}
]' > shipments.json

# Load into Snowflake
./scripts/04_data_loading/handlers/load_data.sh load-json shipments.json RAW.SHIPMENTS
```

## 🔗 Integration with dbt

After loading data, you can run dbt models:

```bash
# Load sample data
./scripts/04_data_loading/handlers/load_data.sh generate-sample 1000

# Run dbt models
cd dbt
dbt run --select tag:raw
dbt run --select tag:staging
dbt run --select tag:marts
```

## 📞 Support

For issues or questions:
1. Check the troubleshooting section above
2. Review the logs in `data_loading.log`
3. Verify environment variables and permissions
4. Test with small sample files first

## 🚀 Next Steps

1. **Load Sample Data**: Start with generated sample data
2. **Test dbt Models**: Run dbt transformations
3. **Connect Real Sources**: Set up Fivetran or custom connectors
4. **Monitor Performance**: Use Snowflake query history and dbt logs
5. **Scale Up**: Increase data volumes and add more sources
