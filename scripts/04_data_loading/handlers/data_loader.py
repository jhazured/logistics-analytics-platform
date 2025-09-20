#!/usr/bin/env python3
"""
Data Loading Script for Logistics Analytics Platform
===================================================
This script provides comprehensive data loading capabilities for Snowflake.
Supports CSV files, JSON data, and direct API connections.

Usage:
    python3 data_loader.py --help
    python3 data_loader.py load-csv --file customers.csv --table RAW.CUSTOMERS
    python3 data_loader.py load-json --file shipments.json --table RAW.SHIPMENTS
    python3 data_loader.py generate-sample --count 1000
"""

import os
import sys
import json
import csv
import argparse
import logging
from pathlib import Path
from typing import Dict, List, Any, Optional
import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from datetime import datetime, timedelta
import random
import uuid

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.append(str(project_root))

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('data_loading.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class SnowflakeDataLoader:
    """Main class for loading data into Snowflake"""
    
    def __init__(self):
        """Initialize the data loader with environment variables"""
        self.connection_params = {
            'account': os.getenv('SF_ACCOUNT'),
            'user': os.getenv('SF_USER'),
            'password': os.getenv('SF_PASSWORD'),
            'role': os.getenv('SF_ROLE', 'ACCOUNTADMIN'),
            'warehouse': os.getenv('SF_WAREHOUSE', 'COMPUTE_WH_XS'),
            'database': os.getenv('SF_DATABASE', 'LOGISTICS_DW_DEV'),
            'schema': os.getenv('SF_SCHEMA', 'RAW')
        }
        
        # Validate required parameters
        required_params = ['account', 'user', 'password']
        missing_params = [param for param in required_params if not self.connection_params[param]]
        if missing_params:
            raise ValueError(f"Missing required environment variables: {missing_params}")
        
        self.conn = None
        self.cursor = None
    
    def connect(self):
        """Establish connection to Snowflake"""
        try:
            self.conn = snowflake.connector.connect(**self.connection_params)
            self.cursor = self.conn.cursor()
            logger.info(f"Connected to Snowflake: {self.connection_params['database']}.{self.connection_params['schema']}")
        except Exception as e:
            logger.error(f"Failed to connect to Snowflake: {e}")
            raise
    
    def disconnect(self):
        """Close Snowflake connection"""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
        logger.info("Disconnected from Snowflake")
    
    def load_csv_file(self, file_path: str, table_name: str, 
                     if_exists: str = 'append', 
                     chunk_size: int = 10000) -> bool:
        """
        Load data from CSV file into Snowflake table
        
        Args:
            file_path: Path to CSV file
            table_name: Target table name (e.g., 'RAW.CUSTOMERS')
            if_exists: What to do if table exists ('append', 'replace', 'fail')
            chunk_size: Number of rows to process at once
        """
        try:
            logger.info(f"Loading CSV file: {file_path} -> {table_name}")
            
            # Read CSV file
            df = pd.read_csv(file_path)
            logger.info(f"Read {len(df)} rows from {file_path}")
            
            # Set schema and database context
            if '.' in table_name:
                schema, table = table_name.split('.', 1)
                self.cursor.execute(f"USE SCHEMA {schema}")
            else:
                table = table_name
            
            # Create table if it doesn't exist
            if if_exists == 'replace':
                self.cursor.execute(f"DROP TABLE IF EXISTS {table}")
                logger.info(f"Dropped existing table: {table}")
            
            # Load data using pandas
            success, nchunks, nrows, _ = write_pandas(
                self.conn, 
                df, 
                table_name=table,
                database=self.connection_params['database'],
                schema=self.connection_params['schema'],
                chunk_size=chunk_size,
                auto_create_table=True
            )
            
            if success:
                logger.info(f"Successfully loaded {nrows} rows into {table_name}")
                return True
            else:
                logger.error(f"Failed to load data into {table_name}")
                return False
                
        except Exception as e:
            logger.error(f"Error loading CSV file {file_path}: {e}")
            return False
    
    def load_json_file(self, file_path: str, table_name: str, 
                      json_column: str = 'data') -> bool:
        """
        Load data from JSON file into Snowflake table
        
        Args:
            file_path: Path to JSON file
            table_name: Target table name
            json_column: Column name for JSON data
        """
        try:
            logger.info(f"Loading JSON file: {file_path} -> {table_name}")
            
            # Read JSON file
            with open(file_path, 'r') as f:
                data = json.load(f)
            
            # Convert to DataFrame
            if isinstance(data, list):
                df = pd.DataFrame(data)
            else:
                df = pd.DataFrame([data])
            
            logger.info(f"Read {len(df)} records from {file_path}")
            
            # Set schema context
            if '.' in table_name:
                schema, table = table_name.split('.', 1)
                self.cursor.execute(f"USE SCHEMA {schema}")
            else:
                table = table_name
            
            # Load data
            success, nchunks, nrows, _ = write_pandas(
                self.conn, 
                df, 
                table_name=table,
                database=self.connection_params['database'],
                schema=self.connection_params['schema'],
                auto_create_table=True
            )
            
            if success:
                logger.info(f"Successfully loaded {nrows} rows into {table_name}")
                return True
            else:
                logger.error(f"Failed to load data into {table_name}")
                return False
                
        except Exception as e:
            logger.error(f"Error loading JSON file {file_path}: {e}")
            return False
    
    def execute_sql_file(self, sql_file: str) -> bool:
        """
        Execute SQL file for data loading
        
        Args:
            sql_file: Path to SQL file
        """
        try:
            logger.info(f"Executing SQL file: {sql_file}")
            
            with open(sql_file, 'r') as f:
                sql_content = f.read()
            
            # Split by semicolon and execute each statement
            statements = [stmt.strip() for stmt in sql_content.split(';') if stmt.strip()]
            
            for i, statement in enumerate(statements, 1):
                logger.info(f"Executing statement {i}/{len(statements)}")
                self.cursor.execute(statement)
            
            logger.info(f"Successfully executed {len(statements)} statements from {sql_file}")
            return True
            
        except Exception as e:
            logger.error(f"Error executing SQL file {sql_file}: {e}")
            return False
    
    def validate_table_exists(self, table_name: str) -> bool:
        """Check if table exists in Snowflake"""
        try:
            if '.' in table_name:
                schema, table = table_name.split('.', 1)
                self.cursor.execute(f"SHOW TABLES IN SCHEMA {schema} LIKE '{table}'")
            else:
                self.cursor.execute(f"SHOW TABLES LIKE '{table_name}'")
            
            result = self.cursor.fetchall()
            exists = len(result) > 0
            logger.info(f"Table {table_name} exists: {exists}")
            return exists
            
        except Exception as e:
            logger.error(f"Error checking table existence: {e}")
            return False
    
    def load_dataframe(self, df: pd.DataFrame, table_name: str, 
                      if_exists: str = 'append') -> bool:
        """
        Load pandas DataFrame into Snowflake table
        
        Args:
            df: pandas DataFrame to load
            table_name: Target table name (e.g., 'RAW.CUSTOMERS')
            if_exists: What to do if table exists ('append', 'replace', 'fail')
        """
        try:
            logger.info(f"Loading DataFrame with {len(df)} rows -> {table_name}")
            
            # Set schema and database context
            if '.' in table_name:
                schema, table = table_name.split('.', 1)
                self.cursor.execute(f"USE SCHEMA {schema}")
            else:
                table = table_name
                schema = self.connection_params['schema']
            
            # Create table if it doesn't exist
            if if_exists == 'replace':
                self.cursor.execute(f"DROP TABLE IF EXISTS {table}")
                logger.info(f"Dropped existing table: {table}")
            
            # Load data using pandas
            success, nchunks, nrows, _ = write_pandas(
                self.conn, 
                df, 
                table_name=table,
                database=self.connection_params['database'],
                schema=schema,
                auto_create_table=True
            )
            
            if success:
                logger.info(f"Successfully loaded {nrows} rows into {table_name}")
                return True
            else:
                logger.error(f"Failed to load data into {table_name}")
                return False
                
        except Exception as e:
            logger.error(f"Error loading DataFrame to {table_name}: {e}")
            return False

    def get_table_info(self, table_name: str) -> Dict[str, Any]:
        """Get information about a table"""
        try:
            if '.' in table_name:
                schema, table = table_name.split('.', 1)
                self.cursor.execute(f"DESCRIBE TABLE {schema}.{table}")
            else:
                self.cursor.execute(f"DESCRIBE TABLE {table_name}")
            
            columns = self.cursor.fetchall()
            
            # Get row count
            if '.' in table_name:
                self.cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            else:
                self.cursor.execute(f"SELECT COUNT(*) FROM {self.connection_params['schema']}.{table_name}")
            
            row_count = self.cursor.fetchone()[0]
            
            return {
                'table_name': table_name,
                'columns': columns,
                'row_count': row_count
            }
            
        except Exception as e:
            logger.error(f"Error getting table info: {e}")
            return {}

def main():
    """Main function for command line interface"""
    parser = argparse.ArgumentParser(description='Snowflake Data Loader')
    subparsers = parser.add_subparsers(dest='command', help='Available commands')
    
    # CSV loading command
    csv_parser = subparsers.add_parser('load-csv', help='Load CSV file')
    csv_parser.add_argument('--file', required=True, help='CSV file path')
    csv_parser.add_argument('--table', required=True, help='Target table name')
    csv_parser.add_argument('--if-exists', choices=['append', 'replace', 'fail'], 
                           default='append', help='What to do if table exists')
    csv_parser.add_argument('--chunk-size', type=int, default=10000, 
                           help='Chunk size for loading')
    
    # JSON loading command
    json_parser = subparsers.add_parser('load-json', help='Load JSON file')
    json_parser.add_argument('--file', required=True, help='JSON file path')
    json_parser.add_argument('--table', required=True, help='Target table name')
    json_parser.add_argument('--json-column', default='data', 
                           help='JSON column name')
    
    # SQL execution command
    sql_parser = subparsers.add_parser('execute-sql', help='Execute SQL file')
    sql_parser.add_argument('--file', required=True, help='SQL file path')
    
    # Table info command
    info_parser = subparsers.add_parser('table-info', help='Get table information')
    info_parser.add_argument('--table', required=True, help='Table name')
    
    # Sample data generation command
    sample_parser = subparsers.add_parser('generate-sample', help='Generate sample data')
    sample_parser.add_argument('--count', type=int, default=1000, 
                              help='Number of records to generate')
    sample_parser.add_argument('--table', help='Specific table to generate data for')
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        return
    
    # Initialize data loader
    loader = SnowflakeDataLoader()
    
    try:
        loader.connect()
        
        if args.command == 'load-csv':
            success = loader.load_csv_file(
                args.file, 
                args.table, 
                args.if_exists, 
                args.chunk_size
            )
            print(f"CSV loading {'successful' if success else 'failed'}")
            
        elif args.command == 'load-json':
            success = loader.load_json_file(args.file, args.table, args.json_column)
            print(f"JSON loading {'successful' if success else 'failed'}")
            
        elif args.command == 'execute-sql':
            success = loader.execute_sql_file(args.file)
            print(f"SQL execution {'successful' if success else 'failed'}")
            
        elif args.command == 'table-info':
            info = loader.get_table_info(args.table)
            if info:
                print(f"Table: {info['table_name']}")
                print(f"Row count: {info['row_count']}")
                print("Columns:")
                for col in info['columns']:
                    print(f"  {col[0]} - {col[1]}")
            else:
                print("Failed to get table information")
                
        elif args.command == 'generate-sample':
            from sample_data_generator import SampleDataGenerator
            generator = SampleDataGenerator(loader)
            generator.generate_all_sample_data(args.count, args.table)
    
    except Exception as e:
        logger.error(f"Error: {e}")
        sys.exit(1)
    
    finally:
        loader.disconnect()

if __name__ == '__main__':
    main()
