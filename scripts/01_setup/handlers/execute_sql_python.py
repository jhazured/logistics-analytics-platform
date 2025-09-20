#!/usr/bin/env python3

"""
Parameterized SQL Execution Script for Snowflake
This script executes SQL files with environment variable substitution
"""

import snowflake.connector
import os
import sys
import re
from pathlib import Path

def substitute_variables(sql_content, env_vars):
    """Substitute environment variables in SQL content"""
    
    # First, handle IFNULL patterns to set session variables
    ifnull_pattern = r'IFNULL\(\$([A-Z_]+),\s*\'([^\']+)\'\)'
    def replace_ifnull(match):
        var_name = match.group(1)
        default_value = match.group(2)
        if var_name in env_vars:
            return f"'{env_vars[var_name]}'"
        else:
            return f"'{default_value}'"
    
    sql_content = re.sub(ifnull_pattern, replace_ifnull, sql_content)
    
    # Then handle direct variable references
    pattern = r'\$([A-Z_]+)'
    def replace_var(match):
        var_name = match.group(1)
        if var_name in env_vars:
            return f"'{env_vars[var_name]}'"
        else:
            print(f"Warning: Environment variable {var_name} not found, using literal")
            return match.group(0)
    
    # Replace session variables with quoted values
    sql_content = re.sub(pattern, replace_var, sql_content)
    
    return sql_content

def execute_sql_file(sql_file_path, env_vars):
    """Execute SQL file with environment variable substitution"""
    
    # Read SQL file
    with open(sql_file_path, 'r') as f:
        sql_content = f.read()
    
    # Substitute environment variables
    sql_content = substitute_variables(sql_content, env_vars)
    
    # Connect to Snowflake
    conn = snowflake.connector.connect(
        account=env_vars.get('SF_ACCOUNT'),
        user=env_vars.get('SF_USER'),
        password=env_vars.get('SF_PASSWORD'),
        role=env_vars.get('SF_ROLE', 'ACCOUNTADMIN'),
        warehouse=env_vars.get('SF_WAREHOUSE', 'COMPUTE_WH_XS'),
        database=env_vars.get('SF_DATABASE', 'LOGISTICS_DW_DEV'),
        schema=env_vars.get('SF_SCHEMA', 'ANALYTICS')
    )
    
    try:
        cursor = conn.cursor()
        
        # Split by semicolon and execute each statement
        statements = [stmt.strip() for stmt in sql_content.split(';') if stmt.strip()]
        
        for i, statement in enumerate(statements):
            if statement and not statement.startswith('--'):
                print(f"Executing statement {i+1}/{len(statements)}: {statement[:100]}...")
                cursor.execute(statement)
        
        cursor.close()
        conn.close()
        print("✅ SQL execution completed successfully!")
        return True
        
    except Exception as e:
        print(f"❌ Error executing SQL: {e}")
        cursor.close()
        conn.close()
        return False

def main():
    if len(sys.argv) < 2:
        print("Usage: python3 execute_sql_python.py <sql_file>")
        sys.exit(1)
    
    sql_file = sys.argv[1]
    
    if not os.path.exists(sql_file):
        print(f"Error: SQL file not found: {sql_file}")
        sys.exit(1)
    
    # Get environment variables
    env_vars = {
        'SF_ACCOUNT': os.getenv('SF_ACCOUNT'),
        'SF_USER': os.getenv('SF_USER'),
        'SF_PASSWORD': os.getenv('SF_PASSWORD'),
        'SF_ROLE': os.getenv('SF_ROLE', 'ACCOUNTADMIN'),
        'SF_WAREHOUSE': os.getenv('SF_WAREHOUSE', 'COMPUTE_WH_XS'),
        'SF_DATABASE': os.getenv('SF_DATABASE', 'LOGISTICS_DW_DEV'),
        'SF_SCHEMA': os.getenv('SF_SCHEMA', 'ANALYTICS'),
        'SETUP_MODE': os.getenv('SETUP_MODE', 'complete'),
        'SKIP_WAREHOUSES': os.getenv('SKIP_WAREHOUSES', 'false'),
        'SKIP_RESOURCE_MONITORS': os.getenv('SKIP_RESOURCE_MONITORS', 'false')
    }
    
    # Check required variables
    required_vars = ['SF_ACCOUNT', 'SF_USER', 'SF_PASSWORD']
    missing_vars = [var for var in required_vars if not env_vars[var]]
    
    if missing_vars:
        print(f"Error: Missing required environment variables: {', '.join(missing_vars)}")
        sys.exit(1)
    
    print(f"Executing SQL file: {sql_file}")
    print(f"Using database: {env_vars['SF_DATABASE']}")
    print(f"Using warehouse: {env_vars['SF_WAREHOUSE']}")
    print(f"Using schema: {env_vars['SF_SCHEMA']}")
    
    success = execute_sql_file(sql_file, env_vars)
    sys.exit(0 if success else 1)

if __name__ == "__main__":
    main()
