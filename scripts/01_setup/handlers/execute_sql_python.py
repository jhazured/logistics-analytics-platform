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
    
    # Handle SET statements to create session variables
    set_pattern = r'SET\s+([A-Z_]+)\s*=\s*IFNULL\(\$([A-Z_]+),\s*\'([^\']+)\'\)'
    def replace_set(match):
        session_var = match.group(1)
        env_var = match.group(2)
        default_value = match.group(3)
        
        if env_var in env_vars:
            value = env_vars[env_var]
        else:
            value = default_value
        
        # Create a session variable assignment
        return f"SET {session_var} = '{value}'"
    
    sql_content = re.sub(set_pattern, replace_set, sql_content)
    
    # Also handle direct SET statements with environment variables
    direct_set_pattern = r'SET\s+([A-Z_]+)\s*=\s*\'([^\']+)\''
    def replace_direct_set(match):
        session_var = match.group(1)
        value = match.group(2)
        
        # If the value is an environment variable reference, replace it
        if value.startswith('$') and value[1:] in env_vars:
            value = env_vars[value[1:]]
        
        return f"SET {session_var} = '{value}'"
    
    sql_content = re.sub(direct_set_pattern, replace_direct_set, sql_content)
    
    # Handle IDENTIFIER with concatenation - replace with direct values
    identifier_concat_pattern = r'IDENTIFIER\(\$([A-Z_]+)\s*\|\|\s*\'\.([^\']+)\'\)'
    def replace_identifier_concat(match):
        var_name = match.group(1)
        schema_name = match.group(2)
        if var_name in env_vars:
            return f"IDENTIFIER('{env_vars[var_name]}.{schema_name}')"
        else:
            return f"IDENTIFIER('LOGISTICS_DW_DEV.{schema_name}')"
    
    sql_content = re.sub(identifier_concat_pattern, replace_identifier_concat, sql_content)
    
    # Create a mapping of session variables for later reference
    session_vars = {}
    
    # Extract session variables from SET statements
    set_matches = re.findall(r'SET\s+([A-Z_]+)\s*=\s*\'([^\']+)\'', sql_content)
    for var_name, var_value in set_matches:
        session_vars[var_name] = var_value
    
    # Then handle direct variable references
    pattern = r'\$([A-Z_]+)'
    def replace_var(match):
        var_name = match.group(1)
        # First check if it's a session variable
        if var_name in session_vars:
            return f"'{session_vars[var_name]}'"
        # Then check if it's an environment variable
        elif var_name in env_vars:
            return f"'{env_vars[var_name]}'"
        else:
            print(f"Warning: Variable {var_name} not found, using literal")
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
    # For setup scripts, don't specify database in connection to allow account-level operations
    conn = snowflake.connector.connect(
        account=env_vars.get('SF_ACCOUNT'),
        user=env_vars.get('SF_USER'),
        password=env_vars.get('SF_PASSWORD'),
        role=env_vars.get('SF_ROLE', 'ACCOUNTADMIN'),
        warehouse=env_vars.get('SF_WAREHOUSE', 'COMPUTE_WH_XS')
        # Note: Not specifying database and schema for setup scripts
        # This allows the scripts to work at account level and create/use databases as needed
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
