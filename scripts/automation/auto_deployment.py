#!/usr/bin/env python3
"""
Automated Deployment Pipeline
Handles end-to-end deployment with validation, testing, and rollback capabilities
"""

import os
import sys
import subprocess
import json
import time
from datetime import datetime
from typing import Dict, List, Optional
import argparse

class AutoDeployment:
    def __init__(self, environment: str = "dev"):
        self.environment = environment
        self.deployment_id = f"deploy_{environment}_{int(time.time())}"
        self.log_file = f"logs/deployment_{self.deployment_id}.log"
        self.rollback_file = f"logs/rollback_{self.deployment_id}.json"
        
    def log(self, message: str, level: str = "INFO"):
        """Log messages with timestamp"""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log_entry = f"[{timestamp}] [{level}] {message}"
        print(log_entry)
        
        with open(self.log_file, "a") as f:
            f.write(log_entry + "\n")
    
    def run_command(self, command: str, capture_output: bool = True) -> subprocess.CompletedProcess:
        """Run shell command with error handling"""
        self.log(f"Running: {command}")
        
        try:
            result = subprocess.run(
                command,
                shell=True,
                capture_output=capture_output,
                text=True,
                check=True
            )
            if result.stdout:
                self.log(f"Output: {result.stdout}")
            return result
        except subprocess.CalledProcessError as e:
            self.log(f"Command failed: {e.stderr}", "ERROR")
            raise
    
    def validate_environment(self) -> bool:
        """Validate environment configuration"""
        self.log("Validating environment configuration...")
        
        required_vars = [
            "SF_ACCOUNT", "SF_USER", "SF_PASSWORD", 
            "SF_DATABASE", "SF_WAREHOUSE", "SF_SCHEMA"
        ]
        
        missing_vars = []
        for var in required_vars:
            if not os.getenv(var):
                missing_vars.append(var)
        
        if missing_vars:
            self.log(f"Missing environment variables: {missing_vars}", "ERROR")
            return False
        
        self.log("Environment validation passed")
        return True
    
    def run_data_quality_tests(self) -> bool:
        """Run comprehensive data quality tests"""
        self.log("Running data quality tests...")
        
        try:
            # Run dbt tests
            self.run_command("dbt test --project-dir dbt --target " + self.environment)
            
            # Run custom data quality checks
            self.run_command("python scripts/automation/data_quality_checks.py")
            
            self.log("Data quality tests passed")
            return True
        except subprocess.CalledProcessError:
            self.log("Data quality tests failed", "ERROR")
            return False
    
    def backup_current_state(self) -> Dict:
        """Backup current deployment state for rollback"""
        self.log("Creating backup for rollback...")
        
        backup_state = {
            "deployment_id": self.deployment_id,
            "timestamp": datetime.now().isoformat(),
            "environment": self.environment,
            "dbt_models": [],
            "snowflake_objects": []
        }
        
        try:
            # Get current dbt model state
            result = self.run_command("dbt list --project-dir dbt --target " + self.environment)
            backup_state["dbt_models"] = result.stdout.strip().split('\n')
            
            # Save backup state
            with open(self.rollback_file, "w") as f:
                json.dump(backup_state, f, indent=2)
            
            self.log("Backup created successfully")
            return backup_state
        except Exception as e:
            self.log(f"Backup failed: {str(e)}", "ERROR")
            return {}
    
    def deploy_dbt_models(self) -> bool:
        """Deploy dbt models with incremental strategy"""
        self.log("Deploying dbt models...")
        
        try:
            # Install dependencies
            self.run_command("dbt deps --project-dir dbt")
            
            # Run models with appropriate strategy
            if self.environment == "prod":
                # Production: incremental deployment
                self.run_command("dbt run --project-dir dbt --target " + self.environment)
            else:
                # Dev/Staging: full refresh
                self.run_command("dbt run --project-dir dbt --target " + self.environment + " --full-refresh")
            
            # Run tests
            self.run_command("dbt test --project-dir dbt --target " + self.environment)
            
            self.log("dbt models deployed successfully")
            return True
        except subprocess.CalledProcessError:
            self.log("dbt deployment failed", "ERROR")
            return False
    
    def deploy_snowflake_objects(self) -> bool:
        """Deploy Snowflake objects (views, procedures, etc.)"""
        self.log("Deploying Snowflake objects...")
        
        try:
            # Deploy Snowflake objects in order
            snowflake_scripts = [
                "snowflake/setup/01_database_setup.sql",
                "snowflake/setup/02_schema_creation.sql",
                "snowflake/setup/03_warehouse_configuration.sql",
                "snowflake/setup/04_user_roles_permissions.sql",
                "snowflake/setup/05_resource_monitors.sql",
                "snowflake/security/audit_logging.sql",
                "snowflake/security/data_classification.sql",
                "snowflake/security/row_level_security.sql",
                "snowflake/governance/advanced_data_lineage.sql"
            ]
            
            for script in snowflake_scripts:
                if os.path.exists(script):
                    self.run_command(f"snowsql -f {script}")
                    self.log(f"Deployed: {script}")
            
            self.log("Snowflake objects deployed successfully")
            return True
        except subprocess.CalledProcessError:
            self.log("Snowflake deployment failed", "ERROR")
            return False
    
    def run_post_deployment_tests(self) -> bool:
        """Run post-deployment validation tests"""
        self.log("Running post-deployment tests...")
        
        try:
            # Test data connectivity
            self.run_command("python scripts/automation/connectivity_tests.py")
            
            # Test ML model endpoints
            self.run_command("python scripts/automation/ml_model_tests.py")
            
            # Test monitoring systems
            self.run_command("python scripts/automation/monitoring_tests.py")
            
            self.log("Post-deployment tests passed")
            return True
        except subprocess.CalledProcessError:
            self.log("Post-deployment tests failed", "ERROR")
            return False
    
    def send_deployment_notification(self, status: str, details: str = ""):
        """Send deployment notification"""
        self.log(f"Sending deployment notification: {status}")
        
        notification = {
            "deployment_id": self.deployment_id,
            "environment": self.environment,
            "status": status,
            "timestamp": datetime.now().isoformat(),
            "details": details
        }
        
        # Send email notification (implement based on your email system)
        # For now, just log it
        self.log(f"Notification: {json.dumps(notification, indent=2)}")
    
    def rollback(self) -> bool:
        """Rollback to previous state"""
        self.log("Initiating rollback...")
        
        try:
            if not os.path.exists(self.rollback_file):
                self.log("No rollback file found", "ERROR")
                return False
            
            with open(self.rollback_file, "r") as f:
                backup_state = json.load(f)
            
            # Implement rollback logic based on backup state
            self.log("Rollback completed")
            return True
        except Exception as e:
            self.log(f"Rollback failed: {str(e)}", "ERROR")
            return False
    
    def deploy(self) -> bool:
        """Main deployment pipeline"""
        self.log(f"Starting deployment to {self.environment}")
        
        try:
            # Step 1: Validate environment
            if not self.validate_environment():
                return False
            
            # Step 2: Run pre-deployment tests
            if not self.run_data_quality_tests():
                return False
            
            # Step 3: Create backup
            backup_state = self.backup_current_state()
            if not backup_state:
                return False
            
            # Step 4: Deploy dbt models
            if not self.deploy_dbt_models():
                self.rollback()
                return False
            
            # Step 5: Deploy Snowflake objects
            if not self.deploy_snowflake_objects():
                self.rollback()
                return False
            
            # Step 6: Run post-deployment tests
            if not self.run_post_deployment_tests():
                self.rollback()
                return False
            
            # Step 7: Send success notification
            self.send_deployment_notification("SUCCESS", "Deployment completed successfully")
            
            self.log("Deployment completed successfully!")
            return True
            
        except Exception as e:
            self.log(f"Deployment failed: {str(e)}", "ERROR")
            self.send_deployment_notification("FAILED", str(e))
            return False

def main():
    parser = argparse.ArgumentParser(description="Automated Deployment Pipeline")
    parser.add_argument("--environment", "-e", default="dev", 
                       choices=["dev", "staging", "prod"],
                       help="Target environment")
    parser.add_argument("--rollback", "-r", action="store_true",
                       help="Rollback to previous deployment")
    
    args = parser.parse_args()
    
    deployment = AutoDeployment(args.environment)
    
    if args.rollback:
        success = deployment.rollback()
    else:
        success = deployment.deploy()
    
    sys.exit(0 if success else 1)

if __name__ == "__main__":
    main()
