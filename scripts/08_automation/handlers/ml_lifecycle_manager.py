#!/usr/bin/env python3
"""
ML Lifecycle Manager
Automates ML model training, deployment, monitoring, and retraining
"""

import os
import sys
import json
import time
import schedule
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import snowflake.connector
from snowflake.snowpark import Session
import pandas as pd
import subprocess

class MLLifecycleManager:
    def __init__(self, environment: str = "prod"):
        self.environment = environment
        self.connection_params = {
            'account': os.getenv('SNOWFLAKE_ACCOUNT'),
            'user': os.getenv('SNOWFLAKE_USER'),
            'password': os.getenv('SNOWFLAKE_PASSWORD'),
            'warehouse': 'COMPUTE_WH_MEDIUM',
            'database': f'LOGISTICS_DW_{environment.upper()}',
            'schema': 'ML_OBJECTS'
        }
        self.session = None
        self.model_configs = self.load_model_configs()
        
    def connect(self):
        """Establish Snowflake connection"""
        try:
            self.session = Session.builder.configs(self.connection_params).create()
            print(f"✅ Connected to Snowflake ({self.environment})")
        except Exception as e:
            print(f"❌ Failed to connect to Snowflake: {e}")
            raise
    
    def load_model_configs(self) -> Dict:
        """Load ML model configurations"""
        return {
            "route_optimization": {
                "model_name": "route_optimization_model",
                "training_script": "dbt/models/ml_models/train_route_optimization_model.py",
                "retrain_frequency": "weekly",
                "performance_threshold": 0.8,
                "drift_threshold": 0.2,
                "feature_table": "tbl_ml_consolidated_feature_store",
                "target_column": "optimized_delivery_time_minutes"
            },
            "predictive_maintenance": {
                "model_name": "predictive_maintenance_model",
                "training_script": "dbt/models/ml_models/train_predictive_maintenance_model.py",
                "retrain_frequency": "weekly",
                "performance_threshold": 0.9,
                "drift_threshold": 0.15,
                "feature_table": "tbl_ml_maintenance_features",
                "target_column": "maintenance_needed_30d"
            }
        }
    
    def check_model_performance(self) -> List[Dict]:
        """Check ML model performance and identify models needing retraining"""
        print("🔍 Checking ML model performance...")
        
        query = """
        SELECT 
            model_name,
            model_type,
            validation_score,
            production_score,
            drift_score,
            last_training_date,
            prediction_count,
            DATEDIFF('day', last_training_date, CURRENT_DATE()) as days_since_training
        FROM vw_ml_model_performance
        WHERE status = 'ACTIVE'
        """
        
        try:
            df = self.session.sql(query).to_pandas()
            retrain_candidates = []
            
            for _, row in df.iterrows():
                model_config = self.model_configs.get(row['model_type'], {})
                
                # Check if model needs retraining
                needs_retrain = False
                reasons = []
                
                # Check performance degradation
                if row['production_score'] < model_config.get('performance_threshold', 0.8):
                    needs_retrain = True
                    reasons.append(f"Performance below threshold: {row['production_score']:.3f}")
                
                # Check model drift
                if row['drift_score'] > model_config.get('drift_threshold', 0.2):
                    needs_retrain = True
                    reasons.append(f"Drift above threshold: {row['drift_score']:.3f}")
                
                # Check training age
                if row['days_since_training'] > 7:  # Weekly retraining
                    needs_retrain = True
                    reasons.append(f"Model age: {row['days_since_training']} days")
                
                if needs_retrain:
                    retrain_candidates.append({
                        'model_name': row['model_name'],
                        'model_type': row['model_type'],
                        'reasons': reasons,
                        'current_performance': row['production_score'],
                        'drift_score': row['drift_score'],
                        'days_since_training': row['days_since_training']
                    })
            
            return retrain_candidates
        except Exception as e:
            print(f"❌ Error checking model performance: {e}")
            return []
    
    def train_model(self, model_type: str) -> bool:
        """Train a specific ML model"""
        print(f"🚀 Training {model_type} model...")
        
        model_config = self.model_configs.get(model_type)
        if not model_config:
            print(f"❌ No configuration found for model type: {model_type}")
            return False
        
        training_script = model_config['training_script']
        
        try:
            # Run training script
            result = subprocess.run(
                [sys.executable, training_script],
                capture_output=True,
                text=True,
                check=True
            )
            
            print(f"✅ {model_type} model training completed")
            print(f"Output: {result.stdout}")
            
            # Log training completion
            self.log_training_completion(model_type, "SUCCESS", result.stdout)
            
            return True
        except subprocess.CalledProcessError as e:
            print(f"❌ {model_type} model training failed: {e.stderr}")
            self.log_training_completion(model_type, "FAILED", e.stderr)
            return False
    
    def deploy_model(self, model_type: str) -> bool:
        """Deploy a trained model to production"""
        print(f"🚀 Deploying {model_type} model...")
        
        try:
            # Update model status in registry
            update_query = f"""
            UPDATE tbl_ml_model_registry
            SET status = 'ACTIVE',
                deployment_date = CURRENT_TIMESTAMP(),
                updated_at = CURRENT_TIMESTAMP()
            WHERE model_type = '{model_type}'
            AND status = 'TRAINING'
            """
            
            self.session.sql(update_query).collect()
            
            # Test model deployment
            test_result = self.test_model_deployment(model_type)
            
            if test_result:
                print(f"✅ {model_type} model deployed successfully")
                self.log_deployment(model_type, "SUCCESS", "Model deployed and tested")
                return True
            else:
                print(f"❌ {model_type} model deployment test failed")
                self.log_deployment(model_type, "FAILED", "Deployment test failed")
                return False
                
        except Exception as e:
            print(f"❌ {model_type} model deployment failed: {e}")
            self.log_deployment(model_type, "FAILED", str(e))
            return False
    
    def test_model_deployment(self, model_type: str) -> bool:
        """Test deployed model with sample data"""
        print(f"🧪 Testing {model_type} model deployment...")
        
        try:
            if model_type == "route_optimization":
                test_query = """
                SELECT predict_route_optimization(
                    0.8, 0.2, 0.1, 0.7, 100, 8.5, 2.5, 0.95, 4.2
                ) as prediction
                """
            elif model_type == "predictive_maintenance":
                test_query = """
                SELECT predict_maintenance_risk(
                    1, 2020, 50000, 8.5, 4, 190, 2000, 75, 800, 5, 100, 3, 500, 90, 0.85, 8.2, 150
                ) as prediction
                """
            else:
                print(f"❌ Unknown model type for testing: {model_type}")
                return False
            
            result = self.session.sql(test_query).collect()
            prediction = result[0]['PREDICTION']
            
            if prediction is not None:
                print(f"✅ Model test successful, prediction: {prediction}")
                return True
            else:
                print(f"❌ Model test failed, no prediction returned")
                return False
                
        except Exception as e:
            print(f"❌ Model test failed: {e}")
            return False
    
    def monitor_model_drift(self) -> List[Dict]:
        """Monitor model drift and performance degradation"""
        print("🔍 Monitoring model drift...")
        
        query = """
        SELECT 
            model_name,
            model_type,
            drift_score,
            validation_score,
            production_score,
            prediction_count,
            last_prediction_date
        FROM vw_ml_model_drift_detection
        WHERE retraining_needed = 'YES'
        """
        
        try:
            df = self.session.sql(query).to_pandas()
            drift_alerts = []
            
            for _, row in df.iterrows():
                drift_alerts.append({
                    'model_name': row['model_name'],
                    'model_type': row['model_type'],
                    'drift_score': row['drift_score'],
                    'performance_degradation': row['validation_score'] - row['production_score'],
                    'prediction_count': row['prediction_count'],
                    'last_prediction_date': row['last_prediction_date']
                })
            
            return drift_alerts
        except Exception as e:
            print(f"❌ Error monitoring model drift: {e}")
            return []
    
    def run_ab_testing(self, model_type: str) -> Dict:
        """Run A/B testing between current and new model"""
        print(f"🧪 Running A/B testing for {model_type} model...")
        
        try:
            # Create A/B test configuration
            ab_test_config = {
                'test_id': f"ab_test_{model_type}_{int(time.time())}",
                'model_a': f"{model_type}_current",
                'model_b': f"{model_type}_new",
                'test_start_date': datetime.now().isoformat(),
                'test_duration_days': 7,
                'traffic_split': 0.5  # 50/50 split
            }
            
            # Log A/B test start
            insert_query = f"""
            INSERT INTO model_ab_test_results (
                test_id, model_a_name, model_b_name, test_start_date, test_end_date
            ) VALUES (
                '{ab_test_config['test_id']}',
                '{ab_test_config['model_a']}',
                '{ab_test_config['model_b']}',
                '{ab_test_config['test_start_date']}',
                DATEADD('day', {ab_test_config['test_duration_days']}, '{ab_test_config['test_start_date']}')
            )
            """
            
            self.session.sql(insert_query).collect()
            
            print(f"✅ A/B test started: {ab_test_config['test_id']}")
            return ab_test_config
            
        except Exception as e:
            print(f"❌ A/B testing failed: {e}")
            return {}
    
    def log_training_completion(self, model_type: str, status: str, details: str):
        """Log model training completion"""
        try:
            insert_query = f"""
            INSERT INTO ml_training_log (
                model_type, training_status, training_details, training_timestamp
            ) VALUES (
                '{model_type}',
                '{status}',
                '{details}',
                CURRENT_TIMESTAMP()
            )
            """
            self.session.sql(insert_query).collect()
        except Exception as e:
            print(f"❌ Error logging training completion: {e}")
    
    def log_deployment(self, model_type: str, status: str, details: str):
        """Log model deployment"""
        try:
            insert_query = f"""
            INSERT INTO ml_deployment_log (
                model_type, deployment_status, deployment_details, deployment_timestamp
            ) VALUES (
                '{model_type}',
                '{status}',
                '{details}',
                CURRENT_TIMESTAMP()
            )
            """
            self.session.sql(insert_query).collect()
        except Exception as e:
            print(f"❌ Error logging deployment: {e}")
    
    def run_ml_lifecycle_management(self):
        """Run complete ML lifecycle management"""
        print(f"🔍 Running ML lifecycle management for {self.environment}")
        
        # Check model performance
        retrain_candidates = self.check_model_performance()
        
        # Check for drift
        drift_alerts = self.monitor_model_drift()
        
        # Process retraining candidates
        for candidate in retrain_candidates:
            print(f"🔄 Retraining {candidate['model_name']} due to: {', '.join(candidate['reasons'])}")
            
            # Train model
            if self.train_model(candidate['model_type']):
                # Deploy model
                if self.deploy_model(candidate['model_type']):
                    print(f"✅ {candidate['model_name']} retrained and deployed successfully")
                else:
                    print(f"❌ {candidate['model_name']} deployment failed")
            else:
                print(f"❌ {candidate['model_name']} training failed")
        
        # Process drift alerts
        for alert in drift_alerts:
            print(f"🚨 Drift alert for {alert['model_name']}: drift={alert['drift_score']:.3f}")
            
            # Start A/B testing for drift alerts
            ab_test = self.run_ab_testing(alert['model_type'])
            if ab_test:
                print(f"🧪 A/B test started for {alert['model_name']}")
        
        return len(retrain_candidates) + len(drift_alerts)
    
    def start_ml_lifecycle_monitoring(self):
        """Start continuous ML lifecycle monitoring"""
        print(f"🚀 Starting ML lifecycle monitoring for {self.environment}")
        
        # Connect to Snowflake
        self.connect()
        
        # Schedule ML lifecycle management
        schedule.every().day.at("03:00").do(self.run_ml_lifecycle_management)
        schedule.every().hour.do(self.monitor_model_drift)
        
        # Run initial check
        self.run_ml_lifecycle_management()
        
        # Keep running
        while True:
            schedule.run_pending()
            time.sleep(60)

def main():
    import argparse
    
    parser = argparse.ArgumentParser(description="ML Lifecycle Manager")
    parser.add_argument("--environment", "-e", default="prod", 
                       choices=["dev", "staging", "prod"],
                       help="Target environment")
    parser.add_argument("--once", action="store_true",
                       help="Run lifecycle management once instead of continuous monitoring")
    parser.add_argument("--train", type=str, choices=["route_optimization", "predictive_maintenance"],
                       help="Train a specific model")
    parser.add_argument("--deploy", type=str, choices=["route_optimization", "predictive_maintenance"],
                       help="Deploy a specific model")
    
    args = parser.parse_args()
    
    manager = MLLifecycleManager(args.environment)
    
    if args.train:
        manager.connect()
        success = manager.train_model(args.train)
        sys.exit(0 if success else 1)
    elif args.deploy:
        manager.connect()
        success = manager.deploy_model(args.deploy)
        sys.exit(0 if success else 1)
    elif args.once:
        manager.connect()
        issues = manager.run_ml_lifecycle_management()
        sys.exit(0 if not issues else 1)
    else:
        manager.start_ml_lifecycle_monitoring()

if __name__ == "__main__":
    main()
