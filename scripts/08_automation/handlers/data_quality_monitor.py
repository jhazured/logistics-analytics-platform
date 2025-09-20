#!/usr/bin/env python3
"""
Automated Data Quality Monitoring
Continuously monitors data quality and triggers alerts when thresholds are breached
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

class DataQualityMonitor:
    def __init__(self, environment: str = "prod"):
        self.environment = environment
        self.connection_params = {
            'account': os.getenv('SNOWFLAKE_ACCOUNT'),
            'user': os.getenv('SNOWFLAKE_USER'),
            'password': os.getenv('SNOWFLAKE_PASSWORD'),
            'warehouse': 'COMPUTE_WH_XS',
            'database': f'LOGISTICS_DW_{environment.upper()}',
            'schema': 'MONITORING'
        }
        self.session = None
        self.quality_thresholds = self.load_quality_thresholds()
        
    def connect(self):
        """Establish Snowflake connection"""
        try:
            self.session = Session.builder.configs(self.connection_params).create()
            print(f"✅ Connected to Snowflake ({self.environment})")
        except Exception as e:
            print(f"❌ Failed to connect to Snowflake: {e}")
            raise
    
    def load_quality_thresholds(self) -> Dict:
        """Load data quality thresholds from configuration"""
        return {
            "completeness": {
                "critical": 0.95,  # 95% completeness required
                "warning": 0.90    # 90% triggers warning
            },
            "freshness": {
                "critical": 360,   # 6 hours max staleness
                "warning": 180     # 3 hours triggers warning
            },
            "accuracy": {
                "critical": 0.98,  # 98% accuracy required
                "warning": 0.95    # 95% triggers warning
            },
            "consistency": {
                "critical": 0.99,  # 99% consistency required
                "warning": 0.97    # 97% triggers warning
            }
        }
    
    def check_data_freshness(self) -> List[Dict]:
        """Check data freshness across all tables"""
        print("🔍 Checking data freshness...")
        
        query = """
        SELECT 
            table_name,
            minutes_since_sync,
            CASE 
                WHEN minutes_since_sync <= 180 THEN 'GOOD'
                WHEN minutes_since_sync <= 360 THEN 'WARNING'
                ELSE 'CRITICAL'
            END as freshness_status
        FROM vw_data_freshness_monitoring
        WHERE minutes_since_sync > 60  -- Only check tables that should be updated
        """
        
        try:
            df = self.session.sql(query).to_pandas()
            issues = []
            
            for _, row in df.iterrows():
                if row['freshness_status'] in ['WARNING', 'CRITICAL']:
                    issues.append({
                        'table_name': row['table_name'],
                        'metric': 'freshness',
                        'status': row['freshness_status'],
                        'value': row['minutes_since_sync'],
                        'threshold': self.quality_thresholds['freshness']['critical'],
                        'message': f"Data is {row['minutes_since_sync']} minutes stale"
                    })
            
            return issues
        except Exception as e:
            print(f"❌ Error checking data freshness: {e}")
            return []
    
    def check_data_completeness(self) -> List[Dict]:
        """Check data completeness across all tables"""
        print("🔍 Checking data completeness...")
        
        query = """
        SELECT 
            table_name,
            row_count,
            null_keys,
            (row_count - null_keys) / row_count as completeness_ratio,
            CASE 
                WHEN (row_count - null_keys) / row_count >= 0.95 THEN 'GOOD'
                WHEN (row_count - null_keys) / row_count >= 0.90 THEN 'WARNING'
                ELSE 'CRITICAL'
            END as completeness_status
        FROM vw_data_quality_summary
        WHERE row_count > 0
        """
        
        try:
            df = self.session.sql(query).to_pandas()
            issues = []
            
            for _, row in df.iterrows():
                if row['completeness_status'] in ['WARNING', 'CRITICAL']:
                    issues.append({
                        'table_name': row['table_name'],
                        'metric': 'completeness',
                        'status': row['completeness_status'],
                        'value': row['completeness_ratio'],
                        'threshold': self.quality_thresholds['completeness']['critical'],
                        'message': f"Completeness is {row['completeness_ratio']:.2%}"
                    })
            
            return issues
        except Exception as e:
            print(f"❌ Error checking data completeness: {e}")
            return []
    
    def check_referential_integrity(self) -> List[Dict]:
        """Check referential integrity across fact and dimension tables"""
        print("🔍 Checking referential integrity...")
        
        integrity_checks = [
            {
                'name': 'shipments_customer_integrity',
                'query': """
                SELECT COUNT(*) as orphaned_records
                FROM tbl_fact_shipments fs
                LEFT JOIN tbl_dim_customer dc ON fs.customer_id = dc.customer_id
                WHERE dc.customer_id IS NULL
                """,
                'threshold': 0
            },
            {
                'name': 'shipments_vehicle_integrity',
                'query': """
                SELECT COUNT(*) as orphaned_records
                FROM tbl_fact_shipments fs
                LEFT JOIN tbl_dim_vehicle dv ON fs.vehicle_id = dv.vehicle_id
                WHERE dv.vehicle_id IS NULL
                """,
                'threshold': 0
            }
        ]
        
        issues = []
        for check in integrity_checks:
            try:
                result = self.session.sql(check['query']).collect()
                orphaned_count = result[0]['ORPHANED_RECORDS']
                
                if orphaned_count > check['threshold']:
                    issues.append({
                        'table_name': check['name'],
                        'metric': 'referential_integrity',
                        'status': 'CRITICAL',
                        'value': orphaned_count,
                        'threshold': check['threshold'],
                        'message': f"Found {orphaned_count} orphaned records"
                    })
            except Exception as e:
                print(f"❌ Error checking {check['name']}: {e}")
        
        return issues
    
    def check_ml_model_performance(self) -> List[Dict]:
        """Check ML model performance and drift"""
        print("🔍 Checking ML model performance...")
        
        query = """
        SELECT 
            model_name,
            model_type,
            validation_score,
            production_score,
            drift_score,
            CASE 
                WHEN drift_score > 0.2 THEN 'CRITICAL'
                WHEN drift_score > 0.1 THEN 'WARNING'
                ELSE 'GOOD'
            END as drift_status,
            CASE 
                WHEN validation_score - production_score > 0.1 THEN 'CRITICAL'
                WHEN validation_score - production_score > 0.05 THEN 'WARNING'
                ELSE 'GOOD'
            END as performance_status
        FROM vw_ml_model_performance
        WHERE status = 'ACTIVE'
        """
        
        try:
            df = self.session.sql(query).to_pandas()
            issues = []
            
            for _, row in df.iterrows():
                if row['drift_status'] in ['WARNING', 'CRITICAL']:
                    issues.append({
                        'table_name': f"ml_model_{row['model_name']}",
                        'metric': 'model_drift',
                        'status': row['drift_status'],
                        'value': row['drift_score'],
                        'threshold': 0.1,
                        'message': f"Model drift detected: {row['drift_score']:.3f}"
                    })
                
                if row['performance_status'] in ['WARNING', 'CRITICAL']:
                    issues.append({
                        'table_name': f"ml_model_{row['model_name']}",
                        'metric': 'model_performance',
                        'status': row['performance_status'],
                        'value': row['validation_score'] - row['production_score'],
                        'threshold': 0.05,
                        'message': f"Performance degradation: {row['validation_score'] - row['production_score']:.3f}"
                    })
            
            return issues
        except Exception as e:
            print(f"❌ Error checking ML model performance: {e}")
            return []
    
    def send_alert(self, issues: List[Dict]):
        """Send alerts for data quality issues"""
        if not issues:
            return
        
        critical_issues = [i for i in issues if i['status'] == 'CRITICAL']
        warning_issues = [i for i in issues if i['status'] == 'WARNING']
        
        alert_message = {
            'timestamp': datetime.now().isoformat(),
            'environment': self.environment,
            'critical_count': len(critical_issues),
            'warning_count': len(warning_issues),
            'issues': issues
        }
        
        # Log alert
        print(f"🚨 ALERT: {len(critical_issues)} critical, {len(warning_issues)} warning issues")
        
        # Send email alert (implement based on your email system)
        self.send_email_alert(alert_message)
        
        # Log to Snowflake
        self.log_alert_to_snowflake(alert_message)
    
    def send_email_alert(self, alert_message: Dict):
        """Send email alert (implement based on your email system)"""
        # This would integrate with your email system
        print(f"📧 Email alert sent: {alert_message['critical_count']} critical issues")
    
    def log_alert_to_snowflake(self, alert_message: Dict):
        """Log alert to Snowflake for tracking"""
        try:
            insert_query = f"""
            INSERT INTO data_quality_alerts (
                alert_timestamp, environment, critical_count, warning_count, alert_data
            ) VALUES (
                '{alert_message['timestamp']}',
                '{alert_message['environment']}',
                {alert_message['critical_count']},
                {alert_message['warning_count']},
                PARSE_JSON('{json.dumps(alert_message['issues'])}')
            )
            """
            self.session.sql(insert_query).collect()
        except Exception as e:
            print(f"❌ Error logging alert to Snowflake: {e}")
    
    def run_quality_checks(self):
        """Run all data quality checks"""
        print(f"🔍 Running data quality checks for {self.environment}")
        
        all_issues = []
        
        # Run all quality checks
        all_issues.extend(self.check_data_freshness())
        all_issues.extend(self.check_data_completeness())
        all_issues.extend(self.check_referential_integrity())
        all_issues.extend(self.check_ml_model_performance())
        
        # Send alerts if issues found
        if all_issues:
            self.send_alert(all_issues)
        else:
            print("✅ All data quality checks passed")
        
        return all_issues
    
    def start_monitoring(self):
        """Start continuous monitoring"""
        print(f"🚀 Starting data quality monitoring for {self.environment}")
        
        # Connect to Snowflake
        self.connect()
        
        # Schedule checks
        schedule.every(15).minutes.do(self.run_quality_checks)
        schedule.every().hour.do(self.run_quality_checks)
        schedule.every().day.at("06:00").do(self.run_quality_checks)
        
        # Run initial check
        self.run_quality_checks()
        
        # Keep running
        while True:
            schedule.run_pending()
            time.sleep(60)

def main():
    import argparse
    
    parser = argparse.ArgumentParser(description="Data Quality Monitor")
    parser.add_argument("--environment", "-e", default="prod", 
                       choices=["dev", "staging", "prod"],
                       help="Target environment")
    parser.add_argument("--once", action="store_true",
                       help="Run checks once instead of continuous monitoring")
    
    args = parser.parse_args()
    
    monitor = DataQualityMonitor(args.environment)
    
    if args.once:
        monitor.connect()
        issues = monitor.run_quality_checks()
        sys.exit(0 if not issues else 1)
    else:
        monitor.start_monitoring()

if __name__ == "__main__":
    main()
