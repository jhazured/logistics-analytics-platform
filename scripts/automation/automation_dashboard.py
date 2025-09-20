#!/usr/bin/env python3
"""
Automation Dashboard
Provides a simple web dashboard to monitor automation status and results
"""

import os
import sys
import json
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import pandas as pd
from flask import Flask, render_template, jsonify, request
import snowflake.connector
from snowflake.snowpark import Session

app = Flask(__name__)

class AutomationDashboard:
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
        
    def connect(self):
        """Establish Snowflake connection"""
        try:
            self.session = Session.builder.configs(self.connection_params).create()
            print(f"✅ Connected to Snowflake ({self.environment})")
        except Exception as e:
            print(f"❌ Failed to connect to Snowflake: {e}")
            raise
    
    def get_automation_status(self) -> Dict:
        """Get current automation status"""
        try:
            # Get data quality status
            dq_query = """
            SELECT 
                COUNT(*) as total_checks,
                COUNT(CASE WHEN sla_result = 'PASS' THEN 1 END) as passed_checks,
                COUNT(CASE WHEN sla_result = 'FAIL' THEN 1 END) as failed_checks
            FROM vw_data_quality_sla
            WHERE evaluation_timestamp >= DATEADD('day', -1, CURRENT_TIMESTAMP())
            """
            
            dq_result = self.session.sql(dq_query).collect()
            dq_status = {
                'total_checks': dq_result[0]['TOTAL_CHECKS'],
                'passed_checks': dq_result[0]['PASSED_CHECKS'],
                'failed_checks': dq_result[0]['FAILED_CHECKS'],
                'success_rate': dq_result[0]['PASSED_CHECKS'] / max(dq_result[0]['TOTAL_CHECKS'], 1)
            }
            
            # Get performance optimization status
            perf_query = """
            SELECT 
                COUNT(*) as total_recommendations,
                COUNT(CASE WHEN implementation_effort = 'LOW' THEN 1 END) as quick_wins,
                SUM(potential_savings_usd) as total_potential_savings
            FROM query_optimization_recommendations
            WHERE DATE(created_at) = CURRENT_DATE()
            """
            
            perf_result = self.session.sql(perf_query).collect()
            perf_status = {
                'total_recommendations': perf_result[0]['TOTAL_RECOMMENDATIONS'],
                'quick_wins': perf_result[0]['QUICK_WINS'],
                'total_potential_savings': perf_result[0]['TOTAL_POTENTIAL_SAVINGS']
            }
            
            # Get ML model status
            ml_query = """
            SELECT 
                COUNT(*) as total_models,
                COUNT(CASE WHEN model_health = 'HEALTHY' THEN 1 END) as healthy_models,
                COUNT(CASE WHEN retraining_needed = 'YES' THEN 1 END) as models_needing_retraining
            FROM vw_ml_model_performance
            WHERE status = 'ACTIVE'
            """
            
            ml_result = self.session.sql(ml_query).collect()
            ml_status = {
                'total_models': ml_result[0]['TOTAL_MODELS'],
                'healthy_models': ml_result[0]['HEALTHY_MODELS'],
                'models_needing_retraining': ml_result[0]['MODELS_NEEDING_RETRAINING']
            }
            
            return {
                'timestamp': datetime.now().isoformat(),
                'environment': self.environment,
                'data_quality': dq_status,
                'performance': perf_status,
                'ml_models': ml_status
            }
            
        except Exception as e:
            return {'error': str(e)}
    
    def get_recent_alerts(self) -> List[Dict]:
        """Get recent automation alerts"""
        try:
            query = """
            SELECT 
                alert_timestamp,
                alert_type,
                severity,
                alert_message,
                alert_data
            FROM alert_history
            WHERE alert_timestamp >= DATEADD('day', -7, CURRENT_TIMESTAMP())
            ORDER BY alert_timestamp DESC
            LIMIT 50
            """
            
            result = self.session.sql(query).collect()
            alerts = []
            
            for row in result:
                alerts.append({
                    'timestamp': row['ALERT_TIMESTAMP'].isoformat(),
                    'type': row['ALERT_TYPE'],
                    'severity': row['SEVERITY'],
                    'message': row['ALERT_MESSAGE'],
                    'data': row['ALERT_DATA']
                })
            
            return alerts
            
        except Exception as e:
            return [{'error': str(e)}]
    
    def get_automation_metrics(self) -> Dict:
        """Get automation metrics for the last 30 days"""
        try:
            query = """
            SELECT 
                DATE(execution_timestamp) as execution_date,
                COUNT(*) as total_executions,
                COUNT(CASE WHEN status = 'SUCCESS' THEN 1 END) as successful_executions,
                AVG(execution_duration_seconds) as avg_execution_time
            FROM automation_execution_log
            WHERE execution_timestamp >= DATEADD('day', -30, CURRENT_TIMESTAMP())
            GROUP BY DATE(execution_timestamp)
            ORDER BY execution_date DESC
            """
            
            result = self.session.sql(query).collect()
            metrics = []
            
            for row in result:
                metrics.append({
                    'date': row['EXECUTION_DATE'].isoformat(),
                    'total_executions': row['TOTAL_EXECUTIONS'],
                    'successful_executions': row['SUCCESSFUL_EXECUTIONS'],
                    'success_rate': row['SUCCESSFUL_EXECUTIONS'] / max(row['TOTAL_EXECUTIONS'], 1),
                    'avg_execution_time': row['AVG_EXECUTION_TIME']
                })
            
            return {'metrics': metrics}
            
        except Exception as e:
            return {'error': str(e)}

# Initialize dashboard
dashboard = AutomationDashboard()

@app.route('/')
def index():
    """Main dashboard page"""
    return render_template('automation_dashboard.html')

@app.route('/api/status')
def get_status():
    """Get automation status API"""
    dashboard.connect()
    status = dashboard.get_automation_status()
    return jsonify(status)

@app.route('/api/alerts')
def get_alerts():
    """Get recent alerts API"""
    dashboard.connect()
    alerts = dashboard.get_recent_alerts()
    return jsonify(alerts)

@app.route('/api/metrics')
def get_metrics():
    """Get automation metrics API"""
    dashboard.connect()
    metrics = dashboard.get_automation_metrics()
    return jsonify(metrics)

@app.route('/api/health')
def health_check():
    """Health check endpoint"""
    return jsonify({
        'status': 'healthy',
        'timestamp': datetime.now().isoformat(),
        'environment': dashboard.environment
    })

if __name__ == '__main__':
    import argparse
    
    parser = argparse.ArgumentParser(description="Automation Dashboard")
    parser.add_argument("--environment", "-e", default="prod", 
                       choices=["dev", "staging", "prod"],
                       help="Target environment")
    parser.add_argument("--port", "-p", type=int, default=5000,
                       help="Port to run the dashboard on")
    parser.add_argument("--host", default="0.0.0.0",
                       help="Host to run the dashboard on")
    
    args = parser.parse_args()
    
    dashboard = AutomationDashboard(args.environment)
    
    print(f"🚀 Starting Automation Dashboard for {args.environment}")
    print(f"📊 Dashboard will be available at http://{args.host}:{args.port}")
    
    app.run(host=args.host, port=args.port, debug=True)
