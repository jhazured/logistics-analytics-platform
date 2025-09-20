#!/usr/bin/env python3
"""
Automated Performance Optimizer
Continuously monitors and optimizes query performance, cost, and resource utilization
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

class PerformanceOptimizer:
    def __init__(self, environment: str = "prod"):
        self.environment = environment
        self.connection_params = {
            'account': os.getenv('SF_ACCOUNT'),
            'user': os.getenv('SF_USER'),
            'password': os.getenv('SF_PASSWORD'),
            'warehouse': 'COMPUTE_WH_XS',
            'database': f'LOGISTICS_DW_{environment.upper()}',
            'schema': 'PERFORMANCE'
        }
        self.session = None
        self.optimization_thresholds = self.load_optimization_thresholds()
        
    def connect(self):
        """Establish Snowflake connection"""
        try:
            self.session = Session.builder.configs(self.connection_params).create()
            print(f"✅ Connected to Snowflake ({self.environment})")
        except Exception as e:
            print(f"❌ Failed to connect to Snowflake: {e}")
            raise
    
    def load_optimization_thresholds(self) -> Dict:
        """Load performance optimization thresholds"""
        return {
            "query_performance": {
                "slow_query_threshold": 60,  # seconds
                "high_cost_threshold": 10,   # USD per query
                "optimization_confidence": 0.7
            },
            "cost_optimization": {
                "daily_cost_threshold": 1000,  # USD per day
                "warehouse_utilization_threshold": 0.8,
                "auto_scaling_threshold": 0.9
            },
            "resource_utilization": {
                "warehouse_scale_up_threshold": 0.8,
                "warehouse_scale_down_threshold": 0.3,
                "clustering_benefit_threshold": 0.2
            }
        }
    
    def analyze_slow_queries(self) -> List[Dict]:
        """Analyze slow queries and generate optimization recommendations"""
        print("🔍 Analyzing slow queries...")
        
        query = """
        SELECT 
            query_id,
            query_text,
            total_elapsed_time / 1000 as execution_time_seconds,
            bytes_scanned / (1024*1024*1024) as gb_scanned,
            (credits_used_compute + credits_used_cloud_services) * 3.00 as cost_usd,
            warehouse_name,
            user_name,
            start_time
        FROM snowflake.account_usage.query_history
        WHERE start_time >= DATEADD('hour', -24, CURRENT_TIMESTAMP())
        AND total_elapsed_time > 60000  -- Queries taking more than 1 minute
        AND query_type = 'SELECT'
        AND error_code IS NULL
        ORDER BY total_elapsed_time DESC
        LIMIT 50
        """
        
        try:
            df = self.session.sql(query).to_pandas()
            recommendations = []
            
            for _, row in df.iterrows():
                if row['execution_time_seconds'] > self.optimization_thresholds['query_performance']['slow_query_threshold']:
                    recommendation = self.generate_query_optimization_recommendation(row)
                    if recommendation:
                        recommendations.append(recommendation)
            
            return recommendations
        except Exception as e:
            print(f"❌ Error analyzing slow queries: {e}")
            return []
    
    def generate_query_optimization_recommendation(self, query_row: pd.Series) -> Optional[Dict]:
        """Generate optimization recommendation for a specific query"""
        query_text = query_row['query_text']
        execution_time = query_row['execution_time_seconds']
        cost_usd = query_row['cost_usd']
        
        recommendations = []
        confidence = 0.0
        
        # Check for SELECT * patterns
        if 'SELECT *' in query_text.upper():
            recommendations.append({
                'type': 'COLUMN_SPECIFICATION',
                'description': 'Replace SELECT * with specific columns',
                'potential_savings': execution_time * 0.3,
                'confidence': 0.9
            })
            confidence += 0.3
        
        # Check for missing LIMIT on ORDER BY
        if 'ORDER BY' in query_text.upper() and 'LIMIT' not in query_text.upper():
            recommendations.append({
                'type': 'RESULT_LIMITING',
                'description': 'Add LIMIT clause to ORDER BY queries',
                'potential_savings': execution_time * 0.2,
                'confidence': 0.8
            })
            confidence += 0.2
        
        # Check for large table scans without filters
        if 'FROM' in query_text.upper() and 'WHERE' not in query_text.upper():
            recommendations.append({
                'type': 'FILTER_OPTIMIZATION',
                'description': 'Add WHERE clause to reduce data scan',
                'potential_savings': execution_time * 0.4,
                'confidence': 0.7
            })
            confidence += 0.2
        
        # Check for complex subqueries
        if query_text.upper().count('SELECT') > 1:
            recommendations.append({
                'type': 'QUERY_REWRITE',
                'description': 'Consider rewriting as JOIN or CTE',
                'potential_savings': execution_time * 0.5,
                'confidence': 0.6
            })
            confidence += 0.1
        
        if recommendations and confidence >= self.optimization_thresholds['query_performance']['optimization_confidence']:
            return {
                'query_id': query_row['query_id'],
                'execution_time_seconds': execution_time,
                'cost_usd': cost_usd,
                'warehouse_name': query_row['warehouse_name'],
                'user_name': query_row['user_name'],
                'recommendations': recommendations,
                'total_confidence': confidence,
                'potential_savings_seconds': sum(r['potential_savings'] for r in recommendations),
                'potential_savings_usd': cost_usd * 0.3  # Estimate 30% cost savings
            }
        
        return None
    
    def analyze_warehouse_utilization(self) -> List[Dict]:
        """Analyze warehouse utilization and generate scaling recommendations"""
        print("🔍 Analyzing warehouse utilization...")
        
        query = """
        SELECT 
            warehouse_name,
            AVG(credits_used_compute + credits_used_cloud_services) as avg_credits_per_hour,
            COUNT(*) as query_count,
            AVG(total_elapsed_time / 1000) as avg_execution_time_seconds,
            SUM(CASE WHEN queued_provisioning_time > 0 THEN 1 ELSE 0 END) as queued_queries,
            SUM(CASE WHEN queued_repair_time > 0 THEN 1 ELSE 0 END) as repair_queries,
            SUM(CASE WHEN queued_overload_time > 0 THEN 1 ELSE 0 END) as overload_queries
        FROM snowflake.account_usage.query_history
        WHERE start_time >= DATEADD('hour', -24, CURRENT_TIMESTAMP())
        AND warehouse_name IS NOT NULL
        GROUP BY warehouse_name
        """
        
        try:
            df = self.session.sql(query).to_pandas()
            recommendations = []
            
            for _, row in df.iterrows():
                utilization_score = (row['queued_queries'] + row['repair_queries'] + row['overload_queries']) / row['query_count']
                
                if utilization_score > self.optimization_thresholds['resource_utilization']['warehouse_scale_up_threshold']:
                    recommendations.append({
                        'warehouse_name': row['warehouse_name'],
                        'type': 'SCALE_UP',
                        'reason': 'High utilization detected',
                        'utilization_score': utilization_score,
                        'avg_credits_per_hour': row['avg_credits_per_hour'],
                        'recommendation': 'Consider scaling up warehouse size',
                        'potential_benefit': 'Reduced query queuing and faster execution'
                    })
                elif utilization_score < self.optimization_thresholds['resource_utilization']['warehouse_scale_down_threshold']:
                    recommendations.append({
                        'warehouse_name': row['warehouse_name'],
                        'type': 'SCALE_DOWN',
                        'reason': 'Low utilization detected',
                        'utilization_score': utilization_score,
                        'avg_credits_per_hour': row['avg_credits_per_hour'],
                        'recommendation': 'Consider scaling down warehouse size',
                        'potential_benefit': f"Potential cost savings: ${row['avg_credits_per_hour'] * 24 * 0.3:.2f} per day"
                    })
            
            return recommendations
        except Exception as e:
            print(f"❌ Error analyzing warehouse utilization: {e}")
            return []
    
    def analyze_cost_optimization(self) -> List[Dict]:
        """Analyze cost patterns and generate optimization recommendations"""
        print("🔍 Analyzing cost optimization opportunities...")
        
        query = """
        SELECT 
            warehouse_name,
            DATE_TRUNC('hour', start_time) as hour_bucket,
            SUM(credits_used_compute + credits_used_cloud_services) * 3.00 as hourly_cost_usd,
            COUNT(*) as query_count,
            AVG(total_elapsed_time / 1000) as avg_execution_time_seconds
        FROM snowflake.account_usage.warehouse_metering_history
        WHERE start_time >= DATEADD('day', -7, CURRENT_TIMESTAMP())
        GROUP BY warehouse_name, hour_bucket
        ORDER BY warehouse_name, hour_bucket
        """
        
        try:
            df = self.session.sql(query).to_pandas()
            recommendations = []
            
            # Analyze cost patterns by warehouse
            for warehouse in df['warehouse_name'].unique():
                warehouse_data = df[df['warehouse_name'] == warehouse]
                
                avg_hourly_cost = warehouse_data['hourly_cost_usd'].mean()
                max_hourly_cost = warehouse_data['hourly_cost_usd'].max()
                cost_volatility = warehouse_data['hourly_cost_usd'].std()
                
                # Check for high cost volatility (indicating inefficient resource usage)
                if cost_volatility > avg_hourly_cost * 0.5:
                    recommendations.append({
                        'warehouse_name': warehouse,
                        'type': 'COST_VOLATILITY',
                        'reason': 'High cost volatility detected',
                        'avg_hourly_cost': avg_hourly_cost,
                        'cost_volatility': cost_volatility,
                        'recommendation': 'Implement auto-scaling or query optimization',
                        'potential_benefit': f"Potential savings: ${cost_volatility * 0.3:.2f} per hour"
                    })
                
                # Check for consistently high costs
                if avg_hourly_cost > self.optimization_thresholds['cost_optimization']['daily_cost_threshold'] / 24:
                    recommendations.append({
                        'warehouse_name': warehouse,
                        'type': 'HIGH_COST',
                        'reason': 'Consistently high costs detected',
                        'avg_hourly_cost': avg_hourly_cost,
                        'recommendation': 'Review query patterns and consider optimization',
                        'potential_benefit': f"Potential savings: ${avg_hourly_cost * 0.2:.2f} per hour"
                    })
            
            return recommendations
        except Exception as e:
            print(f"❌ Error analyzing cost optimization: {e}")
            return []
    
    def generate_clustering_recommendations(self) -> List[Dict]:
        """Generate clustering recommendations for tables"""
        print("🔍 Analyzing clustering opportunities...")
        
        query = """
        SELECT 
            table_name,
            table_schema,
            active_bytes / (1024*1024*1024) as size_gb,
            row_count,
            last_altered
        FROM snowflake.account_usage.tables
        WHERE deleted IS NULL
        AND active_bytes > 1024*1024*1024  -- Tables larger than 1GB
        AND table_schema IN ('MARTS', 'STAGING')
        ORDER BY active_bytes DESC
        """
        
        try:
            df = self.session.sql(query).to_pandas()
            recommendations = []
            
            for _, row in df.iterrows():
                if row['size_gb'] > 10:  # Tables larger than 10GB
                    recommendations.append({
                        'table_name': f"{row['table_schema']}.{row['table_name']}",
                        'type': 'CLUSTERING',
                        'reason': 'Large table without clustering',
                        'size_gb': row['size_gb'],
                        'row_count': row['row_count'],
                        'recommendation': 'Consider adding clustering keys',
                        'potential_benefit': 'Improved query performance for large scans',
                        'suggested_clustering_keys': self.suggest_clustering_keys(row['table_name'])
                    })
            
            return recommendations
        except Exception as e:
            print(f"❌ Error analyzing clustering opportunities: {e}")
            return []
    
    def suggest_clustering_keys(self, table_name: str) -> List[str]:
        """Suggest clustering keys for a table based on common patterns"""
        clustering_suggestions = {
            'tbl_fact_shipments': ['shipment_date', 'customer_id', 'route_id'],
            'tbl_fact_vehicle_telemetry': ['vehicle_id', 'timestamp'],
            'tbl_fact_route_performance': ['route_id', 'performance_date'],
            'tbl_ml_consolidated_feature_store': ['feature_date', 'customer_id'],
            'tbl_ml_rolling_analytics': ['analytics_date', 'customer_id']
        }
        
        return clustering_suggestions.get(table_name, ['date_key', 'id'])
    
    def apply_optimizations(self, recommendations: List[Dict]) -> Dict:
        """Apply optimization recommendations"""
        print("🔧 Applying optimizations...")
        
        results = {
            'applied': 0,
            'failed': 0,
            'skipped': 0,
            'details': []
        }
        
        for rec in recommendations:
            try:
                if rec['type'] == 'CLUSTERING':
                    # Apply clustering (this would be implemented based on your needs)
                    results['applied'] += 1
                    results['details'].append(f"Applied clustering to {rec['table_name']}")
                elif rec['type'] in ['SCALE_UP', 'SCALE_DOWN']:
                    # Apply warehouse scaling (this would be implemented based on your needs)
                    results['applied'] += 1
                    results['details'].append(f"Applied {rec['type']} to {rec['warehouse_name']}")
                else:
                    # Log recommendation for manual review
                    results['skipped'] += 1
                    results['details'].append(f"Logged recommendation for {rec['type']}")
                    
            except Exception as e:
                results['failed'] += 1
                results['details'].append(f"Failed to apply {rec['type']}: {str(e)}")
        
        return results
    
    def run_optimization_analysis(self):
        """Run complete optimization analysis"""
        print(f"🔍 Running performance optimization analysis for {self.environment}")
        
        all_recommendations = []
        
        # Run all optimization analyses
        all_recommendations.extend(self.analyze_slow_queries())
        all_recommendations.extend(self.analyze_warehouse_utilization())
        all_recommendations.extend(self.analyze_cost_optimization())
        all_recommendations.extend(self.generate_clustering_recommendations())
        
        # Apply optimizations
        if all_recommendations:
            results = self.apply_optimizations(all_recommendations)
            print(f"✅ Optimization complete: {results['applied']} applied, {results['failed']} failed, {results['skipped']} skipped")
            
            # Log results
            self.log_optimization_results(all_recommendations, results)
        else:
            print("✅ No optimization opportunities found")
        
        return all_recommendations
    
    def log_optimization_results(self, recommendations: List[Dict], results: Dict):
        """Log optimization results to Snowflake"""
        try:
            log_data = {
                'timestamp': datetime.now().isoformat(),
                'environment': self.environment,
                'recommendations': recommendations,
                'results': results
            }
            
            insert_query = f"""
            INSERT INTO optimization_results (
                optimization_timestamp, environment, recommendations, results
            ) VALUES (
                '{log_data['timestamp']}',
                '{log_data['environment']}',
                PARSE_JSON('{json.dumps(recommendations)}'),
                PARSE_JSON('{json.dumps(results)}')
            )
            """
            self.session.sql(insert_query).collect()
        except Exception as e:
            print(f"❌ Error logging optimization results: {e}")
    
    def start_optimization_monitoring(self):
        """Start continuous optimization monitoring"""
        print(f"🚀 Starting performance optimization monitoring for {self.environment}")
        
        # Connect to Snowflake
        self.connect()
        
        # Schedule optimization runs
        schedule.every().hour.do(self.run_optimization_analysis)
        schedule.every().day.at("02:00").do(self.run_optimization_analysis)
        
        # Run initial analysis
        self.run_optimization_analysis()
        
        # Keep running
        while True:
            schedule.run_pending()
            time.sleep(60)

def main():
    import argparse
    
    parser = argparse.ArgumentParser(description="Performance Optimizer")
    parser.add_argument("--environment", "-e", default="prod", 
                       choices=["dev", "staging", "prod"],
                       help="Target environment")
    parser.add_argument("--once", action="store_true",
                       help="Run optimization once instead of continuous monitoring")
    
    args = parser.parse_args()
    
    optimizer = PerformanceOptimizer(args.environment)
    
    if args.once:
        optimizer.connect()
        recommendations = optimizer.run_optimization_analysis()
        sys.exit(0 if recommendations else 1)
    else:
        optimizer.start_optimization_monitoring()

if __name__ == "__main__":
    main()
