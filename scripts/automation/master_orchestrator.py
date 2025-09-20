#!/usr/bin/env python3
"""
Master Automation Orchestrator
Coordinates all automation processes and provides a unified interface
"""

import os
import sys
import json
import time
import schedule
import argparse
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import subprocess
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

class MasterOrchestrator:
    def __init__(self, environment: str = "prod"):
        self.environment = environment
        self.automation_config = self.load_automation_config()
        self.running_processes = {}
        self.log_file = f"logs/orchestrator_{environment}_{int(time.time())}.log"
        
    def load_automation_config(self) -> Dict:
        """Load automation configuration"""
        return {
            "data_quality_monitor": {
                "enabled": True,
                "schedule": "every_15_minutes",
                "script": "scripts/automation/data_quality_monitor.py",
                "timeout": 300  # 5 minutes
            },
            "performance_optimizer": {
                "enabled": True,
                "schedule": "every_hour",
                "script": "scripts/automation/performance_optimizer.py",
                "timeout": 600  # 10 minutes
            },
            "ml_lifecycle_manager": {
                "enabled": True,
                "schedule": "daily_at_3am",
                "script": "scripts/automation/ml_lifecycle_manager.py",
                "timeout": 1800  # 30 minutes
            },
            "auto_deployment": {
                "enabled": False,  # Manual trigger only
                "script": "scripts/automation/auto_deployment.py",
                "timeout": 3600  # 1 hour
            }
        }
    
    def log(self, message: str, level: str = "INFO"):
        """Log messages with timestamp"""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log_entry = f"[{timestamp}] [{level}] {message}"
        print(log_entry)
        
        with open(self.log_file, "a") as f:
            f.write(log_entry + "\n")
    
    def run_automation_script(self, script_name: str, args: List[str] = None) -> Dict:
        """Run an automation script with timeout and error handling"""
        config = self.automation_config.get(script_name)
        if not config or not config.get("enabled", False):
            return {"status": "skipped", "reason": "disabled"}
        
        script_path = config["script"]
        timeout = config.get("timeout", 300)
        
        if not os.path.exists(script_path):
            return {"status": "error", "reason": f"Script not found: {script_path}"}
        
        self.log(f"Running {script_name}: {script_path}")
        
        try:
            # Prepare command
            cmd = [sys.executable, script_path, "--environment", self.environment]
            if args:
                cmd.extend(args)
            
            # Run script with timeout
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=timeout
            )
            
            if result.returncode == 0:
                self.log(f"✅ {script_name} completed successfully")
                return {
                    "status": "success",
                    "stdout": result.stdout,
                    "stderr": result.stderr
                }
            else:
                self.log(f"❌ {script_name} failed with return code {result.returncode}", "ERROR")
                return {
                    "status": "error",
                    "returncode": result.returncode,
                    "stdout": result.stdout,
                    "stderr": result.stderr
                }
                
        except subprocess.TimeoutExpired:
            self.log(f"⏰ {script_name} timed out after {timeout} seconds", "ERROR")
            return {"status": "timeout", "timeout": timeout}
        except Exception as e:
            self.log(f"❌ {script_name} failed with exception: {str(e)}", "ERROR")
            return {"status": "exception", "error": str(e)}
    
    def run_data_quality_monitoring(self):
        """Run data quality monitoring"""
        self.log("🔍 Starting data quality monitoring")
        result = self.run_automation_script("data_quality_monitor", ["--once"])
        return result
    
    def run_performance_optimization(self):
        """Run performance optimization"""
        self.log("⚡ Starting performance optimization")
        result = self.run_automation_script("performance_optimizer", ["--once"])
        return result
    
    def run_ml_lifecycle_management(self):
        """Run ML lifecycle management"""
        self.log("🤖 Starting ML lifecycle management")
        result = self.run_automation_script("ml_lifecycle_manager", ["--once"])
        return result
    
    def run_auto_deployment(self, target_env: str = None):
        """Run automated deployment"""
        env = target_env or self.environment
        self.log(f"🚀 Starting auto deployment to {env}")
        result = self.run_automation_script("auto_deployment", ["--environment", env])
        return result
    
    def run_parallel_automation(self, automation_types: List[str]) -> Dict:
        """Run multiple automation processes in parallel"""
        self.log(f"🔄 Running parallel automation: {', '.join(automation_types)}")
        
        results = {}
        
        with ThreadPoolExecutor(max_workers=len(automation_types)) as executor:
            # Submit all automation tasks
            future_to_type = {}
            
            for automation_type in automation_types:
                if automation_type == "data_quality":
                    future = executor.submit(self.run_data_quality_monitoring)
                elif automation_type == "performance":
                    future = executor.submit(self.run_performance_optimization)
                elif automation_type == "ml_lifecycle":
                    future = executor.submit(self.run_ml_lifecycle_management)
                else:
                    continue
                
                future_to_type[future] = automation_type
            
            # Collect results
            for future in as_completed(future_to_type):
                automation_type = future_to_type[future]
                try:
                    result = future.result()
                    results[automation_type] = result
                    self.log(f"✅ {automation_type} completed: {result['status']}")
                except Exception as e:
                    results[automation_type] = {"status": "exception", "error": str(e)}
                    self.log(f"❌ {automation_type} failed: {str(e)}", "ERROR")
        
        return results
    
    def run_health_check(self) -> Dict:
        """Run comprehensive health check"""
        self.log("🏥 Running comprehensive health check")
        
        health_status = {
            "timestamp": datetime.now().isoformat(),
            "environment": self.environment,
            "overall_status": "healthy",
            "checks": {}
        }
        
        # Check data quality
        dq_result = self.run_data_quality_monitoring()
        health_status["checks"]["data_quality"] = {
            "status": dq_result["status"],
            "details": dq_result.get("stderr", "")
        }
        
        # Check performance
        perf_result = self.run_performance_optimization()
        health_status["checks"]["performance"] = {
            "status": perf_result["status"],
            "details": perf_result.get("stderr", "")
        }
        
        # Check ML models
        ml_result = self.run_ml_lifecycle_management()
        health_status["checks"]["ml_models"] = {
            "status": ml_result["status"],
            "details": ml_result.get("stderr", "")
        }
        
        # Determine overall status
        failed_checks = [k for k, v in health_status["checks"].items() if v["status"] != "success"]
        if failed_checks:
            health_status["overall_status"] = "degraded"
            health_status["failed_checks"] = failed_checks
        
        self.log(f"🏥 Health check completed: {health_status['overall_status']}")
        return health_status
    
    def generate_automation_report(self) -> Dict:
        """Generate comprehensive automation report"""
        self.log("📊 Generating automation report")
        
        report = {
            "timestamp": datetime.now().isoformat(),
            "environment": self.environment,
            "automation_summary": {},
            "recommendations": []
        }
        
        # Run all automation checks
        automation_results = self.run_parallel_automation([
            "data_quality", "performance", "ml_lifecycle"
        ])
        
        report["automation_summary"] = automation_results
        
        # Generate recommendations
        recommendations = []
        
        for automation_type, result in automation_results.items():
            if result["status"] == "error":
                recommendations.append({
                    "type": "error_resolution",
                    "automation": automation_type,
                    "priority": "high",
                    "description": f"Fix {automation_type} automation error"
                })
            elif result["status"] == "timeout":
                recommendations.append({
                    "type": "performance_optimization",
                    "automation": automation_type,
                    "priority": "medium",
                    "description": f"Optimize {automation_type} automation performance"
                })
        
        report["recommendations"] = recommendations
        
        # Save report
        report_file = f"logs/automation_report_{self.environment}_{int(time.time())}.json"
        with open(report_file, "w") as f:
            json.dump(report, f, indent=2)
        
        self.log(f"📊 Automation report saved: {report_file}")
        return report
    
    def start_continuous_monitoring(self):
        """Start continuous automation monitoring"""
        self.log(f"🚀 Starting continuous automation monitoring for {self.environment}")
        
        # Schedule automation tasks
        schedule.every(15).minutes.do(self.run_data_quality_monitoring)
        schedule.every().hour.do(self.run_performance_optimization)
        schedule.every().day.at("03:00").do(self.run_ml_lifecycle_management)
        schedule.every().day.at("06:00").do(self.generate_automation_report)
        
        # Run initial health check
        self.run_health_check()
        
        # Keep running
        while True:
            schedule.run_pending()
            time.sleep(60)

def main():
    parser = argparse.ArgumentParser(description="Master Automation Orchestrator")
    parser.add_argument("--environment", "-e", default="prod", 
                       choices=["dev", "staging", "prod"],
                       help="Target environment")
    parser.add_argument("--action", "-a", 
                       choices=["health_check", "deploy", "monitor", "report", "parallel"],
                       help="Action to perform")
    parser.add_argument("--automation", "-auto", nargs="+",
                       choices=["data_quality", "performance", "ml_lifecycle"],
                       help="Specific automation to run")
    parser.add_argument("--target-env", help="Target environment for deployment")
    
    args = parser.parse_args()
    
    orchestrator = MasterOrchestrator(args.environment)
    
    if args.action == "health_check":
        result = orchestrator.run_health_check()
        print(json.dumps(result, indent=2))
        sys.exit(0 if result["overall_status"] == "healthy" else 1)
    
    elif args.action == "deploy":
        result = orchestrator.run_auto_deployment(args.target_env)
        print(json.dumps(result, indent=2))
        sys.exit(0 if result["status"] == "success" else 1)
    
    elif args.action == "report":
        result = orchestrator.generate_automation_report()
        print(json.dumps(result, indent=2))
        sys.exit(0)
    
    elif args.action == "parallel":
        if not args.automation:
            print("❌ Please specify automation types with --automation")
            sys.exit(1)
        result = orchestrator.run_parallel_automation(args.automation)
        print(json.dumps(result, indent=2))
        sys.exit(0)
    
    elif args.action == "monitor":
        orchestrator.start_continuous_monitoring()
    
    else:
        # Default: run health check
        result = orchestrator.run_health_check()
        print(json.dumps(result, indent=2))
        sys.exit(0 if result["overall_status"] == "healthy" else 1)

if __name__ == "__main__":
    main()
