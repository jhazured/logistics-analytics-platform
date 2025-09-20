#!/usr/bin/env python3
"""
Predictive Maintenance ML Model Training
Trains and deploys ML models for predictive maintenance using Snowflake ML
"""

import pandas as pd
import numpy as np
from sklearn.ensemble import RandomForestClassifier, GradientBoostingClassifier
from sklearn.model_selection import train_test_split, cross_val_score
from sklearn.metrics import classification_report, confusion_matrix, roc_auc_score
import joblib
import json
from datetime import datetime
from snowflake.snowpark import Session
import os

class PredictiveMaintenanceModelTrainer:
    def __init__(self):
        self.connection_params = {
            'account': os.getenv('SNOWFLAKE_ACCOUNT'),
            'user': os.getenv('SNOWFLAKE_USER'),
            'password': os.getenv('SNOWFLAKE_PASSWORD'),
            'warehouse': 'COMPUTE_WH_MEDIUM',
            'database': 'LOGISTICS_DW_PROD',
            'schema': 'MARTS'
        }
        self.session = Session.builder.configs(self.connection_params).create()
        
    def load_training_data(self):
        """Load features from the consolidated feature store and maintenance data"""
        query = """
        WITH maintenance_features AS (
            SELECT 
                v.vehicle_id,
                v.vehicle_type,
                v.model_year,
                v.current_mileage,
                v.fuel_efficiency_mpg,
                v.vehicle_age_years,
                v.vehicle_type_numeric,
                
                -- Telemetry features
                AVG(vt.engine_temperature_f) as avg_engine_temperature,
                AVG(vt.engine_rpm) as avg_engine_rpm,
                AVG(vt.fuel_level_pct) as avg_fuel_level,
                AVG(vt.brake_pressure_psi) as avg_brake_pressure,
                STDDEV(vt.engine_temperature_f) as engine_temp_volatility,
                STDDEV(vt.engine_rpm) as engine_rpm_volatility,
                
                -- Maintenance history
                COUNT(m.maintenance_id) as maintenance_count_12m,
                AVG(m.total_cost) as avg_maintenance_cost,
                MAX(m.maintenance_date) as last_maintenance_date,
                DATEDIFF('day', MAX(m.maintenance_date), CURRENT_DATE()) as days_since_maintenance,
                
                -- Performance metrics
                AVG(fs.route_efficiency_score) as avg_route_efficiency,
                AVG(fs.fuel_efficiency_mpg) as avg_fuel_efficiency,
                COUNT(fs.shipment_id) as shipment_count_12m,
                
                -- Target variable: maintenance needed in next 30 days
                CASE 
                    WHEN EXISTS (
                        SELECT 1 FROM tbl_fact_vehicle_maintenance vm 
                        WHERE vm.vehicle_id = v.vehicle_id 
                        AND vm.maintenance_date BETWEEN CURRENT_DATE() AND DATEADD('day', 30, CURRENT_DATE())
                    ) THEN 1
                    ELSE 0
                END as maintenance_needed_30d
                
            FROM tbl_dim_vehicle v
            LEFT JOIN tbl_fact_vehicle_telemetry vt ON v.vehicle_id = vt.vehicle_id
                AND vt.timestamp >= DATEADD('month', -12, CURRENT_TIMESTAMP())
            LEFT JOIN tbl_fact_vehicle_maintenance m ON v.vehicle_id = m.vehicle_id
                AND m.maintenance_date >= DATEADD('month', -12, CURRENT_DATE())
            LEFT JOIN tbl_fact_shipments fs ON v.vehicle_id = fs.vehicle_id
                AND fs.shipment_date >= DATEADD('month', -12, CURRENT_DATE())
            WHERE v.vehicle_status = 'ACTIVE'
            GROUP BY v.vehicle_id, v.vehicle_type, v.model_year, v.current_mileage, 
                     v.fuel_efficiency_mpg, v.vehicle_age_years, v.vehicle_type_numeric
        )
        SELECT * FROM maintenance_features
        WHERE maintenance_needed_30d IS NOT NULL
        """
        
        df = self.session.sql(query).to_pandas()
        return df
    
    def prepare_features(self, df):
        """Prepare features for model training"""
        feature_columns = [
            'vehicle_type_numeric',
            'model_year',
            'current_mileage',
            'fuel_efficiency_mpg',
            'vehicle_age_years',
            'avg_engine_temperature',
            'avg_engine_rpm',
            'avg_fuel_level',
            'avg_brake_pressure',
            'engine_temp_volatility',
            'engine_rpm_volatility',
            'maintenance_count_12m',
            'avg_maintenance_cost',
            'days_since_maintenance',
            'avg_route_efficiency',
            'avg_fuel_efficiency',
            'shipment_count_12m'
        ]
        
        X = df[feature_columns].fillna(0)
        y = df['maintenance_needed_30d']
        
        return X, y, feature_columns
    
    def train_models(self, X, y):
        """Train multiple models and select the best one"""
        models = {
            'random_forest': RandomForestClassifier(
                n_estimators=100,
                max_depth=10,
                random_state=42,
                n_jobs=-1,
                class_weight='balanced'
            ),
            'gradient_boosting': GradientBoostingClassifier(
                n_estimators=100,
                max_depth=6,
                learning_rate=0.1,
                random_state=42
            )
        }
        
        best_model = None
        best_score = -np.inf
        best_model_name = None
        
        for name, model in models.items():
            # Cross-validation with ROC AUC
            cv_scores = cross_val_score(model, X, y, cv=5, scoring='roc_auc')
            mean_score = cv_scores.mean()
            
            print(f"{name} - CV ROC AUC Score: {mean_score:.4f} (+/- {cv_scores.std() * 2:.4f})")
            
            if mean_score > best_score:
                best_score = mean_score
                best_model = model
                best_model_name = name
        
        # Train the best model on full dataset
        best_model.fit(X, y)
        
        return best_model, best_model_name, best_score
    
    def evaluate_model(self, model, X_test, y_test):
        """Evaluate model performance"""
        y_pred = model.predict(X_test)
        y_pred_proba = model.predict_proba(X_test)[:, 1]
        
        metrics = {
            'accuracy': model.score(X_test, y_test),
            'roc_auc': roc_auc_score(y_test, y_pred_proba),
            'classification_report': classification_report(y_test, y_pred, output_dict=True)
        }
        
        return metrics
    
    def deploy_to_snowflake(self, model, feature_columns, model_name):
        """Deploy model to Snowflake ML"""
        # Save model locally
        model_path = f"/tmp/{model_name}_maintenance_model.pkl"
        joblib.dump(model, model_path)
        
        # Create model in Snowflake
        create_model_sql = f"""
        CREATE OR REPLACE MODEL {model_name}_maintenance_model
        USING 'file://{model_path}'
        COMMENT = 'Predictive maintenance model trained on {datetime.now().strftime("%Y-%m-%d")}'
        """
        
        self.session.sql(create_model_sql).collect()
        
        # Create prediction function
        prediction_function_sql = f"""
        CREATE OR REPLACE FUNCTION predict_maintenance_risk(
            vehicle_type_numeric FLOAT,
            model_year FLOAT,
            current_mileage FLOAT,
            fuel_efficiency_mpg FLOAT,
            vehicle_age_years FLOAT,
            avg_engine_temperature FLOAT,
            avg_engine_rpm FLOAT,
            avg_fuel_level FLOAT,
            avg_brake_pressure FLOAT,
            engine_temp_volatility FLOAT,
            engine_rpm_volatility FLOAT,
            maintenance_count_12m FLOAT,
            avg_maintenance_cost FLOAT,
            days_since_maintenance FLOAT,
            avg_route_efficiency FLOAT,
            avg_fuel_efficiency FLOAT,
            shipment_count_12m FLOAT
        )
        RETURNS FLOAT
        LANGUAGE SQL
        AS
        $$
            SELECT PREDICT({model_name}_maintenance_model, 
                vehicle_type_numeric,
                model_year,
                current_mileage,
                fuel_efficiency_mpg,
                vehicle_age_years,
                avg_engine_temperature,
                avg_engine_rpm,
                avg_fuel_level,
                avg_brake_pressure,
                engine_temp_volatility,
                engine_rpm_volatility,
                maintenance_count_12m,
                avg_maintenance_cost,
                days_since_maintenance,
                avg_route_efficiency,
                avg_fuel_efficiency,
                shipment_count_12m
            )
        $$
        """
        
        self.session.sql(prediction_function_sql).collect()
        
        print(f"Maintenance model {model_name} deployed successfully to Snowflake")
    
    def log_model_metadata(self, model_name, metrics, feature_columns):
        """Log model metadata to tracking table"""
        metadata = {
            'model_name': model_name,
            'training_date': datetime.now().isoformat(),
            'metrics': metrics,
            'feature_columns': feature_columns,
            'model_version': '1.0.0',
            'status': 'ACTIVE'
        }
        
        # Insert into model registry
        insert_sql = f"""
        INSERT INTO tbl_ml_model_registry (
            model_name, model_type, training_date, metrics, 
            feature_columns, model_version, status
        ) VALUES (
            '{model_name}_maintenance',
            'predictive_maintenance',
            '{metadata['training_date']}',
            PARSE_JSON('{json.dumps(metrics)}'),
            PARSE_JSON('{json.dumps(feature_columns)}'),
            '{metadata['model_version']}',
            '{metadata['status']}'
        )
        """
        
        self.session.sql(insert_sql).collect()
    
    def train_and_deploy(self):
        """Main training and deployment pipeline"""
        print("Loading training data...")
        df = self.load_training_data()
        print(f"Loaded {len(df)} training samples")
        
        print("Preparing features...")
        X, y, feature_columns = self.prepare_features(df)
        
        print("Splitting data...")
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=0.2, random_state=42, stratify=y
        )
        
        print("Training models...")
        best_model, best_model_name, best_score = self.train_models(X_train, y_train)
        
        print("Evaluating model...")
        metrics = self.evaluate_model(best_model, X_test, y_test)
        
        print(f"Best model: {best_model_name}")
        print(f"Performance metrics: {metrics}")
        
        print("Deploying to Snowflake...")
        self.deploy_to_snowflake(best_model, feature_columns, best_model_name)
        
        print("Logging model metadata...")
        self.log_model_metadata(best_model_name, metrics, feature_columns)
        
        print("Maintenance model training pipeline completed successfully!")
        
        return {
            'model_name': best_model_name,
            'metrics': metrics,
            'feature_columns': feature_columns
        }

if __name__ == "__main__":
    trainer = PredictiveMaintenanceModelTrainer()
    result = trainer.train_and_deploy()
    print(f"Training completed: {result}")
