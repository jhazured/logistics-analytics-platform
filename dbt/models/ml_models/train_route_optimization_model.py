#!/usr/bin/env python3
"""
Route Optimization ML Model Training
Trains and deploys ML models for route optimization using Snowflake ML
"""

import pandas as pd
import numpy as np
from sklearn.ensemble import RandomForestRegressor, GradientBoostingRegressor
from sklearn.model_selection import train_test_split, cross_val_score
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
import joblib
import json
from datetime import datetime
import snowflake.connector
from snowflake.snowpark import Session
import os

class RouteOptimizationModelTrainer:
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
        """Load features from the consolidated feature store"""
        query = """
        SELECT 
            route_id,
            route_efficiency_score,
            traffic_delay_factor,
            weather_impact_score,
            route_complexity_score,
            distance_km,
            estimated_duration_minutes,
            actual_duration_minutes,
            fuel_efficiency_mpg,
            cost_per_km,
            on_time_delivery_rate,
            customer_satisfaction_score,
            -- Target variable: optimized delivery time
            CASE 
                WHEN actual_duration_minutes <= estimated_duration_minutes * 0.9 THEN estimated_duration_minutes * 0.9
                WHEN actual_duration_minutes <= estimated_duration_minutes * 1.1 THEN actual_duration_minutes
                ELSE estimated_duration_minutes * 1.1
            END as optimized_delivery_time_minutes
        FROM tbl_ml_consolidated_feature_store
        WHERE feature_date >= DATEADD('month', -12, CURRENT_DATE())
        AND route_efficiency_score IS NOT NULL
        AND traffic_delay_factor IS NOT NULL
        AND weather_impact_score IS NOT NULL
        """
        
        df = self.session.sql(query).to_pandas()
        return df
    
    def prepare_features(self, df):
        """Prepare features for model training"""
        feature_columns = [
            'route_efficiency_score',
            'traffic_delay_factor', 
            'weather_impact_score',
            'route_complexity_score',
            'distance_km',
            'fuel_efficiency_mpg',
            'cost_per_km',
            'on_time_delivery_rate',
            'customer_satisfaction_score'
        ]
        
        X = df[feature_columns].fillna(0)
        y = df['optimized_delivery_time_minutes']
        
        return X, y, feature_columns
    
    def train_models(self, X, y):
        """Train multiple models and select the best one"""
        models = {
            'random_forest': RandomForestRegressor(
                n_estimators=100,
                max_depth=10,
                random_state=42,
                n_jobs=-1
            ),
            'gradient_boosting': GradientBoostingRegressor(
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
            # Cross-validation
            cv_scores = cross_val_score(model, X, y, cv=5, scoring='r2')
            mean_score = cv_scores.mean()
            
            print(f"{name} - CV R2 Score: {mean_score:.4f} (+/- {cv_scores.std() * 2:.4f})")
            
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
        
        metrics = {
            'mae': mean_absolute_error(y_test, y_pred),
            'mse': mean_squared_error(y_test, y_pred),
            'rmse': np.sqrt(mean_squared_error(y_test, y_pred)),
            'r2': r2_score(y_test, y_pred)
        }
        
        return metrics
    
    def deploy_to_snowflake(self, model, feature_columns, model_name):
        """Deploy model to Snowflake ML"""
        # Save model locally
        model_path = f"/tmp/{model_name}_model.pkl"
        joblib.dump(model, model_path)
        
        # Create model in Snowflake
        create_model_sql = f"""
        CREATE OR REPLACE MODEL {model_name}_model
        USING 'file://{model_path}'
        COMMENT = 'Route optimization model trained on {datetime.now().strftime("%Y-%m-%d")}'
        """
        
        self.session.sql(create_model_sql).collect()
        
        # Create prediction function
        prediction_function_sql = f"""
        CREATE OR REPLACE FUNCTION predict_route_optimization(
            route_efficiency_score FLOAT,
            traffic_delay_factor FLOAT,
            weather_impact_score FLOAT,
            route_complexity_score FLOAT,
            distance_km FLOAT,
            fuel_efficiency_mpg FLOAT,
            cost_per_km FLOAT,
            on_time_delivery_rate FLOAT,
            customer_satisfaction_score FLOAT
        )
        RETURNS FLOAT
        LANGUAGE SQL
        AS
        $$
            SELECT PREDICT({model_name}_model, 
                route_efficiency_score,
                traffic_delay_factor,
                weather_impact_score,
                route_complexity_score,
                distance_km,
                fuel_efficiency_mpg,
                cost_per_km,
                on_time_delivery_rate,
                customer_satisfaction_score
            )
        $$
        """
        
        self.session.sql(prediction_function_sql).collect()
        
        print(f"Model {model_name} deployed successfully to Snowflake")
    
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
            '{model_name}',
            'route_optimization',
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
            X, y, test_size=0.2, random_state=42
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
        
        print("Training pipeline completed successfully!")
        
        return {
            'model_name': best_model_name,
            'metrics': metrics,
            'feature_columns': feature_columns
        }

if __name__ == "__main__":
    trainer = RouteOptimizationModelTrainer()
    result = trainer.train_and_deploy()
    print(f"Training completed: {result}")
