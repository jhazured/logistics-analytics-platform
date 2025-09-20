#!/usr/bin/env python3
"""
Sample Data Generator for Logistics Analytics Platform
====================================================
Generates realistic sample data for testing and development.

Usage:
    python3 sample_data_generator.py --count 1000
    python3 sample_data_generator.py --table customers --count 500
"""

import os
import sys
import random
import uuid
import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
import logging
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.append(str(project_root))

logger = logging.getLogger(__name__)

class SampleDataGenerator:
    """Generate realistic sample data for logistics analytics"""
    
    def __init__(self, data_loader):
        """Initialize with data loader instance"""
        self.loader = data_loader
        
        # Sample data templates
        self.customer_names = [
            "Acme Corp", "Global Logistics Inc", "FastTrack Shipping", "Reliable Transport",
            "Swift Delivery Co", "Premium Logistics", "Express Cargo", "Secure Shipping",
            "Rapid Transit", "Efficient Movers", "Quick Ship", "Trusted Transport",
            "Speedy Logistics", "Safe Cargo", "Direct Delivery", "Priority Shipping"
        ]
        
        self.cities = [
            "New York", "Los Angeles", "Chicago", "Houston", "Phoenix", "Philadelphia",
            "San Antonio", "San Diego", "Dallas", "San Jose", "Austin", "Jacksonville",
            "Fort Worth", "Columbus", "Charlotte", "San Francisco", "Indianapolis",
            "Seattle", "Denver", "Washington", "Boston", "El Paso", "Nashville",
            "Detroit", "Oklahoma City", "Portland", "Las Vegas", "Memphis", "Louisville"
        ]
        
        self.states = [
            "NY", "CA", "IL", "TX", "AZ", "PA", "TX", "CA", "TX", "CA", "TX", "FL",
            "TX", "OH", "NC", "CA", "IN", "WA", "CO", "DC", "MA", "TX", "TN",
            "MI", "OK", "OR", "NV", "TN", "KY"
        ]
        
        self.vehicle_types = [
            "Truck", "Van", "Trailer", "Container", "Flatbed", "Refrigerated",
            "Box Truck", "Semi-Trailer", "Pickup", "Cargo Van"
        ]
        
        self.vehicle_makes = [
            "Ford", "Chevrolet", "Freightliner", "Peterbilt", "Kenworth", "Volvo",
            "Mack", "International", "Isuzu", "Hino", "Mercedes-Benz", "Iveco"
        ]
        
        self.vehicle_models = [
            "F-150", "Silverado", "Cascadia", "579", "T680", "VNL", "Anthem",
            "LT", "NPR", "L6", "Sprinter", "Daily"
        ]
        
        self.weather_conditions = [
            "Clear", "Partly Cloudy", "Cloudy", "Rain", "Heavy Rain", "Snow",
            "Fog", "Windy", "Storm", "Hail"
        ]
        
        self.traffic_conditions = [
            "Light", "Moderate", "Heavy", "Severe", "Standstill", "Accident",
            "Construction", "Weather", "Event", "Rush Hour"
        ]
        
        self.shipment_statuses = [
            "Pending", "In Transit", "Delivered", "Delayed", "Cancelled", "Returned"
        ]
        
        self.maintenance_types = [
            "Oil Change", "Brake Inspection", "Tire Replacement", "Engine Service",
            "Transmission Service", "Electrical Check", "Safety Inspection",
            "Preventive Maintenance", "Emergency Repair", "Annual Service"
        ]
    
    def generate_customers(self, count: int) -> pd.DataFrame:
        """Generate sample customer data matching dbt model schema"""
        logger.info(f"Generating {count} customer records")
        
        data = []
        for i in range(count):
            customer_id = f"CUST_{i+1:06d}"
            company_name = random.choice(self.customer_names)
            if i > len(self.customer_names):
                company_name += f" {i//len(self.customer_names) + 1}"
            
            data.append({
                'customer_id': customer_id,
                'customer_name': company_name,
                'customer_type': random.choice(['BASIC', 'STANDARD', 'PREMIUM']),
                'industry_code': f"IND_{random.randint(1000, 9999)}",
                'credit_limit': random.randint(10000, 1000000),
                'payment_terms': random.choice(['NET_30', 'NET_15', 'NET_60', 'IMMEDIATE']),
                'customer_since': datetime.now() - timedelta(days=random.randint(30, 2000)),
                'status': 'ACTIVE',
                'billing_address': f"{random.randint(100, 9999)} Main St, {random.choice(self.cities)}, {random.choice(self.states)} {random.randint(10000, 99999)}",
                'shipping_address': f"{random.randint(100, 9999)} Business Ave, {random.choice(self.cities)}, {random.choice(self.states)} {random.randint(10000, 99999)}",
                'contact_email': f"contact{i+1}@{company_name.lower().replace(' ', '')}.com",
                'contact_phone': f"555-{random.randint(100, 999)}-{random.randint(1000, 9999)}",
                'account_manager': f"Manager_{random.randint(1, 20)}",
                'created_at': datetime.now() - timedelta(days=random.randint(1, 365)),
                'updated_at': datetime.now(),
                '_loaded_at': datetime.now()
            })
        
        return pd.DataFrame(data)
    
    def generate_vehicles(self, count: int) -> pd.DataFrame:
        """Generate sample vehicle data matching dbt model schema"""
        logger.info(f"Generating {count} vehicle records")
        
        data = []
        for i in range(count):
            vehicle_id = f"VEH_{i+1:06d}"
            make = random.choice(self.vehicle_makes)
            model = random.choice(self.vehicle_models)
            vehicle_type = random.choice(['TRUCK', 'VAN', 'MOTORCYCLE', 'CAR'])
            
            data.append({
                'vehicle_id': vehicle_id,
                'vehicle_number': f"V{random.randint(1000, 9999)}",
                'vehicle_type': vehicle_type,
                'make': make,
                'model': model,
                'model_year': random.randint(2015, 2024),
                'capacity_lbs': random.randint(1000, 80000),
                'capacity_cubic_feet': random.randint(50, 3000),
                'fuel_type': random.choice(['DIESEL', 'GASOLINE', 'ELECTRIC', 'HYBRID']),
                'fuel_efficiency_mpg': random.randint(8, 50),
                'maintenance_interval_miles': random.randint(5000, 15000),
                'current_mileage': random.randint(10000, 200000),
                'last_maintenance_date': datetime.now() - timedelta(days=random.randint(1, 90)),
                'next_maintenance_date': datetime.now() + timedelta(days=random.randint(30, 365)),
                'vehicle_status': random.choice(['ACTIVE', 'MAINTENANCE']),
                'assigned_driver_id': f"DRV_{random.randint(1, 50):03d}",
                'insurance_expiry': datetime.now() + timedelta(days=random.randint(30, 365)),
                'registration_expiry': datetime.now() + timedelta(days=random.randint(30, 365)),
                'purchase_date': datetime.now() - timedelta(days=random.randint(30, 2000)),
                'purchase_price': random.randint(25000, 150000),
                'current_value': random.randint(15000, 100000),
                'created_at': datetime.now() - timedelta(days=random.randint(1, 365)),
                'updated_at': datetime.now(),
                '_loaded_at': datetime.now()
            })
        
        return pd.DataFrame(data)
    
    def generate_routes(self, count: int) -> pd.DataFrame:
        """Generate sample route data"""
        logger.info(f"Generating {count} route records")
        
        data = []
        for i in range(count):
            route_id = f"ROUTE_{i+1:06d}"
            origin_city = random.choice(self.cities)
            destination_city = random.choice([city for city in self.cities if city != origin_city])
            
            data.append({
                'route_id': route_id,
                'origin_city': origin_city,
                'origin_state': random.choice(self.states),
                'destination_city': destination_city,
                'destination_state': random.choice(self.states),
                'distance_km': random.randint(50, 3000),
                'estimated_duration_hours': random.randint(2, 48),
                'route_type': random.choice(['Highway', 'City', 'Mixed', 'Rural']),
                'toll_required': random.choice([True, False]),
                'hazardous_materials_allowed': random.choice([True, False]),
                'created_date': datetime.now() - timedelta(days=random.randint(1, 365)),
                'last_updated': datetime.now()
            })
        
        return pd.DataFrame(data)
    
    def generate_shipments(self, count: int) -> pd.DataFrame:
        """Generate sample shipment data matching dbt model schema"""
        logger.info(f"Generating {count} shipment records")
        
        # Get existing data for foreign keys
        try:
            self.loader.cursor.execute("SELECT customer_id FROM RAW.CUSTOMERS LIMIT 100")
            customer_ids = [row[0] for row in self.loader.cursor.fetchall()]
            
            self.loader.cursor.execute("SELECT vehicle_id FROM RAW.VEHICLES LIMIT 100")
            vehicle_ids = [row[0] for row in self.loader.cursor.fetchall()]
        except:
            # Fallback to generated IDs if tables don't exist
            customer_ids = [f"CUST_{i+1:06d}" for i in range(100)]
            vehicle_ids = [f"VEH_{i+1:06d}" for i in range(100)]
        
        data = []
        for i in range(count):
            shipment_id = f"SHIP_{i+1:06d}"
            pickup_date = datetime.now() - timedelta(days=random.randint(1, 30))
            delivery_date = pickup_date + timedelta(days=random.randint(1, 7))
            requested_delivery = pickup_date + timedelta(days=random.randint(1, 5))
            actual_delivery = delivery_date + timedelta(hours=random.randint(-12, 12))
            
            data.append({
                'shipment_id': shipment_id,
                'customer_id': random.choice(customer_ids),
                'vehicle_id': random.choice(vehicle_ids),
                'driver_id': f"DRV_{random.randint(1, 50):03d}",
                'origin_location_id': f"LOC_{random.randint(1, 100):03d}",
                'destination_location_id': f"LOC_{random.randint(1, 100):03d}",
                'pickup_date': pickup_date,
                'delivery_date': delivery_date,
                'requested_delivery_date': requested_delivery,
                'actual_delivery_date': actual_delivery,
                'shipment_status': random.choice(['PENDING', 'IN_TRANSIT', 'DELIVERED', 'CANCELLED', 'DELAYED']),
                'weight_lbs': random.randint(10, 80000),
                'volume_cubic_feet': random.uniform(1, 3000),
                'shipment_value': random.randint(100, 1000000),
                'fuel_cost': random.randint(50, 500),
                'driver_cost': random.randint(100, 1000),
                'total_cost': random.randint(200, 2000),
                'revenue': random.randint(300, 3000),
                'distance_miles': random.randint(10, 3000),
                'delivery_time_hours': random.randint(2, 168),
                'on_time_delivery': actual_delivery <= requested_delivery,
                'weather_conditions': random.choice(self.weather_conditions),
                'traffic_conditions': random.choice(self.traffic_conditions),
                'special_instructions': random.choice(['None', 'Fragile', 'Temperature Controlled', 'Hazardous']),
                'created_at': datetime.now() - timedelta(days=random.randint(1, 365)),
                'updated_at': datetime.now(),
                '_loaded_at': datetime.now()
            })
        
        return pd.DataFrame(data)
    
    def generate_weather_data(self, count: int) -> pd.DataFrame:
        """Generate sample weather data matching dbt model schema"""
        logger.info(f"Generating {count} weather records")
        
        data = []
        for i in range(count):
            date = datetime.now() - timedelta(days=random.randint(1, 30))
            hour = random.randint(0, 23)
            
            data.append({
                'weather_id': f"WEATHER_{i+1:06d}",
                'location_id': f"LOC_{random.randint(1, 100):03d}",
                'date': date.date(),
                'hour': hour,
                'temperature_f': random.randint(-20, 120),
                'temperature_c': random.randint(-30, 50),
                'humidity_pct': random.randint(20, 100),
                'wind_speed_mph': random.randint(0, 100),
                'wind_direction_degrees': random.randint(0, 360),
                'precipitation_mm': random.uniform(0, 50),
                'visibility_miles': random.uniform(0.1, 20),
                'weather_condition': random.choice(self.weather_conditions),
                'weather_description': f"{random.choice(self.weather_conditions)} conditions",
                'pressure_inhg': random.uniform(28, 32),
                'uv_index': random.randint(0, 11),
                'sunrise_time': f"{random.randint(5, 8):02d}:{random.randint(0, 59):02d}",
                'sunset_time': f"{random.randint(17, 20):02d}:{random.randint(0, 59):02d}",
                'created_at': datetime.now(),
                '_loaded_at': datetime.now()
            })
        
        return pd.DataFrame(data)
    
    def generate_traffic_data(self, count: int) -> pd.DataFrame:
        """Generate sample traffic data matching dbt model schema"""
        logger.info(f"Generating {count} traffic records")
        
        data = []
        for i in range(count):
            date = datetime.now() - timedelta(days=random.randint(1, 30))
            hour = random.randint(0, 23)
            
            data.append({
                'traffic_id': f"TRAFFIC_{i+1:06d}",
                'location_id': f"LOC_{random.randint(1, 100):03d}",
                'date': date.date(),
                'hour': hour,
                'traffic_level': random.choice(['LOW', 'MODERATE', 'HIGH', 'SEVERE']),
                'congestion_delay_minutes': random.randint(0, 120),
                'average_speed_mph': random.randint(10, 80),
                'free_flow_speed_mph': random.randint(50, 80),
                'travel_time_minutes': random.randint(10, 180),
                'free_flow_travel_time_minutes': random.randint(5, 60),
                'confidence_score': random.uniform(0.5, 1.0),
                'road_type': random.choice(['HIGHWAY', 'STREET', 'TOLL_ROAD', 'BRIDGE']),
                'incident_count': random.randint(0, 5),
                'weather_impact': random.choice(['NONE', 'LIGHT', 'MODERATE', 'SEVERE']),
                'created_at': datetime.now(),
                '_loaded_at': datetime.now()
            })
        
        return pd.DataFrame(data)
    
    def generate_maintenance_data(self, count: int) -> pd.DataFrame:
        """Generate sample maintenance data matching dbt model schema"""
        logger.info(f"Generating {count} maintenance records")
        
        # Get existing vehicle IDs
        try:
            self.loader.cursor.execute("SELECT vehicle_id FROM RAW.VEHICLES LIMIT 100")
            vehicle_ids = [row[0] for row in self.loader.cursor.fetchall()]
        except:
            vehicle_ids = [f"VEH_{i+1:06d}" for i in range(100)]
        
        data = []
        for i in range(count):
            maintenance_id = f"MAINT_{i+1:06d}"
            service_date = datetime.now() - timedelta(days=random.randint(1, 365))
            maintenance_type = random.choice(['ROUTINE', 'REPAIR', 'INSPECTION', 'EMERGENCY'])
            
            data.append({
                'maintenance_id': maintenance_id,
                'vehicle_id': random.choice(vehicle_ids),
                'maintenance_type': maintenance_type,
                'maintenance_date': service_date,
                'odometer_reading': random.randint(10000, 200000),
                'description': f"Service for {maintenance_type}",
                'parts_cost': random.randint(20, 1000),
                'labor_cost': random.randint(30, 1500),
                'total_cost': random.randint(50, 2000),
                'maintenance_provider': f"Provider_{random.randint(1, 10)}",
                'next_maintenance_due_date': service_date + timedelta(days=random.randint(30, 365)),
                'next_maintenance_due_mileage': random.randint(10000, 200000),
                'maintenance_status': 'COMPLETED',
                'created_at': datetime.now(),
                'updated_at': datetime.now(),
                '_loaded_at': datetime.now()
            })
        
        return pd.DataFrame(data)
    
    def generate_telematics_data(self, count: int) -> pd.DataFrame:
        """Generate sample telematics data matching dbt model schema"""
        logger.info(f"Generating {count} telematics records")
        
        # Get existing vehicle IDs
        try:
            self.loader.cursor.execute("SELECT vehicle_id FROM RAW.VEHICLES LIMIT 100")
            vehicle_ids = [row[0] for row in self.loader.cursor.fetchall()]
        except:
            vehicle_ids = [f"VEH_{i+1:06d}" for i in range(100)]
        
        data = []
        for i in range(count):
            timestamp = datetime.now() - timedelta(hours=random.randint(1, 720))  # Last 30 days
            
            data.append({
                'telemetry_id': f"TELEM_{i+1:06d}",
                'vehicle_id': random.choice(vehicle_ids),
                'timestamp': timestamp,
                'speed_mph': random.randint(0, 120),
                'engine_rpm': random.randint(600, 4000),
                'fuel_level_pct': random.randint(10, 100),
                'engine_temperature_f': random.randint(160, 220),
                'brake_pressure_psi': random.randint(0, 2000),
                'tire_pressure_psi': random.randint(25, 45),
                'latitude': random.uniform(25.0, 49.0),  # US coordinates
                'longitude': random.uniform(-125.0, -66.0),
                'altitude_ft': random.randint(0, 10000),
                'heading_degrees': random.randint(0, 360),
                'acceleration_g': random.uniform(-2.0, 2.0),
                'created_at': datetime.now(),
                '_loaded_at': datetime.now()
            })
        
        return pd.DataFrame(data)
    
    def generate_all_sample_data(self, count: int = 1000, specific_table: Optional[str] = None):
        """Generate sample data for all tables or specific table"""
        
        if specific_table:
            # Generate data for specific table
            if specific_table.lower() == 'customers':
                df = self.generate_customers(count)
                self.loader.load_dataframe(df, 'RAW.CUSTOMERS')
            elif specific_table.lower() == 'vehicles':
                df = self.generate_vehicles(count)
                self.loader.load_dataframe(df, 'RAW.VEHICLES')
            elif specific_table.lower() == 'routes':
                df = self.generate_routes(count)
                self.loader.load_dataframe(df, 'RAW.ROUTES')
            elif specific_table.lower() == 'shipments':
                df = self.generate_shipments(count)
                self.loader.load_dataframe(df, 'RAW.SHIPMENTS')
            elif specific_table.lower() == 'weather':
                df = self.generate_weather_data(count)
                self.loader.load_dataframe(df, 'RAW.WEATHER')
            elif specific_table.lower() == 'traffic':
                df = self.generate_traffic_data(count)
                self.loader.load_dataframe(df, 'RAW.TRAFFIC')
            elif specific_table.lower() == 'maintenance':
                df = self.generate_maintenance_data(count)
                self.loader.load_dataframe(df, 'RAW.MAINTENANCE')
            elif specific_table.lower() == 'telematics':
                df = self.generate_telematics_data(count)
                self.loader.load_dataframe(df, 'RAW.TELEMATICS')
            else:
                logger.error(f"Unknown table: {specific_table}")
                return
        else:
            # Generate data for all tables
            logger.info("Generating sample data for all tables")
            
            # Generate in dependency order
            tables_data = [
                ('customers', self.generate_customers(count)),
                ('vehicles', self.generate_vehicles(count)),
                ('shipments', self.generate_shipments(count)),
                ('weather', self.generate_weather_data(count)),
                ('traffic', self.generate_traffic_data(count)),
                ('maintenance', self.generate_maintenance_data(count)),
                ('telematics', self.generate_telematics_data(count))
            ]
            
            for table_name, df in tables_data:
                try:
                    success = self.loader.load_dataframe(df, f'RAW.{table_name.upper()}')
                    if success:
                        logger.info(f"Successfully generated {len(df)} records for {table_name}")
                    else:
                        logger.error(f"Failed to generate data for {table_name}")
                except Exception as e:
                    logger.error(f"Error generating data for {table_name}: {e}")
        
        logger.info("Sample data generation completed")

def main():
    """Main function for command line interface"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Sample Data Generator')
    parser.add_argument('--count', type=int, default=1000, help='Number of records to generate')
    parser.add_argument('--table', help='Specific table to generate data for')
    
    args = parser.parse_args()
    
    # Import data loader
    from data_loader import SnowflakeDataLoader
    
    # Initialize
    loader = SnowflakeDataLoader()
    generator = SampleDataGenerator(loader)
    
    try:
        loader.connect()
        generator.generate_all_sample_data(args.count, args.table)
    except Exception as e:
        logger.error(f"Error: {e}")
        sys.exit(1)
    finally:
        loader.disconnect()

if __name__ == '__main__':
    main()
