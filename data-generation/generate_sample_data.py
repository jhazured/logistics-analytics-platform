#!/usr/bin/env python3
"""
Smart Logistics Analytics Platform - Sample Data Generator
Generates realistic sample datasets for the comprehensive logistics analytics project
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
from faker import Faker
import uuid
import json
import os

# Set random seeds for reproducibility
np.random.seed(42)
random.seed(42)
fake = Faker('en_AU')  # Australian locale
Faker.seed(42)

class LogisticsDataGenerator:
    def __init__(self):
        self.start_date = datetime(2023, 1, 1)
        self.end_date = datetime(2025, 9, 19)  # Current date
        
        # Australian locations
        self.major_cities = [
            {'city': 'Sydney', 'state': 'NSW', 'lat': -33.8688, 'lng': 151.2093},
            {'city': 'Melbourne', 'state': 'VIC', 'lat': -37.8136, 'lng': 144.9631},
            {'city': 'Brisbane', 'state': 'QLD', 'lat': -27.4698, 'lng': 153.0251},
            {'city': 'Perth', 'state': 'WA', 'lat': -31.9505, 'lng': 115.8605},
            {'city': 'Adelaide', 'state': 'SA', 'lat': -34.9285, 'lng': 138.6007},
            {'city': 'Gold Coast', 'state': 'QLD', 'lat': -28.0167, 'lng': 153.4000},
            {'city': 'Newcastle', 'state': 'NSW', 'lat': -32.9283, 'lng': 151.7817},
            {'city': 'Canberra', 'state': 'ACT', 'lat': -35.2809, 'lng': 149.1300},
            {'city': 'Sunshine Coast', 'state': 'QLD', 'lat': -26.6500, 'lng': 153.0667},
            {'city': 'Wollongong', 'state': 'NSW', 'lat': -34.4278, 'lng': 150.8931}
        ]
        
        self.weather_conditions = ['Clear', 'Partly Cloudy', 'Cloudy', 'Light Rain', 'Heavy Rain', 'Storm', 'Fog']
        self.traffic_conditions = ['Light', 'Moderate', 'Heavy', 'Severe']
        self.vehicle_types = ['Van', 'Small Truck', 'Medium Truck', 'Large Truck', 'Semi-Trailer']
        self.route_types = ['Urban', 'Suburban', 'Highway', 'Rural', 'Mixed']
        
    def generate_date_dimension(self):
        """Generate comprehensive date dimension"""
        dates = pd.date_range(start=self.start_date, end=self.end_date, freq='D')
        
        date_data = []
        for date in dates:
            date_data.append({
                'date_key': int(date.strftime('%Y%m%d')),
                'date': date.date(),
                'year': date.year,
                'quarter': f'Q{date.quarter}',
                'month': date.month,
                'month_name': date.strftime('%B'),
                'week_of_year': date.isocalendar()[1],
                'day_of_year': date.dayofyear,
                'day_of_month': date.day,
                'day_of_week': date.dayofweek + 1,
                'day_name': date.strftime('%A'),
                'is_weekend': date.dayofweek >= 5,
                'is_business_day': date.dayofweek < 5,
                'is_holiday': self._is_australian_holiday(date),
                'season': self._get_season(date),
                'logistics_day_type': self._get_logistics_day_type(date)
            })
        
        return pd.DataFrame(date_data)
    
    def generate_location_dimension(self):
        """Generate location hierarchy"""
        locations = []
        location_id = 1
        
        # Major distribution centers
        for city_info in self.major_cities:
            # Main depot
            locations.append({
                'location_id': location_id,
                'location_name': f"{city_info['city']} Main Depot",
                'location_type': 'Depot',
                'city': city_info['city'],
                'state': city_info['state'],
                'postcode': fake.postcode(),
                'latitude': city_info['lat'],
                'longitude': city_info['lng'],
                'capacity_rating': random.choice(['Small', 'Medium', 'Large']),
                'operating_hours': '24/7' if random.random() > 0.3 else '6AM-10PM',
                'created_date': fake.date_between(start_date='-2y', end_date='today')
            })
            location_id += 1
            
            # Branch offices (2-4 per major city)
            num_branches = random.randint(2, 4)
            for i in range(num_branches):
                locations.append({
                    'location_id': location_id,
                    'location_name': f"{city_info['city']} Branch {i+1}",
                    'location_type': 'Branch',
                    'city': city_info['city'],
                    'state': city_info['state'],
                    'postcode': fake.postcode(),
                    'latitude': city_info['lat'] + random.uniform(-0.1, 0.1),
                    'longitude': city_info['lng'] + random.uniform(-0.1, 0.1),
                    'capacity_rating': random.choice(['Small', 'Medium']),
                    'operating_hours': random.choice(['6AM-6PM', '7AM-7PM', '8AM-8PM']),
                    'created_date': fake.date_between(start_date='-2y', end_date='today')
                })
                location_id += 1
        
        # Delivery points (more distributed)
        for _ in range(200):
            city_info = random.choice(self.major_cities)
            locations.append({
                'location_id': location_id,
                'location_name': f"Delivery Point {location_id}",
                'location_type': 'Delivery Point',
                'city': city_info['city'],
                'state': city_info['state'],
                'postcode': fake.postcode(),
                'latitude': city_info['lat'] + random.uniform(-0.5, 0.5),
                'longitude': city_info['lng'] + random.uniform(-0.5, 0.5),
                'capacity_rating': 'Small',
                'operating_hours': random.choice(['9AM-5PM', '8AM-6PM', '24/7']),
                'created_date': fake.date_between(start_date='-1y', end_date='today')
            })
            location_id += 1
        
        return pd.DataFrame(locations)
    
    def generate_customer_dimension(self):
        """Generate customer master data with segments"""
        customers = []
        
        for i in range(1000):
            signup_date = fake.date_between(start_date='-3y', end_date='today')
            volume_segment = random.choices(
                ['High Volume', 'Medium Volume', 'Low Volume'],
                weights=[0.1, 0.3, 0.6]
            )[0]
            
            customers.append({
                'customer_id': i + 1,
                'customer_name': fake.company(),
                'customer_type': random.choice(['Enterprise', 'SME', 'Individual']),
                'volume_segment': volume_segment,
                'industry': random.choice(['Retail', 'Manufacturing', 'Healthcare', 'Technology', 'Food & Beverage', 'Automotive']),
                'preferred_delivery_window': random.choice(['Morning', 'Afternoon', 'Evening', 'Any']),
                'service_level': random.choice(['Standard', 'Express', 'Premium']),
                'credit_rating': random.choice(['Excellent', 'Good', 'Fair', 'Poor']),
                'payment_terms': random.choice(['Net 30', 'Net 15', 'COD', 'Prepaid']),
                'signup_date': signup_date,
                'last_order_date': fake.date_between(start_date=signup_date, end_date='today'),
                'total_lifetime_value': round(random.uniform(1000, 500000), 2),
                'average_order_value': round(random.uniform(50, 5000), 2),
                'delivery_flexibility_score': round(random.uniform(1, 10), 1),
                'satisfaction_score': round(random.uniform(6, 10), 1)
            })
        
        return pd.DataFrame(customers)
    
    def generate_vehicle_dimension(self):
        """Generate vehicle fleet master data"""
        vehicles = []
        
        for i in range(200):
            vehicle_type = random.choice(self.vehicle_types)
            manufacture_year = random.randint(2015, 2024)
            purchase_date = fake.date_between(start_date=f'-{2024-manufacture_year+1}y', end_date='today')
            
            # Capacity based on vehicle type
            capacity_mapping = {
                'Van': (1000, 3000),
                'Small Truck': (3000, 8000),
                'Medium Truck': (8000, 15000),
                'Large Truck': (15000, 25000),
                'Semi-Trailer': (25000, 40000)
            }
            
            capacity = random.randint(*capacity_mapping[vehicle_type])
            
            vehicles.append({
                'vehicle_id': f'VH{str(i+1).zfill(4)}',
                'vehicle_type': vehicle_type,
                'make': random.choice(['Isuzu', 'Mercedes', 'Volvo', 'MAN', 'Scania', 'Ford', 'Iveco']),
                'model': f'Model {random.randint(100, 999)}',
                'year': manufacture_year,
                'capacity_kg': capacity,
                'fuel_type': random.choice(['Diesel', 'Petrol', 'Electric', 'Hybrid']),
                'fuel_efficiency_l_100km': round(random.uniform(8, 25), 1),
                'purchase_date': purchase_date,
                'last_service_date': fake.date_between(start_date='-90d', end_date='today'),
                'next_service_due': fake.date_between(start_date='today', end_date='+90d'),
                'odometer_km': random.randint(50000, 500000),
                'condition_score': round(random.uniform(6, 10), 1),
                'maintenance_cost_ytd': round(random.uniform(2000, 15000), 2),
                'is_active': random.random() > 0.05,
                'gps_enabled': True,
                'telematics_enabled': random.random() > 0.1
            })
        
        return pd.DataFrame(vehicles)
    
    def generate_route_dimension(self):
        """Generate route definitions"""
        routes = []
        locations_df = self.generate_location_dimension()  # Get locations for routing
        depots = locations_df[locations_df['location_type'] == 'Depot']
        delivery_points = locations_df[locations_df['location_type'] == 'Delivery Point']
        
        route_id = 1
        for _, depot in depots.iterrows():
            # Create routes from each depot to various delivery points
            num_routes = random.randint(10, 20)
            
            for i in range(num_routes):
                # Select random delivery points for this route
                route_points = delivery_points.sample(n=random.randint(3, 8))
                
                total_distance = sum([random.uniform(5, 50) for _ in range(len(route_points))])
                estimated_time = total_distance * random.uniform(1.2, 2.5)  # minutes per km
                
                routes.append({
                    'route_id': route_id,
                    'route_name': f"Route {depot['city']}-{route_id}",
                    'origin_location_id': depot['location_id'],
                    'route_type': random.choice(self.route_types),
                    'total_distance_km': round(total_distance, 1),
                    'estimated_duration_minutes': round(estimated_time, 0),
                    'number_of_stops': len(route_points),
                    'complexity_score': round(random.uniform(1, 10), 1),
                    'traffic_density': random.choice(['Low', 'Medium', 'High']),
                    'road_quality': random.choice(['Excellent', 'Good', 'Fair', 'Poor']),
                    'weather_risk': random.choice(['Low', 'Medium', 'High']),
                    'is_active': random.random() > 0.1,
                    'created_date': fake.date_between(start_date='-1y', end_date='today')
                })
                route_id += 1
        
        return pd.DataFrame(routes)
    
    def generate_weather_dimension(self):
        """Generate weather conditions data"""
        weather_data = []
        
        # Generate weather data for each major city and date combination
        dates = pd.date_range(start=self.start_date, end=self.end_date, freq='D')
        weather_id = 1
        
        for date in dates[-365:]:  # Last year of data
            for city_info in self.major_cities:
                # Seasonal weather patterns for Australia
                season = self._get_season(date)
                temp_ranges = {
                    'Summer': (20, 40),
                    'Autumn': (15, 30),
                    'Winter': (5, 20),
                    'Spring': (10, 25)
                }
                
                temp_range = temp_ranges[season]
                temperature = random.uniform(*temp_range)
                
                weather_data.append({
                    'weather_id': weather_id,
                    'date': date.date(),
                    'city': city_info['city'],
                    'condition': random.choice(self.weather_conditions),
                    'temperature_c': round(temperature, 1),
                    'humidity_percent': random.randint(30, 95),
                    'wind_speed_kmh': round(random.uniform(0, 50), 1),
                    'precipitation_mm': round(random.uniform(0, 50) if random.random() > 0.7 else 0, 1),
                    'visibility_km': round(random.uniform(5, 50), 1),
                    'weather_severity_score': round(random.uniform(1, 10), 1),
                    'driving_impact_score': round(random.uniform(1, 10), 1)
                })
                weather_id += 1
        
        return pd.DataFrame(weather_data)
    
    def generate_fact_shipments(self, customers_df, locations_df, vehicles_df, routes_df):
        """Generate shipment fact table"""
        shipments = []
        
        # Generate shipments for the last 2 years
        dates = pd.date_range(start=self.start_date + timedelta(days=365), end=self.end_date, freq='D')
        shipment_id = 1
        
        for date in dates:
            # More shipments on weekdays
            daily_volume = random.randint(50, 200) if date.weekday() < 5 else random.randint(20, 80)
            
            for _ in range(daily_volume):
                customer = customers_df.sample(1).iloc[0]
                vehicle = vehicles_df[vehicles_df['is_active']].sample(1).iloc[0]
                route = routes_df[routes_df['is_active']].sample(1).iloc[0]
                
                # Delivery performance based on various factors
                base_time = route['estimated_duration_minutes']
                actual_time = base_time * random.uniform(0.8, 1.5)  # Variation
                
                # On-time delivery probability
                on_time_prob = 0.85
                if date.weekday() >= 5:  # Weekend
                    on_time_prob *= 0.9
                if random.random() > 0.8:  # Weather impact
                    on_time_prob *= 0.7
                
                is_on_time = random.random() < on_time_prob
                
                shipments.append({
                    'shipment_id': shipment_id,
                    'date_key': int(date.strftime('%Y%m%d')),
                    'customer_id': customer['customer_id'],
                    'origin_location_id': route['origin_location_id'],
                    'destination_location_id': random.choice(locations_df[locations_df['location_type'] == 'Delivery Point']['location_id'].tolist()),
                    'vehicle_id': vehicle['vehicle_id'],
                    'route_id': route['route_id'],
                    'shipment_date': date.date(),
                    'planned_delivery_date': (date + timedelta(days=random.randint(0, 3))).date(),
                    'actual_delivery_date': (date + timedelta(days=random.randint(0, 5))).date() if is_on_time else None,
                    'weight_kg': round(random.uniform(10, vehicle['capacity_kg'] * 0.8), 1),
                    'volume_m3': round(random.uniform(0.1, 20), 2),
                    'distance_km': route['total_distance_km'],
                    'planned_duration_minutes': base_time,
                    'actual_duration_minutes': round(actual_time, 0) if is_on_time else None,
                    'fuel_cost': round(route['total_distance_km'] * vehicle['fuel_efficiency_l_100km'] * 1.6 / 100, 2),
                    'delivery_cost': round(random.uniform(50, 300), 2),
                    'revenue': round(random.uniform(100, 800), 2),
                    'is_on_time': is_on_time,
                    'is_delivered': is_on_time,
                    'customer_rating': random.randint(7, 10) if is_on_time else random.randint(3, 8),
                    'delivery_status': 'Delivered' if is_on_time else random.choice(['In Transit', 'Delayed', 'Failed']),
                    'priority_level': random.choice(['Standard', 'High', 'Urgent']),
                    'service_type': customer['service_level']
                })
                shipment_id += 1
        
        return pd.DataFrame(shipments)
    
    def generate_fact_vehicle_telemetry(self, vehicles_df):
        """Generate vehicle telemetry data"""
        telemetry_data = []
        
        # Generate telemetry for last 90 days
        dates = pd.date_range(start=self.end_date - timedelta(days=90), end=self.end_date, freq='H')
        
        for vehicle in vehicles_df[vehicles_df['telematics_enabled']].itertuples():
            for date in dates[::random.randint(1, 4)]:  # Sample some hours
                telemetry_data.append({
                    'telemetry_id': len(telemetry_data) + 1,
                    'vehicle_id': vehicle.vehicle_id,
                    'timestamp': date,
                    'latitude': random.uniform(-45, -10),  # Australia bounds
                    'longitude': random.uniform(110, 160),
                    'speed_kmh': round(random.uniform(0, 110), 1),
                    'fuel_level_percent': random.randint(10, 100),
                    'engine_rpm': random.randint(800, 4000),
                    'engine_temp_c': round(random.uniform(80, 110), 1),
                    'odometer_km': vehicle.odometer_km + random.randint(0, 500),
                    'fuel_consumption_lph': round(random.uniform(5, 25), 2),
                    'harsh_braking_events': random.randint(0, 3),
                    'harsh_acceleration_events': random.randint(0, 2),
                    'speeding_events': random.randint(0, 1),
                    'idle_time_minutes': random.randint(0, 30),
                    'diagnostic_codes': json.dumps([f'P{random.randint(1000, 9999)}' for _ in range(random.randint(0, 2))]),
                    'engine_health_score': round(random.uniform(7, 10), 1),
                    'maintenance_alert': random.random() > 0.95
                })
        
        return pd.DataFrame(telemetry_data)
    
    def _is_australian_holiday(self, date):
        """Simple Australian holiday detection"""
        holidays = [
            (1, 1),   # New Year's Day
            (1, 26),  # Australia Day
            (4, 25),  # ANZAC Day
            (12, 25), # Christmas Day
            (12, 26), # Boxing Day
        ]
        return (date.month, date.day) in holidays
    
    def _get_season(self, date):
        """Get Australian season"""
        month = date.month
        if month in [12, 1, 2]:
            return 'Summer'
        elif month in [3, 4, 5]:
            return 'Autumn'
        elif month in [6, 7, 8]:
            return 'Winter'
        else:
            return 'Spring'
    
    def _get_logistics_day_type(self, date):
        """Classify day type for logistics operations"""
        if self._is_australian_holiday(date):
            return 'Holiday'
        elif date.weekday() >= 5:
            return 'Weekend'
        elif date.weekday() == 4:
            return 'Friday'
        elif date.weekday() == 0:
            return 'Monday'
        else:
            return 'Weekday'
    
    def save_datasets(self, output_dir='logistics_sample_data'):
        """Generate and save all datasets"""
        os.makedirs(output_dir, exist_ok=True)
        
        print("Generating dimension tables...")
        
        # Generate dimensions
        dim_date = self.generate_date_dimension()
        dim_location = self.generate_location_dimension()
        dim_customer = self.generate_customer_dimension()
        dim_vehicle = self.generate_vehicle_dimension()
        dim_route = self.generate_route_dimension()
        dim_weather = self.generate_weather_dimension()
        
        print("Generating fact tables...")
        
        # Generate facts
        fact_shipments = self.generate_fact_shipments(dim_customer, dim_location, dim_vehicle, dim_route)
        fact_vehicle_telemetry = self.generate_fact_vehicle_telemetry(dim_vehicle)
        
        # Save all datasets
        datasets = {
            'dim_date': dim_date,
            'dim_location': dim_location,
            'dim_customer': dim_customer,
            'dim_vehicle': dim_vehicle,
            'dim_route': dim_route,
            'dim_weather': dim_weather,
            'fact_shipments': fact_shipments,
            'fact_vehicle_telemetry': fact_vehicle_telemetry
        }
        
        for name, df in datasets.items():
            filepath = os.path.join(output_dir, f'{name}.csv')
            df.to_csv(filepath, index=False)
            print(f"✓ Saved {name}: {len(df)} records to {filepath}")
        
        # Generate data quality report
        self._generate_data_quality_report(datasets, output_dir)
        
        return datasets
    
    def _generate_data_quality_report(self, datasets, output_dir):
        """Generate data quality report"""
        report = []
        
        for name, df in datasets.items():
            report.append({
                'table': name,
                'row_count': len(df),
                'column_count': len(df.columns),
                'null_percentage': round((df.isnull().sum().sum() / (len(df) * len(df.columns))) * 100, 2),
                'duplicate_rows': df.duplicated().sum(),
                'memory_usage_mb': round(df.memory_usage(deep=True).sum() / 1024**2, 2)
            })
        
        report_df = pd.DataFrame(report)
        report_df.to_csv(os.path.join(output_dir, 'data_quality_report.csv'), index=False)
        
        print("\nData Quality Summary:")
        print(report_df.to_string(index=False))

if __name__ == "__main__":
    print("Smart Logistics Analytics Platform - Sample Data Generator")
    print("=" * 60)
    
    generator = LogisticsDataGenerator()
    datasets = generator.save_datasets()
    
    print(f"\n✅ Successfully generated {len(datasets)} datasets")
    print("📊 Datasets are ready for import into Snowflake/dbt development")
    print("\nNext steps:")
    print("1. Upload CSV files to Snowflake using Fivetran or COPY INTO commands")
    print("2. Run dbt models to create the 22 analytical views")
    print("3. Validate data quality and relationships")
    print("4. Set up monitoring and alerting")