#!/usr/bin/env python3
"""
Smart Logistics Analytics Platform - Sample Data Generator
Generates realistic sample datasets for the comprehensive logistics analytics project
Supports all tables: dimensions, facts, raw data, and real-time processing tables
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
                
                # Calculate additional fields for the updated schema
                fuel_cost = round(route['total_distance_km'] * vehicle['fuel_efficiency_l_100km'] * 1.6 / 100, 2)
                delivery_cost = round(random.uniform(50, 300), 2)
                revenue = round(random.uniform(100, 800), 2)
                total_cost = fuel_cost + delivery_cost
                profit_margin = round((revenue - total_cost) / revenue * 100, 2) if revenue > 0 else 0
                
                # Calculate route efficiency score
                route_efficiency = round(100 - ((actual_time - base_time) / base_time * 100), 1) if base_time > 0 else 50
                route_efficiency = max(0, min(100, route_efficiency))
                
                # Calculate carbon emissions (simplified)
                carbon_emissions = round(route['total_distance_km'] * 0.2, 2)  # kg CO2 per km
                
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
                    'fuel_cost': fuel_cost,
                    'delivery_cost': delivery_cost,
                    'revenue': revenue,
                    'is_on_time': is_on_time,
                    'is_delivered': is_on_time,
                    'delivery_status': 'Delivered' if is_on_time else random.choice(['In Transit', 'Delayed', 'Failed']),
                    'priority_level': random.choice(['Standard', 'High', 'Urgent']),
                    'service_type': customer['service_level'],
                    # New calculated fields for updated schema
                    'actual_distance_miles': round(route['total_distance_km'] * 0.621371, 2),
                    'planned_distance_miles': round(route['total_distance_km'] * 0.621371, 2),
                    'actual_delivery_time_hours': round(actual_time / 60, 2) if is_on_time else None,
                    'estimated_delivery_time_hours': round(base_time / 60, 2),
                    'fuel_cost_usd': fuel_cost,
                    'driver_cost_usd': delivery_cost,
                    'total_cost_usd': total_cost,
                    'profit_margin_pct': profit_margin,
                    'on_time_delivery_flag': 1 if is_on_time else 0,
                    'route_efficiency_score': route_efficiency,
                    'carbon_emissions_kg': carbon_emissions,
                    'weather_delay_minutes': round(random.uniform(0, 30), 0) if not is_on_time else 0,
                    'traffic_delay_minutes': round(random.uniform(0, 20), 0) if not is_on_time else 0
                })
                shipment_id += 1
        
        return pd.DataFrame(shipments)
    
    def generate_fact_vehicle_telemetry(self, vehicles_df):
        """Generate vehicle telemetry data"""
        telemetry_data = []
        
        # Generate telemetry for last 90 days
        dates = pd.date_range(start=self.end_date - timedelta(days=90), end=self.end_date, freq='h')
        
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
    
    def generate_traffic_conditions_dimension(self):
        """Generate traffic conditions dimension"""
        traffic_data = []
        
        # Generate traffic data for last 90 days
        dates = pd.date_range(start=self.end_date - timedelta(days=90), end=self.end_date, freq='h')
        traffic_id = 1
        
        for date in dates:
            for city_info in self.major_cities:
                # Traffic patterns based on time of day and day of week
                hour = date.hour
                day_of_week = date.weekday()
                
                # Peak hours: 7-9 AM and 5-7 PM on weekdays
                if day_of_week < 5 and (7 <= hour <= 9 or 17 <= hour <= 19):
                    traffic_level = random.choices(['Heavy', 'Severe'], weights=[0.7, 0.3])[0]
                    congestion_delay = random.uniform(15, 45)
                elif day_of_week < 5 and (10 <= hour <= 16):
                    traffic_level = random.choices(['Light', 'Moderate'], weights=[0.6, 0.4])[0]
                    congestion_delay = random.uniform(0, 10)
                else:
                    traffic_level = random.choices(['Light', 'Moderate'], weights=[0.8, 0.2])[0]
                    congestion_delay = random.uniform(0, 5)
                
                traffic_data.append({
                    'traffic_id': traffic_id,
                    'date': date.date(),
                    'hour_of_day': hour,
                    'city': city_info['city'],
                    'traffic_level': traffic_level,
                    'congestion_delay_minutes': round(congestion_delay, 1),
                    'average_speed_kmh': round(random.uniform(20, 80), 1),
                    'incident_count': random.randint(0, 3),
                    'road_closure_count': random.randint(0, 1),
                    'weather_impact': random.choice(['None', 'Light', 'Moderate', 'Heavy']),
                    'is_peak_hours': 1 if (day_of_week < 5 and (7 <= hour <= 9 or 17 <= hour <= 19)) else 0
                })
                traffic_id += 1
        
        return pd.DataFrame(traffic_data)
    
    def generate_vehicle_maintenance_dimension(self, vehicles_df):
        """Generate vehicle maintenance dimension"""
        maintenance_data = []
        maintenance_id = 1
        
        for vehicle in vehicles_df.itertuples():
            # Generate maintenance history for each vehicle
            num_maintenance_records = random.randint(5, 20)
            
            for i in range(num_maintenance_records):
                maintenance_date = fake.date_between(
                    start_date=vehicle.purchase_date, 
                    end_date='today'
                )
                
                maintenance_type = random.choice([
                    'Routine Service', 'Oil Change', 'Brake Service', 
                    'Tire Replacement', 'Engine Repair', 'Transmission Service',
                    'Electrical Repair', 'Body Work', 'Preventive Maintenance'
                ])
                
                # Cost based on maintenance type
                cost_ranges = {
                    'Routine Service': (200, 800),
                    'Oil Change': (50, 150),
                    'Brake Service': (300, 1200),
                    'Tire Replacement': (400, 2000),
                    'Engine Repair': (1000, 8000),
                    'Transmission Service': (1500, 5000),
                    'Electrical Repair': (200, 1500),
                    'Body Work': (500, 3000),
                    'Preventive Maintenance': (100, 500)
                }
                
                cost_range = cost_ranges.get(maintenance_type, (100, 1000))
                maintenance_cost = round(random.uniform(*cost_range), 2)
                
                maintenance_data.append({
                    'maintenance_id': maintenance_id,
                    'vehicle_id': vehicle.vehicle_id,
                    'maintenance_type': maintenance_type,
                    'maintenance_date': maintenance_date,
                    'maintenance_mileage': random.randint(50000, 500000),
                    'maintenance_cost_usd': maintenance_cost,
                    'maintenance_duration_hours': round(random.uniform(1, 8), 1),
                    'next_maintenance_due_date': maintenance_date + timedelta(days=random.randint(30, 180)),
                    'next_maintenance_due_mileage': random.randint(50000, 500000),
                    'maintenance_status': random.choice(['Completed', 'In Progress', 'Scheduled']),
                    'risk_score': random.randint(1, 10),
                    'parts_replaced': json.dumps([fake.word() for _ in range(random.randint(0, 3))]),
                    'service_provider': fake.company(),
                    'warranty_covered': random.choice([True, False])
                })
                maintenance_id += 1
        
        return pd.DataFrame(maintenance_data)
    
    def generate_fact_route_conditions(self, routes_df, weather_df, traffic_df):
        """Generate route conditions fact table"""
        route_conditions = []
        condition_id = 1
        
        # Generate route conditions for last 30 days
        dates = pd.date_range(start=self.end_date - timedelta(days=30), end=self.end_date, freq='D')
        
        for date in dates:
            for route in routes_df[routes_df['is_active']].itertuples():
                # Get weather and traffic data for this date
                weather = weather_df[
                    (weather_df['date'] == date.date()) & 
                    (weather_df['city'].str.contains(route.route_name.split('-')[0], case=False))
                ]
                traffic = traffic_df[
                    (traffic_df['date'] == date.date()) & 
                    (traffic_df['city'].str.contains(route.route_name.split('-')[0], case=False))
                ]
                
                if not weather.empty and not traffic.empty:
                    weather_condition = weather.iloc[0]
                    traffic_condition = traffic.iloc[0]
                    
                    # Calculate route performance impact
                    weather_impact = weather_condition['weather_severity_score'] / 10
                    traffic_impact = traffic_condition['congestion_delay_minutes'] / 60
                    
                    route_conditions.append({
                        'condition_id': condition_id,
                        'route_id': route.route_id,
                        'date': date.date(),
                        'weather_condition': weather_condition['condition'],
                        'temperature_c': weather_condition['temperature_c'],
                        'precipitation_mm': weather_condition['precipitation_mm'],
                        'wind_speed_kmh': weather_condition['wind_speed_kmh'],
                        'visibility_km': weather_condition['visibility_km'],
                        'traffic_level': traffic_condition['traffic_level'],
                        'congestion_delay_minutes': traffic_condition['congestion_delay_minutes'],
                        'average_speed_kmh': traffic_condition['average_speed_kmh'],
                        'route_performance_score': round(100 - (weather_impact * 30 + traffic_impact * 20), 1),
                        'safety_risk_score': round(weather_impact * 40 + traffic_impact * 30, 1),
                        'fuel_efficiency_impact_pct': round((weather_impact + traffic_impact) * 15, 1),
                        'delivery_delay_risk_pct': round((weather_impact + traffic_impact) * 25, 1)
                    })
                    condition_id += 1
        
        return pd.DataFrame(route_conditions)
    
    def generate_fact_vehicle_utilization(self, vehicles_df, shipments_df):
        """Generate vehicle utilization fact table"""
        utilization_data = []
        utilization_id = 1
        
        # Generate daily utilization for last 90 days
        dates = pd.date_range(start=self.end_date - timedelta(days=90), end=self.end_date, freq='D')
        
        for date in dates:
            for vehicle in vehicles_df[vehicles_df['is_active']].itertuples():
                # Get shipments for this vehicle on this date
                vehicle_shipments = shipments_df[
                    (shipments_df['vehicle_id'] == vehicle.vehicle_id) & 
                    (shipments_df['shipment_date'] == date.date())
                ]
                
                if not vehicle_shipments.empty:
                    total_weight = vehicle_shipments['weight_kg'].sum()
                    total_volume = vehicle_shipments['volume_m3'].sum()
                    total_distance = vehicle_shipments['distance_km'].sum()
                    total_revenue = vehicle_shipments['revenue'].sum()
                    total_cost = vehicle_shipments['fuel_cost'].sum() + vehicle_shipments['delivery_cost'].sum()
                    
                    utilization_data.append({
                        'utilization_id': utilization_id,
                        'vehicle_id': vehicle.vehicle_id,
                        'date': date.date(),
                        'total_shipments': len(vehicle_shipments),
                        'total_weight_kg': round(total_weight, 1),
                        'total_volume_m3': round(total_volume, 2),
                        'total_distance_km': round(total_distance, 1),
                        'total_revenue': round(total_revenue, 2),
                        'total_cost': round(total_cost, 2),
                        'capacity_utilization_pct': round((total_weight / vehicle.capacity_kg) * 100, 1),
                        'volume_utilization_pct': round((total_volume / (vehicle.capacity_kg / 100)) * 100, 1),
                        'distance_utilization_km': round(total_distance, 1),
                        'revenue_per_km': round(total_revenue / max(total_distance, 1), 2),
                        'cost_per_km': round(total_cost / max(total_distance, 1), 2),
                        'profit_per_km': round((total_revenue - total_cost) / max(total_distance, 1), 2),
                        'utilization_score': round(((total_weight / vehicle.capacity_kg) + (total_volume / (vehicle.capacity_kg / 100))) / 2 * 100, 1),
                        'efficiency_rating': random.choice(['Excellent', 'Good', 'Average', 'Poor']),
                        'maintenance_required': random.random() > 0.95
                    })
                    utilization_id += 1
        
        return pd.DataFrame(utilization_data)
    
    def generate_raw_azure_tables(self, customers_df, vehicles_df, shipments_df, maintenance_df):
        """Generate raw Azure SQL tables (source data) matching expected schemas"""
        raw_data = {}
        
        # Raw Azure Customers - Match expected schema from raw_azure_customers.sql
        raw_data['raw_azure_customers'] = pd.DataFrame({
            'customer_id': customers_df['customer_id'],
            'customer_name': customers_df['customer_name'],
            'customer_type': customers_df['customer_type'],
            'industry_code': customers_df['industry'].map({
                'Retail': 'RETAIL',
                'Manufacturing': 'MFG',
                'Healthcare': 'HEALTH',
                'Technology': 'TECH',
                'Food & Beverage': 'F&B',
                'Automotive': 'AUTO'
            }),
            'credit_limit': customers_df['total_lifetime_value'] * random.uniform(0.5, 2.0),
            'payment_terms': customers_df['payment_terms'],
            'customer_since': customers_df['signup_date'],
            'status': 'ACTIVE',
            'billing_address': [fake.address() for _ in range(len(customers_df))],
            'shipping_address': [fake.address() for _ in range(len(customers_df))],
            'contact_email': [fake.email() for _ in range(len(customers_df))],
            'contact_phone': [fake.phone_number() for _ in range(len(customers_df))],
            'account_manager': [fake.name() for _ in range(len(customers_df))],
            'created_at': [fake.date_time_between(start_date='-3y', end_date='now') for _ in range(len(customers_df))],
            'updated_at': [fake.date_time_between(start_date='-1y', end_date='now') for _ in range(len(customers_df))],
            '_loaded_at': [fake.date_time_between(start_date='-1d', end_date='now') for _ in range(len(customers_df))]
        })
        
        # Raw Azure Vehicles - Match expected schema from raw_azure_vehicles.sql
        raw_data['raw_azure_vehicles'] = pd.DataFrame({
            'vehicle_id': vehicles_df['vehicle_id'],
            'vehicle_number': vehicles_df['vehicle_id'],
            'vehicle_type': vehicles_df['vehicle_type'],
            'make': vehicles_df['make'],
            'model': vehicles_df['model'],
            'model_year': vehicles_df['year'],
            'capacity_lbs': vehicles_df['capacity_kg'] * 2.20462,
            'capacity_cubic_feet': vehicles_df['capacity_kg'] * 0.0353147,  # Rough conversion
            'fuel_type': vehicles_df['fuel_type'],
            'fuel_efficiency_mpg': 235.214 / vehicles_df['fuel_efficiency_l_100km'],
            'maintenance_interval_miles': [random.randint(10000, 50000) for _ in range(len(vehicles_df))],
            'current_mileage': vehicles_df['odometer_km'] * 0.621371,
            'last_maintenance_date': vehicles_df['last_service_date'],
            'next_maintenance_date': vehicles_df['next_service_due'],
            'vehicle_status': vehicles_df['is_active'].map({True: 'ACTIVE', False: 'MAINTENANCE'}),
            'assigned_driver_id': [fake.name() for _ in range(len(vehicles_df))],
            'insurance_expiry': [fake.date_between(start_date='today', end_date='+1y') for _ in range(len(vehicles_df))],
            'registration_expiry': [fake.date_between(start_date='today', end_date='+1y') for _ in range(len(vehicles_df))],
            'purchase_date': vehicles_df['purchase_date'],
            'purchase_price': [random.uniform(20000, 150000) for _ in range(len(vehicles_df))],
            'current_value': [random.uniform(10000, 100000) for _ in range(len(vehicles_df))],
            'created_at': [fake.date_time_between(start_date='-5y', end_date='now') for _ in range(len(vehicles_df))],
            'updated_at': [fake.date_time_between(start_date='-1y', end_date='now') for _ in range(len(vehicles_df))],
            '_loaded_at': [fake.date_time_between(start_date='-1d', end_date='now') for _ in range(len(vehicles_df))]
        })
        
        # Raw Azure Shipments - Match expected schema from raw_azure_shipments.sql
        raw_data['raw_azure_shipments'] = pd.DataFrame({
            'shipment_id': shipments_df['shipment_id'],
            'customer_id': shipments_df['customer_id'],
            'vehicle_id': shipments_df['vehicle_id'],
            'driver_id': [fake.name() for _ in range(len(shipments_df))],
            'origin_location_id': shipments_df['origin_location_id'],
            'destination_location_id': shipments_df['destination_location_id'],
            'pickup_date': shipments_df['shipment_date'],
            'delivery_date': shipments_df['actual_delivery_date'],
            'requested_delivery_date': shipments_df['planned_delivery_date'],
            'actual_delivery_date': shipments_df['actual_delivery_date'],
            'shipment_status': shipments_df['delivery_status'].map({
                'Delivered': 'DELIVERED',
                'In Transit': 'IN_TRANSIT',
                'Pending': 'PENDING',
                'Cancelled': 'CANCELLED'
            }),
            'weight_lbs': shipments_df['weight_kg'] * 2.20462,
            'volume_cubic_feet': shipments_df['volume_m3'] * 35.3147,
            'shipment_value': shipments_df['revenue'],
            'fuel_cost': shipments_df['fuel_cost'],
            'driver_cost': shipments_df['delivery_cost'],
            'total_cost': shipments_df['fuel_cost'] + shipments_df['delivery_cost'],
            'revenue': shipments_df['revenue'],
            'distance_miles': shipments_df['distance_km'] * 0.621371,
            'delivery_time_hours': shipments_df['actual_duration_minutes'] / 60.0,
            'on_time_delivery': shipments_df['is_on_time'],
            'weather_conditions': [random.choice(['Clear', 'Rain', 'Snow', 'Fog']) for _ in range(len(shipments_df))],
            'traffic_conditions': [random.choice(['Light', 'Moderate', 'Heavy']) for _ in range(len(shipments_df))],
            'special_instructions': [fake.sentence() if random.random() > 0.7 else None for _ in range(len(shipments_df))],
            'created_at': [fake.date_time_between(start_date='-2y', end_date='now') for _ in range(len(shipments_df))],
            'updated_at': [fake.date_time_between(start_date='-1y', end_date='now') for _ in range(len(shipments_df))],
            '_loaded_at': [fake.date_time_between(start_date='-1d', end_date='now') for _ in range(len(shipments_df))]
        })
        
        # Raw Azure Maintenance - Match expected schema from raw_azure_maintenance.sql
        raw_data['raw_azure_maintenance'] = pd.DataFrame({
            'maintenance_id': maintenance_df['maintenance_id'],
            'vehicle_id': maintenance_df['vehicle_id'],
            'maintenance_type': maintenance_df['maintenance_type'],
            'maintenance_date': maintenance_df['maintenance_date'],
            'odometer_reading': maintenance_df['maintenance_mileage'],
            'description': [fake.sentence() for _ in range(len(maintenance_df))],
            'parts_cost': maintenance_df['maintenance_cost_usd'] * random.uniform(0.3, 0.7),
            'labor_cost': maintenance_df['maintenance_cost_usd'] * random.uniform(0.3, 0.7),
            'total_cost': maintenance_df['maintenance_cost_usd'],
            'maintenance_provider': maintenance_df['service_provider'],
            'next_maintenance_due_date': maintenance_df['next_maintenance_due_date'],
            'next_maintenance_due_mileage': maintenance_df['next_maintenance_due_mileage'],
            'maintenance_status': 'COMPLETED',
            'created_at': [fake.date_time_between(start_date='-2y', end_date='now') for _ in range(len(maintenance_df))],
            'updated_at': [fake.date_time_between(start_date='-1y', end_date='now') for _ in range(len(maintenance_df))],
            '_loaded_at': [fake.date_time_between(start_date='-1d', end_date='now') for _ in range(len(maintenance_df))]
        })
        
        return raw_data
    
    def generate_additional_raw_tables(self, vehicles_df, locations_df):
        """Generate additional raw data tables for telematics, traffic, and weather"""
        raw_data = {}
        
        # Raw Telematics Data
        telematics_data = []
        telemetry_id = 1
        
        # Generate telematics data for last 7 days
        dates = pd.date_range(start=self.end_date - timedelta(days=7), end=self.end_date, freq='h')
        
        for vehicle in vehicles_df[vehicles_df['telematics_enabled']].itertuples():
            for date in dates[::random.randint(1, 4)]:  # Sample some hours
                telematics_data.append({
                    'telemetry_id': telemetry_id,
                    'vehicle_id': vehicle.vehicle_id,
                    'timestamp': date,
                    'latitude': random.uniform(-45, -10),  # Australia bounds
                    'longitude': random.uniform(110, 160),
                    'speed_mph': round(random.uniform(0, 70), 1),  # Convert to mph
                    'heading_degrees': random.randint(0, 360),
                    'engine_rpm': random.randint(800, 4000),
                    'fuel_level_pct': random.randint(10, 100),
                    'engine_temperature_f': round(random.uniform(180, 220), 1),  # Convert to Fahrenheit
                    'battery_voltage': round(random.uniform(12.0, 14.5), 1),
                    'odometer_miles': vehicle.odometer_km * 0.621371,  # Convert to miles
                    'acceleration_g': round(random.uniform(-0.5, 0.5), 2),
                    'brake_force': round(random.uniform(0, 100), 1),
                    'steering_angle': round(random.uniform(-180, 180), 1),
                    'gps_accuracy_meters': random.randint(1, 10),
                    'signal_strength': random.randint(1, 5),
                    'created_at': fake.date_time_between(start_date='-7d', end_date='now'),
                    '_loaded_at': fake.date_time_between(start_date='-1d', end_date='now')
                })
                telemetry_id += 1
        
        raw_data['raw_telematics_data'] = pd.DataFrame(telematics_data)
        
        # Raw Traffic Data
        traffic_data = []
        traffic_id = 1
        
        # Generate traffic data for last 30 days
        dates = pd.date_range(start=self.end_date - timedelta(days=30), end=self.end_date, freq='h')
        
        for date in dates:
            for city_info in self.major_cities:
                hour = date.hour
                day_of_week = date.weekday()
                
                # Traffic patterns based on time of day and day of week
                if day_of_week < 5 and (7 <= hour <= 9 or 17 <= hour <= 19):
                    traffic_level = random.choices(['HEAVY', 'SEVERE'], weights=[0.7, 0.3])[0]
                    congestion_delay = random.uniform(15, 45)
                elif day_of_week < 5 and (10 <= hour <= 16):
                    traffic_level = random.choices(['LIGHT', 'MODERATE'], weights=[0.6, 0.4])[0]
                    congestion_delay = random.uniform(0, 10)
                else:
                    traffic_level = random.choices(['LIGHT', 'MODERATE'], weights=[0.8, 0.2])[0]
                    congestion_delay = random.uniform(0, 5)
                
                traffic_data.append({
                    'traffic_id': traffic_id,
                    'location_id': random.randint(1, 240),  # Match location IDs
                    'date': date.date(),
                    'hour': hour,
                    'traffic_level': traffic_level,
                    'congestion_delay_minutes': round(congestion_delay, 1),
                    'average_speed_mph': round(random.uniform(20, 80), 1),
                    'free_flow_speed_mph': round(random.uniform(50, 70), 1),
                    'travel_time_minutes': round(random.uniform(10, 60), 1),
                    'free_flow_travel_time_minutes': round(random.uniform(5, 20), 1),
                    'confidence_score': round(random.uniform(0.7, 1.0), 2),
                    'road_type': random.choice(['HIGHWAY', 'ARTERIAL', 'LOCAL']),
                    'incident_count': random.randint(0, 3),
                    'weather_impact': random.choice(['NONE', 'LIGHT', 'MODERATE', 'HEAVY']),
                    'created_at': fake.date_time_between(start_date='-30d', end_date='now'),
                    '_loaded_at': fake.date_time_between(start_date='-1d', end_date='now')
                })
                traffic_id += 1
        
        raw_data['raw_traffic_data'] = pd.DataFrame(traffic_data)
        
        # Raw Weather Data
        weather_data = []
        weather_id = 1
        
        # Generate weather data for last 30 days
        dates = pd.date_range(start=self.end_date - timedelta(days=30), end=self.end_date, freq='h')
        
        for date in dates:
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
                temperature_c = random.uniform(*temp_range)
                temperature_f = (temperature_c * 9/5) + 32
                
                weather_data.append({
                    'weather_id': weather_id,
                    'location_id': random.randint(1, 240),  # Match location IDs
                    'date': date.date(),
                    'hour': date.hour,
                    'temperature_f': round(temperature_f, 1),
                    'temperature_c': round(temperature_c, 1),
                    'humidity_pct': random.randint(30, 95),
                    'wind_speed_mph': round(random.uniform(0, 30), 1),
                    'wind_direction_degrees': random.randint(0, 360),
                    'precipitation_mm': round(random.uniform(0, 50) if random.random() > 0.7 else 0, 1),
                    'visibility_miles': round(random.uniform(5, 50), 1),
                    'weather_condition': random.choice(['CLEAR', 'PARTLY_CLOUDY', 'CLOUDY', 'LIGHT_RAIN', 'RAIN', 'HEAVY_RAIN', 'STORM', 'FOG']),
                    'weather_description': random.choice(['Clear skies', 'Partly cloudy', 'Overcast', 'Light rain', 'Heavy rain', 'Thunderstorm', 'Foggy']),
                    'pressure_inhg': round(random.uniform(29.5, 30.5), 2),
                    'uv_index': random.randint(0, 11),
                    'sunrise_time': '06:00:00',
                    'sunset_time': '18:00:00',
                    'created_at': fake.date_time_between(start_date='-30d', end_date='now'),
                    '_loaded_at': fake.date_time_between(start_date='-1d', end_date='now')
                })
                weather_id += 1
        
        raw_data['raw_weather_data'] = pd.DataFrame(weather_data)
        
        return raw_data
    
    def generate_real_time_tables(self):
        """Generate real-time processing tables"""
        real_time_data = {}
        
        # Real-time KPIs
        kpi_data = []
        for i in range(100):
            kpi_data.append({
                'metric_id': i + 1,
                'metric_name': random.choice([
                    'on_time_delivery_rate', 'avg_delivery_time_hours', 
                    'revenue_per_hour', 'avg_profit_margin', 'avg_route_efficiency'
                ]),
                'metric_value': round(random.uniform(0, 100), 2),
                'dimensions': json.dumps({'timeframe': random.choice(['last_hour', 'current_hour', 'last_24h'])}),
                'timestamp': fake.date_time_between(start_date='-1h', end_date='now'),
                'alert_threshold': round(random.uniform(50, 90), 2),
                'alert_triggered': random.choice([True, False])
            })
        real_time_data['real_time_kpis'] = pd.DataFrame(kpi_data)
        
        # Real-time Vehicle Alerts
        alert_data = []
        for i in range(50):
            alert_data.append({
                'alert_id': i + 1,
                'vehicle_id': f'VH{str(random.randint(1, 200)).zfill(4)}',
                'alert_type': random.choice([
                    'ENGINE_OVERHEATING', 'LOW_FUEL', 'SPEEDING', 
                    'HARSH_BRAKING', 'MAINTENANCE_DUE', 'GPS_SIGNAL_LOST'
                ]),
                'severity': random.choice(['INFO', 'WARNING', 'CRITICAL']),
                'message': fake.sentence(),
                'timestamp': fake.date_time_between(start_date='-1h', end_date='now'),
                'resolved': random.choice([True, False]),
                'resolved_timestamp': fake.date_time_between(start_date='-1h', end_date='now') if random.choice([True, False]) else None
            })
        real_time_data['real_time_vehicle_alerts'] = pd.DataFrame(alert_data)
        
        return real_time_data
    
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
        
        print("🚀 Generating comprehensive logistics analytics datasets...")
        print("=" * 60)
        
        print("📊 Generating dimension tables...")
        
        # Generate all dimensions
        dim_date = self.generate_date_dimension()
        dim_location = self.generate_location_dimension()
        dim_customer = self.generate_customer_dimension()
        dim_vehicle = self.generate_vehicle_dimension()
        dim_route = self.generate_route_dimension()
        dim_weather = self.generate_weather_dimension()
        dim_traffic_conditions = self.generate_traffic_conditions_dimension()
        dim_vehicle_maintenance = self.generate_vehicle_maintenance_dimension(dim_vehicle)
        
        print("📈 Generating fact tables...")
        
        # Generate all facts
        fact_shipments = self.generate_fact_shipments(dim_customer, dim_location, dim_vehicle, dim_route)
        fact_vehicle_telemetry = self.generate_fact_vehicle_telemetry(dim_vehicle)
        fact_route_conditions = self.generate_fact_route_conditions(dim_route, dim_weather, dim_traffic_conditions)
        fact_vehicle_utilization = self.generate_fact_vehicle_utilization(dim_vehicle, fact_shipments)
        
        print("🗄️ Generating raw source tables...")
        
        # Generate raw Azure tables
        raw_azure_tables = self.generate_raw_azure_tables(dim_customer, dim_vehicle, fact_shipments, dim_vehicle_maintenance)
        
        print("📡 Generating additional raw data sources...")
        
        # Generate additional raw tables (telematics, traffic, weather)
        additional_raw_tables = self.generate_additional_raw_tables(dim_vehicle, dim_location)
        
        print("⚡ Generating real-time processing tables...")
        
        # Generate real-time tables
        real_time_tables = self.generate_real_time_tables()
        
        # Combine all datasets
        datasets = {
            # Dimension tables
            'dim_date': dim_date,
            'dim_location': dim_location,
            'dim_customer': dim_customer,
            'dim_vehicle': dim_vehicle,
            'dim_route': dim_route,
            'dim_weather': dim_weather,
            'dim_traffic_conditions': dim_traffic_conditions,
            'dim_vehicle_maintenance': dim_vehicle_maintenance,
            
            # Fact tables
            'fact_shipments': fact_shipments,
            'fact_vehicle_telemetry': fact_vehicle_telemetry,
            'fact_route_conditions': fact_route_conditions,
            'fact_vehicle_utilization': fact_vehicle_utilization,
            
            # Raw source tables
            **raw_azure_tables,
            **additional_raw_tables,
            
            # Real-time processing tables
            **real_time_tables
        }
        
        print("💾 Saving all datasets...")
        
        # Save all datasets
        for name, df in datasets.items():
            filepath = os.path.join(output_dir, f'{name}.csv')
            df.to_csv(filepath, index=False)
            print(f"✓ Saved {name}: {len(df):,} records to {filepath}")
        
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
    print("🚀 Smart Logistics Analytics Platform - Comprehensive Sample Data Generator")
    print("=" * 80)
    
    generator = LogisticsDataGenerator()
    datasets = generator.save_datasets()
    
    print(f"\n✅ Successfully generated {len(datasets)} comprehensive datasets!")
    print("📊 All datasets are ready for import into Snowflake/dbt development")
    
    print("\n📋 Generated Tables Summary:")
    print("=" * 50)
    
    # Group tables by category
    dimension_tables = [k for k in datasets.keys() if k.startswith('dim_')]
    fact_tables = [k for k in datasets.keys() if k.startswith('fact_')]
    raw_tables = [k for k in datasets.keys() if k.startswith('raw_')]
    real_time_tables = [k for k in datasets.keys() if k.startswith('real_time_')]
    
    print(f"📊 Dimension Tables ({len(dimension_tables)}):")
    for table in sorted(dimension_tables):
        print(f"   • {table}: {len(datasets[table]):,} records")
    
    print(f"\n📈 Fact Tables ({len(fact_tables)}):")
    for table in sorted(fact_tables):
        print(f"   • {table}: {len(datasets[table]):,} records")
    
    print(f"\n🗄️ Raw Source Tables ({len(raw_tables)}):")
    for table in sorted(raw_tables):
        print(f"   • {table}: {len(datasets[table]):,} records")
    
    print(f"\n⚡ Real-time Processing Tables ({len(real_time_tables)}):")
    for table in sorted(real_time_tables):
        print(f"   • {table}: {len(datasets[table]):,} records")
    
    print(f"\n🎯 Total Records Generated: {sum(len(df) for df in datasets.values()):,}")
    
    print("\n🚀 Next Steps:")
    print("=" * 30)
    print("1. 📤 Upload CSV files to Snowflake using Fivetran or COPY INTO commands")
    print("2. 🔄 Run dbt models to create all analytical views and ML features")
    print("3. ✅ Validate data quality and relationships using dbt tests")
    print("4. 📊 Set up real-time monitoring and alerting")
    print("5. 🤖 Deploy ML models using the generated feature store")
    print("6. 📈 Create dashboards and reports using the analytics models")
    
    print(f"\n💡 Pro Tips:")
    print("   • Use the data quality report to validate data integrity")
    print("   • Start with dimension tables, then fact tables")
    print("   • Test real-time processing with the generated streaming data")
    print("   • Leverage the comprehensive feature store for ML model training")