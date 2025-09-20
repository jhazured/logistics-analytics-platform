# Data Dictionary - Logistics Analytics Platform

This document provides comprehensive business definitions and technical specifications for all data elements in the logistics analytics platform.

## Table of Contents
1. [Business Glossary](#business-glossary)
2. [Dimension Tables](#dimension-tables)
3. [Fact Tables](#fact-tables)
4. [ML Feature Store](#ml-feature-store)
5. [Analytics Views](#analytics-views)
6. [Data Quality Metrics](#data-quality-metrics)
7. [Calculated Fields](#calculated-fields)

## Business Glossary

### Core Business Terms

| Term | Definition | Business Context |
|------|------------|------------------|
| **On-Time Delivery** | Delivery completed within the agreed SLA window | Key performance indicator for customer satisfaction |
| **Route Efficiency** | Ratio of actual distance to optimal distance | Measures how well routes are optimized |
| **Fuel Efficiency** | Miles per gallon (MPG) or kilometers per liter | Critical cost driver in logistics operations |
| **Customer Tier** | Classification based on volume, revenue, and strategic importance | Determines service levels and pricing |
| **Haul Type** | Classification of shipment distance (Short: <100mi, Medium: 100-500mi, Long: >500mi) | Affects pricing, vehicle selection, and planning |
| **Predictive Maintenance** | ML-driven maintenance scheduling based on vehicle telemetry | Reduces breakdowns and optimizes maintenance costs |
| **Carbon Footprint** | CO2 emissions calculated from fuel consumption | Sustainability metric for ESG reporting |

### Performance Metrics

| Metric | Definition | Calculation | Business Impact |
|--------|------------|-------------|-----------------|
| **On-Time Rate** | Percentage of deliveries completed on time | (On-time deliveries / Total deliveries) × 100 | Customer satisfaction, SLA compliance |
| **Route Efficiency Score** | How close actual route is to optimal | (Optimal distance / Actual distance) × 100 | Cost optimization, fuel savings |
| **Profit Margin** | Revenue minus total costs | (Revenue - Total Costs) / Revenue × 100 | Financial performance |
| **Vehicle Utilization** | Percentage of vehicle capacity used | (Actual weight / Max capacity) × 100 | Asset optimization |
| **Customer Lifetime Value** | Total revenue from customer over time | Sum of all shipments × Average margin | Customer segmentation |

## Dimension Tables

### tbl_dim_customer
**Purpose**: Master customer data with business classifications

| Column | Data Type | Business Definition | Valid Values |
|--------|-----------|-------------------|--------------|
| `customer_id` | VARCHAR | Unique customer identifier | UUID format |
| `customer_name` | VARCHAR | Legal business name | Non-null, max 255 chars |
| `customer_tier` | VARCHAR | Business classification | 'PLATINUM', 'GOLD', 'SILVER', 'BRONZE' |
| `industry_code` | VARCHAR | Industry classification | SIC codes, e.g., '4841' (General Freight) |
| `credit_limit_usd` | DECIMAL | Maximum credit allowed | > 0, typically $10K - $1M |
| `payment_terms` | VARCHAR | Payment agreement | 'NET_30', 'NET_15', 'CASH_ON_DELIVERY' |
| `customer_since` | DATE | First shipment date | Historical date, not future |
| `status` | VARCHAR | Account status | 'ACTIVE', 'INACTIVE', 'SUSPENDED' |

**Business Rules**:
- Customer tier determines service levels and pricing
- Credit limit affects shipment approval process
- Payment terms impact cash flow planning

### tbl_dim_vehicle
**Purpose**: Fleet management and vehicle specifications

| Column | Data Type | Business Definition | Valid Values |
|--------|-----------|-------------------|--------------|
| `vehicle_id` | VARCHAR | Unique vehicle identifier | UUID format |
| `vehicle_number` | VARCHAR | Fleet number for operations | Format: 'VH-XXXX' |
| `vehicle_type` | VARCHAR | Vehicle category | 'TRUCK', 'VAN', 'TRAILER', 'SPECIALIZED' |
| `make` | VARCHAR | Manufacturer | 'FREIGHTLINER', 'VOLVO', 'PETERBILT', etc. |
| `model` | VARCHAR | Vehicle model | Manufacturer-specific |
| `capacity_kg` | DECIMAL | Maximum payload weight | > 0, typically 1,000 - 40,000 kg |
| `fuel_efficiency_mpg` | DECIMAL | Miles per gallon rating | 5.0 - 15.0 MPG for trucks |
| `vehicle_status` | VARCHAR | Current operational status | 'ACTIVE', 'MAINTENANCE', 'RETIRED' |

**Business Rules**:
- Vehicle type determines suitable shipment types
- Capacity affects route planning and load optimization
- Fuel efficiency impacts cost calculations

### tbl_dim_route
**Purpose**: Route definitions and characteristics

| Column | Data Type | Business Definition | Valid Values |
|--------|-----------|-------------------|--------------|
| `route_id` | VARCHAR | Unique route identifier | UUID format |
| `origin_location_id` | VARCHAR | Starting point | Valid location_id |
| `destination_location_id` | VARCHAR | End point | Valid location_id |
| `route_name` | VARCHAR | Human-readable route name | Format: 'Origin-Destination' |
| `distance_km` | DECIMAL | Total route distance | > 0, typically 10 - 3,000 km |
| `estimated_duration_hours` | DECIMAL | Expected travel time | > 0, typically 0.5 - 48 hours |
| `route_type` | VARCHAR | Route classification | 'HIGHWAY', 'CITY', 'MIXED', 'SPECIAL' |
| `toll_cost_usd` | DECIMAL | Expected toll expenses | ≥ 0, typically $0 - $200 |

**Business Rules**:
- Distance and duration affect pricing and scheduling
- Route type impacts fuel efficiency and timing
- Toll costs are included in total cost calculations

## Fact Tables

### tbl_fact_shipments
**Purpose**: Core business transactions and performance metrics

| Column | Data Type | Business Definition | Calculation |
|--------|-----------|-------------------|-------------|
| `shipment_id` | VARCHAR | Unique shipment identifier | System generated |
| `customer_id` | VARCHAR | Customer reference | FK to tbl_dim_customer |
| `vehicle_id` | VARCHAR | Vehicle used | FK to tbl_dim_vehicle |
| `route_id` | VARCHAR | Route taken | FK to tbl_dim_route |
| `pickup_date` | DATE | Actual pickup date | From operations system |
| `delivery_date` | DATE | Actual delivery date | From operations system |
| `requested_delivery_date` | DATE | Customer requested date | From order system |
| `weight_kg` | DECIMAL | Shipment weight | Measured at pickup |
| `volume_cubic_meters` | DECIMAL | Shipment volume | Calculated from dimensions |
| `revenue_usd` | DECIMAL | Customer payment | From billing system |
| `fuel_cost_usd` | DECIMAL | Fuel expenses | Calculated from distance × fuel price |
| `driver_cost_usd` | DECIMAL | Driver compensation | Hours × hourly rate |
| `total_cost_usd` | DECIMAL | Total operational cost | Sum of all costs |
| `profit_margin_pct` | DECIMAL | Profit percentage | (Revenue - Total Cost) / Revenue × 100 |
| `on_time_delivery_flag` | BOOLEAN | SLA compliance | delivery_date ≤ requested_delivery_date |
| `carbon_emissions_kg` | DECIMAL | CO2 emissions | Distance × Fuel consumption × Emission factor |

**Business Rules**:
- Revenue must be positive
- Total cost includes fuel, driver, tolls, and overhead
- Profit margin should be > 5% for sustainable operations
- Carbon emissions calculated using EPA factors

### tbl_fact_vehicle_telemetry
**Purpose**: Real-time vehicle performance and maintenance data

| Column | Data Type | Business Definition | Business Impact |
|--------|-----------|-------------------|-----------------|
| `telemetry_id` | VARCHAR | Unique telemetry record | System generated |
| `vehicle_id` | VARCHAR | Vehicle reference | FK to tbl_dim_vehicle |
| `timestamp` | TIMESTAMP | Data collection time | Real-time from IoT sensors |
| `speed_mph` | DECIMAL | Current speed | 0 - 80 mph (legal limits) |
| `engine_rpm` | INTEGER | Engine revolutions per minute | 600 - 2,500 RPM (normal range) |
| `fuel_level_pct` | DECIMAL | Fuel tank percentage | 0 - 100% |
| `engine_temperature_f` | DECIMAL | Engine temperature | 180 - 220°F (normal range) |
| `odometer_miles` | DECIMAL | Total vehicle mileage | Cumulative, always increasing |
| `brake_pressure_psi` | DECIMAL | Brake system pressure | 0 - 1,200 PSI |
| `maintenance_risk_score` | DECIMAL | ML-calculated risk | 0 - 100 (higher = more risk) |

**Business Rules**:
- Speed data used for route optimization
- Engine temperature alerts for maintenance
- Fuel level triggers refueling stops
- Maintenance risk score triggers service scheduling

## ML Feature Store

### tbl_ml_consolidated_feature_store
**Purpose**: Unified feature repository for machine learning models

| Column | Data Type | Business Definition | ML Use Case |
|--------|-----------|-------------------|-------------|
| `feature_id` | VARCHAR | Unique feature record | System generated |
| `customer_id` | VARCHAR | Customer reference | Customer segmentation models |
| `vehicle_id` | VARCHAR | Vehicle reference | Predictive maintenance models |
| `route_id` | VARCHAR | Route reference | Route optimization models |
| `feature_date` | DATE | Feature calculation date | Time-based features |
| `customer_tier_numeric` | INTEGER | Numeric customer tier | 1=Bronze, 2=Silver, 3=Gold, 4=Platinum |
| `customer_tenure_days` | INTEGER | Days since first shipment | Customer lifetime value models |
| `customer_on_time_rate_30d` | DECIMAL | 30-day on-time performance | Customer reliability scoring |
| `vehicle_age_years` | DECIMAL | Vehicle age in years | Maintenance prediction |
| `vehicle_utilization_30d` | DECIMAL | 30-day capacity utilization | Asset optimization |
| `route_efficiency_score` | DECIMAL | Historical route performance | Route optimization |
| `fuel_efficiency_trend` | DECIMAL | 7-day fuel efficiency trend | Cost optimization |
| `maintenance_risk_score` | DECIMAL | ML-calculated maintenance risk | Predictive maintenance |
| `weather_impact_score` | DECIMAL | Weather impact on performance | Route planning |
| `traffic_delay_factor` | DECIMAL | Traffic impact on timing | Delivery time prediction |

**Business Rules**:
- Features updated daily for real-time models
- Historical features maintain 90-day rolling windows
- Risk scores trigger automated alerts
- Performance features used for optimization

## Analytics Views

### vw_consolidated_dashboard
**Purpose**: Executive dashboard with key performance indicators

| Column | Data Type | Business Definition | Business Impact |
|--------|-----------|-------------------|-----------------|
| `date_key` | DATE | Reporting date | Time dimension |
| `total_shipments` | INTEGER | Daily shipment count | Volume indicator |
| `on_time_rate_pct` | DECIMAL | Daily on-time percentage | Customer satisfaction |
| `avg_profit_margin_pct` | DECIMAL | Average daily profit margin | Financial performance |
| `total_revenue_usd` | DECIMAL | Daily revenue | Financial performance |
| `total_carbon_emissions_kg` | DECIMAL | Daily CO2 emissions | Sustainability metric |
| `fuel_efficiency_mpg` | DECIMAL | Fleet average fuel efficiency | Cost optimization |
| `vehicle_utilization_pct` | DECIMAL | Fleet utilization rate | Asset optimization |

### vw_ai_recommendations
**Purpose**: AI-driven business recommendations

| Column | Data Type | Business Definition | Action Required |
|--------|-----------|-------------------|-----------------|
| `recommendation_id` | VARCHAR | Unique recommendation | System generated |
| `recommendation_type` | VARCHAR | Type of recommendation | 'ROUTE_OPTIMIZATION', 'MAINTENANCE', 'PRICING' |
| `entity_id` | VARCHAR | Affected entity | Customer, vehicle, or route ID |
| `recommendation_text` | TEXT | Human-readable recommendation | Action description |
| `confidence_score` | DECIMAL | ML model confidence | 0 - 100% |
| `expected_impact_usd` | DECIMAL | Estimated financial impact | Cost savings or revenue increase |
| `priority_level` | VARCHAR | Implementation priority | 'HIGH', 'MEDIUM', 'LOW' |

## Data Quality Metrics

### Data Freshness SLAs
| Table | SLA | Business Impact |
|-------|-----|-----------------|
| `tbl_fact_shipments` | 1 hour | Real-time operations |
| `tbl_fact_vehicle_telemetry` | 5 minutes | Safety and maintenance |
| `tbl_ml_consolidated_feature_store` | 4 hours | ML model accuracy |
| `vw_consolidated_dashboard` | 1 hour | Executive reporting |

### Data Completeness Requirements
| Table | Minimum Completeness | Business Impact |
|-------|---------------------|-----------------|
| `tbl_fact_shipments` | 99.5% | Financial accuracy |
| `tbl_dim_customer` | 100% | Customer service |
| `tbl_fact_vehicle_telemetry` | 95% | Safety compliance |

### Data Accuracy Thresholds
| Metric | Acceptable Range | Business Impact |
|--------|------------------|-----------------|
| Revenue calculations | ±$0.01 | Financial accuracy |
| Distance calculations | ±1% | Cost accuracy |
| Delivery times | ±5 minutes | Customer satisfaction |
| Fuel efficiency | ±2% | Cost optimization |

## Calculated Fields

### Business Calculations

#### Profit Margin Calculation
```sql
profit_margin_pct = (revenue_usd - total_cost_usd) / revenue_usd * 100
```
**Business Rule**: Must be > 5% for sustainable operations

#### Route Efficiency Score
```sql
route_efficiency_score = (optimal_distance_km / actual_distance_km) * 100
```
**Business Rule**: Target > 85% for cost optimization

#### Carbon Emissions Calculation
```sql
carbon_emissions_kg = distance_km * fuel_consumption_l_per_100km * 2.31
```
**Business Rule**: Uses EPA emission factor of 2.31 kg CO2 per liter

#### Customer Lifetime Value
```sql
customer_lifetime_value = SUM(revenue_usd) * AVG(profit_margin_pct) / 100
```
**Business Rule**: Calculated over 12-month rolling window

### ML Feature Calculations

#### Maintenance Risk Score
```sql
maintenance_risk_score = 
  (engine_temperature_anomaly * 0.3) +
  (mileage_since_service * 0.4) +
  (fuel_efficiency_decline * 0.3)
```
**Business Rule**: Score > 70 triggers maintenance alert

#### Customer Reliability Score
```sql
customer_reliability_score = 
  (on_time_rate_30d * 0.5) +
  (payment_timeliness * 0.3) +
  (communication_quality * 0.2)
```
**Business Rule**: Score < 60 triggers account review

## Data Lineage

### Source Systems
| Source | Tables | Update Frequency | Business Owner |
|--------|--------|------------------|----------------|
| Azure ERP | customers, vehicles, routes | Real-time | Operations |
| Fivetran | shipments, maintenance | Hourly | Logistics |
| IoT Sensors | vehicle_telemetry | Real-time | Fleet Management |
| Weather API | weather_conditions | 15 minutes | Route Planning |
| Traffic API | traffic_conditions | 5 minutes | Route Planning |

### Transformation Rules
1. **Raw to Staging**: Data type validation, null handling, format standardization
2. **Staging to Marts**: Business logic application, calculated fields, aggregations
3. **Marts to Analytics**: KPI calculations, trend analysis, performance metrics
4. **Analytics to ML**: Feature engineering, normalization, time-series preparation

## Business Rules Summary

### Data Validation Rules
- All monetary values must be positive
- Dates cannot be in the future (except planned dates)
- Percentages must be between 0 and 100
- IDs must follow UUID format
- Status fields must use predefined values

### Business Logic Rules
- Customer tier affects pricing and service levels
- Vehicle capacity cannot be exceeded
- Maintenance must be scheduled before risk score reaches 80
- Routes must be optimized for efficiency > 85%
- Carbon emissions must be tracked for ESG reporting

### Data Retention Rules
- Shipment data: 7 years (regulatory requirement)
- Telemetry data: 2 years (operational analysis)
- ML features: 1 year (model training)
- Audit logs: 90 days (compliance)

---

**Last Updated**: December 2024  
**Version**: 1.0  
**Owner**: Data Engineering Team  
**Review Cycle**: Quarterly
