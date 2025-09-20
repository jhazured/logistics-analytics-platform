-- Fact Shipments (Core fact table)
CREATE OR REPLACE TABLE TBL_FACT_SHIPMENTS (
    shipment_id NUMBER PRIMARY KEY,
    date_key NUMBER(8) NOT NULL,
    customer_id NUMBER NOT NULL,
    origin_location_id NUMBER NOT NULL,
    destination_location_id NUMBER NOT NULL,
    vehicle_id VARCHAR(20) NOT NULL,
    route_id NUMBER NOT NULL,
    shipment_date DATE NOT NULL,
    planned_delivery_date DATE,
    actual_delivery_date DATE,
    weight_kg NUMBER(10,1),
    volume_m3 NUMBER(8,2),
    distance_km NUMBER(8,1),
    planned_duration_minutes NUMBER(6),
    actual_duration_minutes NUMBER(6),
    fuel_cost NUMBER(10,2),
    delivery_cost NUMBER(10,2),
    revenue NUMBER(10,2),
    is_on_time BOOLEAN,
    is_delivered BOOLEAN,
    customer_rating NUMBER(2),
    delivery_status VARCHAR(50),
    priority_level VARCHAR(20),
    service_type VARCHAR(50),
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    updated_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    
    -- Foreign Keys
    FOREIGN KEY (date_key) REFERENCES dim_date(date_key),
    FOREIGN KEY (customer_id) REFERENCES dim_customer(customer_id),
    FOREIGN KEY (origin_location_id) REFERENCES dim_location(location_id),
    FOREIGN KEY (destination_location_id) REFERENCES dim_location(location_id),
    FOREIGN KEY (vehicle_id) REFERENCES dim_vehicle(vehicle_id),
    FOREIGN KEY (route_id) REFERENCES dim_route(route_id)
)
CLUSTER BY (shipment_date, origin_location_id);