-- Example COPY INTO commands for loading CSV data

COPY INTO dim_date
FROM @my_stage/dim_date.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_DELIMITER = ',' SKIP_HEADER = 1);

COPY INTO dim_location
FROM @my_stage/dim_location.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_DELIMITER = ',' SKIP_HEADER = 1);

COPY INTO dim_customer
FROM @my_stage/dim_customer.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_DELIMITER = ',' SKIP_HEADER = 1);

COPY INTO dim_vehicle
FROM @my_stage/dim_vehicle.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_DELIMITER = ',' SKIP_HEADER = 1);

COPY INTO dim_route
FROM @my_stage/dim_route.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_DELIMITER = ',' SKIP_HEADER = 1);

COPY INTO dim_weather
FROM @my_stage/dim_weather.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_DELIMITER = ',' SKIP_HEADER = 1);

COPY INTO fact_shipments
FROM @my_stage/fact_shipments.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_DELIMITER = ',' SKIP_HEADER = 1);

COPY INTO fact_vehicle_telemetry
FROM @my_stage/fact_vehicle_telemetry.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_DELIMITER = ',' SKIP_HEADER = 1);
