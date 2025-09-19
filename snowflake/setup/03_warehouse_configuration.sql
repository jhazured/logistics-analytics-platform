-- Create optimized warehouses for different workloads
CREATE WAREHOUSE IF NOT EXISTS WH_LOADING
WITH WAREHOUSE_SIZE = 'LARGE'
     AUTO_SUSPEND = 60
     AUTO_RESUME = TRUE
     COMMENT = 'Warehouse for data loading operations';

CREATE WAREHOUSE IF NOT EXISTS WH_ANALYTICS
WITH WAREHOUSE_SIZE = 'MEDIUM'
     AUTO_SUSPEND = 300
     AUTO_RESUME = TRUE
     COMMENT = 'Warehouse for analytics and reporting';

CREATE WAREHOUSE IF NOT EXISTS WH_ML
WITH WAREHOUSE_SIZE = 'XLARGE'
     AUTO_SUSPEND = 60
     AUTO_RESUME = TRUE
     COMMENT = 'Warehouse for ML model training and scoring';

CREATE OR REPLACE WAREHOUSE logistics_etl_small
    WAREHOUSE_SIZE = 'XSMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE;
