-- Create schemas for different layers
CREATE SCHEMA IF NOT EXISTS RAW;           -- Raw data from sources
CREATE SCHEMA IF NOT EXISTS STAGING;      -- Cleaned and validated data
CREATE SCHEMA IF NOT EXISTS MART;         -- Business-ready dimensional model
CREATE SCHEMA IF NOT EXISTS ANALYTICS;    -- Advanced analytics and ML features

-- Set default schema for DDL
USE SCHEMA MART;
