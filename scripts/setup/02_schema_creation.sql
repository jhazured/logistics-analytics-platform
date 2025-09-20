-- =====================================================
-- Schema Creation for Logistics Analytics Platform
-- =====================================================

-- Production Environment Schemas
USE DATABASE LOGISTICS_DW_PROD;

-- Core data layers
CREATE SCHEMA IF NOT EXISTS RAW;                    -- Raw data ingestion from external sources
CREATE SCHEMA IF NOT EXISTS STAGING;                -- Data cleaning and standardization
CREATE SCHEMA IF NOT EXISTS MARTS;                  -- Business logic and star schema
CREATE SCHEMA IF NOT EXISTS ML_FEATURES;            -- ML feature engineering
CREATE SCHEMA IF NOT EXISTS ANALYTICS;              -- Business intelligence views
CREATE SCHEMA IF NOT EXISTS MONITORING;             -- System monitoring and alerting
CREATE SCHEMA IF NOT EXISTS SNAPSHOTS;              -- Change data capture
CREATE SCHEMA IF NOT EXISTS ML_OBJECTS;             -- ML model registry and serving
CREATE SCHEMA IF NOT EXISTS GOVERNANCE;             -- Data governance and lineage
CREATE SCHEMA IF NOT EXISTS PERFORMANCE;            -- Performance optimization
CREATE SCHEMA IF NOT EXISTS SECURITY;               -- Security and access control

-- Development Environment Schemas
USE DATABASE LOGISTICS_DW_DEV;

CREATE SCHEMA IF NOT EXISTS RAW;                    -- Raw data ingestion (dev)
CREATE SCHEMA IF NOT EXISTS STAGING;                -- Data cleaning (dev)
CREATE SCHEMA IF NOT EXISTS MARTS;                  -- Business logic (dev)
CREATE SCHEMA IF NOT EXISTS ML_FEATURES;            -- ML features (dev)
CREATE SCHEMA IF NOT EXISTS ANALYTICS;              -- Analytics views (dev)
CREATE SCHEMA IF NOT EXISTS MONITORING;             -- Monitoring (dev)
CREATE SCHEMA IF NOT EXISTS SNAPSHOTS;              -- Snapshots (dev)
CREATE SCHEMA IF NOT EXISTS ML_OBJECTS;             -- ML objects (dev)
CREATE SCHEMA IF NOT EXISTS GOVERNANCE;             -- Governance (dev)
CREATE SCHEMA IF NOT EXISTS PERFORMANCE;            -- Performance (dev)
CREATE SCHEMA IF NOT EXISTS SECURITY;               -- Security (dev)

-- Staging Environment Schemas
USE DATABASE LOGISTICS_DW_STAGING;

CREATE SCHEMA IF NOT EXISTS RAW;                    -- Raw data ingestion (staging)
CREATE SCHEMA IF NOT EXISTS STAGING;                -- Data cleaning (staging)
CREATE SCHEMA IF NOT EXISTS MARTS;                  -- Business logic (staging)
CREATE SCHEMA IF NOT EXISTS ML_FEATURES;            -- ML features (staging)
CREATE SCHEMA IF NOT EXISTS ANALYTICS;              -- Analytics views (staging)
CREATE SCHEMA IF NOT EXISTS MONITORING;             -- Monitoring (staging)
CREATE SCHEMA IF NOT EXISTS SNAPSHOTS;              -- Snapshots (staging)
CREATE SCHEMA IF NOT EXISTS ML_OBJECTS;             -- ML objects (staging)
CREATE SCHEMA IF NOT EXISTS GOVERNANCE;             -- Governance (staging)
CREATE SCHEMA IF NOT EXISTS PERFORMANCE;            -- Performance (staging)
CREATE SCHEMA IF NOT EXISTS SECURITY;               -- Security (staging)

-- Set default schema for DDL
USE DATABASE LOGISTICS_DW_PROD;
USE SCHEMA MARTS;

-- Add schema comments
COMMENT ON SCHEMA LOGISTICS_DW_PROD.RAW IS 'Raw data ingestion from external sources (Fivetran, APIs)';
COMMENT ON SCHEMA LOGISTICS_DW_PROD.STAGING IS 'Data cleaning and standardization layer';
COMMENT ON SCHEMA LOGISTICS_DW_PROD.MARTS IS 'Business logic and star schema layer';
COMMENT ON SCHEMA LOGISTICS_DW_PROD.ML_FEATURES IS 'ML feature engineering and feature store';
COMMENT ON SCHEMA LOGISTICS_DW_PROD.ANALYTICS IS 'Business intelligence and analytics views';
COMMENT ON SCHEMA LOGISTICS_DW_PROD.MONITORING IS 'System monitoring and alerting';
COMMENT ON SCHEMA LOGISTICS_DW_PROD.SNAPSHOTS IS 'Change data capture and historical tracking';
COMMENT ON SCHEMA LOGISTICS_DW_PROD.ML_OBJECTS IS 'ML model registry and real-time serving';
COMMENT ON SCHEMA LOGISTICS_DW_PROD.GOVERNANCE IS 'Data governance and lineage tracking';
COMMENT ON SCHEMA LOGISTICS_DW_PROD.PERFORMANCE IS 'Performance optimization and monitoring';
COMMENT ON SCHEMA LOGISTICS_DW_PROD.SECURITY IS 'Security policies and access control';