-- =====================================================
-- Schema Creation for Logistics Analytics Platform
-- =====================================================
-- This script creates schemas based on environment variables
-- Usage: Set SF_DATABASE and SF_SCHEMA environment variables before running
-- Example: export SF_DATABASE="LOGISTICS_DW_DEV" && export SF_SCHEMA="ANALYTICS" && snowsql -f 02_schema_creation.sql

-- Get database and schema names from environment variables or use defaults
SET DATABASE_NAME = IFNULL($SF_DATABASE, 'LOGISTICS_DW_DEV');
SET DEFAULT_SCHEMA = IFNULL($SF_SCHEMA, 'ANALYTICS');

-- Use the specified database
USE DATABASE IDENTIFIER($DATABASE_NAME);

-- Core data layers - Create all schemas
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

-- Set default schema for DDL operations
USE SCHEMA IDENTIFIER($DEFAULT_SCHEMA);