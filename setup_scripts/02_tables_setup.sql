USE DATABASE SNOWFLAKE_LEARNING_DB;
USE SCHEMA PUBLIC;
USE ROLE ACCOUNTADMIN;

-- --------------------------------------------------
-- Table: SITE_HAZARD_HISTORY
-- Stores hazard frequency per inspection for trend analysis
-- --------------------------------------------------
CREATE TABLE IF NOT EXISTS SNOWFLAKE_LEARNING_DB.PUBLIC.SITE_HAZARD_HISTORY (
    SITE_ID         STRING,
    INSPECTION_TS   TIMESTAMP,
    HAZARD_CATEGORY STRING,
    HAZARD_COUNT    INTEGER
);

-- --------------------------------------------------
-- Table: SITE_RISK_HISTORY
-- Stores site-level risk summaries per inspection
-- --------------------------------------------------
CREATE TABLE IF NOT EXISTS SNOWFLAKE_LEARNING_DB.PUBLIC.SITE_RISK_HISTORY (
    SITE_ID                     STRING,
    INSPECTION_TS               TIMESTAMP,
    IMAGE_COUNT                 INTEGER,
    WEIGHTED_SITE_RISK_SCORE    FLOAT,
    SITE_SEVERITY               STRING,
    HIGHEST_IMAGE_SCORE         INTEGER
);
