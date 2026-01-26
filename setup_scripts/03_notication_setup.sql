USE DATABASE SNOWFLAKE_LEARNING_DB;
USE SCHEMA PUBLIC;
USE ROLE ACCOUNTADMIN;

-- --------------------------------------------------
-- Notification Integration: SITE_EMAIL_INT
-- Used for automated high-risk alerts and manual report sharing
-- --------------------------------------------------
CREATE OR REPLACE NOTIFICATION INTEGRATION SITE_EMAIL_INT
  TYPE = EMAIL
  ENABLED = TRUE
  ALLOWED_RECIPIENTS = (
    'rafi.hidayat@synogize.io
  );
