-- Iceberg warehouse setup (for Spark SQL / CLI)

CREATE DATABASE IF NOT EXISTS local;

CREATE TABLE IF NOT EXISTS local.bronze_events (
  source STRING,
  title STRING,
  timestamp STRING,
  ingested_at TIMESTAMP
)
USING iceberg;

CREATE TABLE IF NOT EXISTS local.silver_events (
  source STRING,
  event_title STRING,
  timestamp STRING,
  ingested_at TIMESTAMP
)
USING iceberg;