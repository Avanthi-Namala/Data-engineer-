USE CATALOG retail;

-- Bronze landing tables (ingested by Auto Loader)
CREATE TABLE IF NOT EXISTS bronze.orders_bronze (
  data STRING,
  _ingest_ts TIMESTAMP,
  _source_file STRING
) USING DELTA;

CREATE TABLE IF NOT EXISTS bronze.customers_bronze (
  data STRING,
  _ingest_ts TIMESTAMP,
  _source_file STRING
) USING DELTA;

CREATE TABLE IF NOT EXISTS bronze.products_bronze (
  data STRING,
  _ingest_ts TIMESTAMP,
  _source_file STRING
) USING DELTA;

CREATE TABLE IF NOT EXISTS bronze.campaign_touches_bronze (
  data STRING,
  _ingest_ts TIMESTAMP,
  _source_file STRING
) USING DELTA;
```
