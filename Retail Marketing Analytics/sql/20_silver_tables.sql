USE CATALOG retail;

-- Staging cleaned tables
CREATE TABLE IF NOT EXISTS silver.stg_orders (
  order_id STRING,
  order_ts TIMESTAMP,
  customer_id STRING,
  product_id STRING,
  quantity INT,
  unit_price DOUBLE,
  payment_method STRING,
  city STRING,
  country STRING,
  _src_file STRING,
  _ingest_ts TIMESTAMP
) USING DELTA;

CREATE TABLE IF NOT EXISTS silver.stg_customers (
  customer_id STRING,
  full_name STRING,
  email STRING,
  segment STRING,
  city STRING,
  country STRING,
  signup_date DATE,
  _src_file STRING,
  _ingest_ts TIMESTAMP
) USING DELTA;

CREATE TABLE IF NOT EXISTS silver.stg_products (
  product_id STRING,
  product_name STRING,
  category STRING,
  subcategory STRING,
  list_price DOUBLE,
  _src_file STRING,
  _ingest_ts TIMESTAMP
) USING DELTA;

CREATE TABLE IF NOT EXISTS silver.stg_campaign_touches (
  touch_id STRING,
  touch_ts TIMESTAMP,
  customer_id STRING,
  campaign_id STRING,
  channel STRING,
  medium STRING,
  source STRING,
  _src_file STRING,
  _ingest_ts TIMESTAMP
) USING DELTA;

-- SCD2 Customer Dimension
CREATE TABLE IF NOT EXISTS silver.dim_customer_scd2 (
  customer_sk BIGINT GENERATED ALWAYS AS IDENTITY,
  customer_id STRING,
  full_name STRING,
  email STRING,
  segment STRING,
  city STRING,
  country STRING,
  effective_start_ts TIMESTAMP,
  effective_end_ts TIMESTAMP,
  is_current BOOLEAN
) USING DELTA
TBLPROPERTIES (delta.enableChangeDataFeed = true);

-- Other conformed dims (SCD1 for simplicity)
CREATE TABLE IF NOT EXISTS silver.dim_product (
  product_id STRING,
  product_name STRING,
  category STRING,
  subcategory STRING,
  list_price DOUBLE,
  is_active BOOLEAN
) USING DELTA;

CREATE TABLE IF NOT EXISTS silver.dim_campaign (
  campaign_id STRING,
  channel STRING,
  medium STRING,
  source STRING,
  is_active BOOLEAN
) USING DELTA;
```

### `sql/30_gold_tables.sql`

```sql
USE CATALOG retail;

-- Gold fact and star schema
CREATE TABLE IF NOT EXISTS gold.fact_order (
  order_id STRING,
  order_ts TIMESTAMP,
  customer_sk BIGINT,
  product_id STRING,
  campaign_id STRING,
  quantity INT,
  unit_price DOUBLE,
  revenue DOUBLE GENERATED ALWAYS AS (quantity * unit_price),
  payment_method STRING,
  city STRING,
  country STRING
) USING DELTA;

-- Marketing KPIs (daily, by campaign)
CREATE TABLE IF NOT EXISTS gold.mart_campaign_kpis (
  order_date DATE,
  campaign_id STRING,
  orders INT,
  customers INT,
  revenue DOUBLE,
  aov DOUBLE
) USING DELTA;
```
