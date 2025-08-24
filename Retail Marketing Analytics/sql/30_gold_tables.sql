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
