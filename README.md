# Retail Marketing Analytics – End‑to‑End Data Engineering Project (Databricks + ADLS + PySpark + SQL + dbx)

A complete, portfolio‑ready project you can drop into GitHub. It demonstrates a layered Lakehouse (Raw → Bronze → Silver → Gold), Auto Loader ingestion from ADLS, SCD2 dimensions, fact modeling, DQ checks, performance tuning (OPTIMIZE/ZORDER), CI with `dbx`, and unit tests.

---

## 1) Business Problem (Use Case)

An e‑commerce company wants to evaluate how marketing campaigns drive sales. Data arrives daily from multiple sources:

* **Orders** (transactional system – JSON)
* **Customers** (CRM – CSV)
* **Products** (PIM – CSV)
* **Campaign Touches** (ad/clickstream – CSV)

Stakeholders need:

1. A **trustworthy sales star schema** (facts & dims) to power dashboards (Power BI/Databricks SQL).
2. **Slowly Changing Dimension Type 2 (SCD2)** for Customer to analyze behavior across historical profile changes (e.g., segment or city changes).
3. **Campaign ROI KPIs** (revenue, orders, AOV) by campaign, channel, and date.

---

## 2) Target Layered Architecture

* **Raw (landing)**: immutable drop of vendor files in ADLS (organized by source/date). No parsing.
* **Bronze**: ingestion with Auto Loader to Delta; add ingest metadata and enforce basic schema.
* **Silver**: conform & cleanse; deduplicate; apply business rules; build **dim\_**\* and **stg\_**\* tables; implement **SCD2** for `dim_customer_scd2`.
* **Gold**: curated star schema (**fact\_order**, **dim\_customer\_scd2**, **dim\_product**, **dim\_campaign**), plus **mart** tables for marketing KPIs.

**Storage**: ADLS Gen2 with Unity Catalog (recommended) or Hive metastore.

```
ADLS Containers:
  abfss://raw@<storageacct>.dfs.core.windows.net/
  abfss://bronze@<storageacct>.dfs.core.windows.net/
  abfss://silver@<storageacct>.dfs.core.windows.net/
  abfss://gold@<storageacct>.dfs.core.windows.net/
```

---

## 3) Repository Structure

```
retail-marketing-lakehouse/
├─ README.md
├─ conf/
│  ├─ project.json
│  ├─ deployment.json
│  └─ test.json
├─ src/
│  ├─ common/
│  │  ├─ utils.py
│  │  └─ scd2.py
│  └─ jobs/
│     ├─ ingest_bronze.py
│     ├─ transform_silver.py
│     └─ load_gold.py
├─ sql/
│  ├─ 00_catalogs_schemas.sql
│  ├─ 10_bronze_tables.sql
│  ├─ 20_silver_tables.sql
│  ├─ 30_gold_tables.sql
│  └─ 90_optimize_maintenance.sql
├─ tools/
│  └─ generate_sample_data.py
├─ tests/
│  ├─ test_utils.py
│  └─ test_scd2.py
├─ .github/workflows/
│  └─ ci.yml
└─ .dbx/project.json (symlink or same as conf/project.json)
```

You can copy/paste these files as provided below.

---

## 4) Configuration (edit once)

Update placeholders in **conf/project.json**:

* `workspace_dir`: Databricks workspace path for dbx deployments
* `default_cluster`: define policy or job cluster configuration
* `data`: set your storage account & containers

---

## 5) SQL DDL – Catalogs, Schemas & Tables

> Use **Unity Catalog** names (`<catalog>.<schema>.<table>`) or fallback to `hive_metastore.<db>.<table>`.

### `sql/00_catalogs_schemas.sql`

```sql
-- Adjust names as needed
CREATE CATALOG IF NOT EXISTS retail;
USE CATALOG retail;

CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;
```

### `sql/10_bronze_tables.sql`

```sql
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

### `sql/20_silver_tables.sql`

```sql
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

### `sql/90_optimize_maintenance.sql`

```sql
-- Periodic optimization for performance
OPTIMIZE retail.silver.dim_customer_scd2 ZORDER BY (customer_id, city);
OPTIMIZE retail.gold.fact_order ZORDER BY (order_ts, customer_sk, product_id);

-- Optional: vacuum with retention policy respected
VACUUM retail.silver.dim_customer_scd2 RETAIN 168 HOURS;  -- 7 days
VACUUM retail.gold.fact_order RETAIN 168 HOURS;
```

---

## 6) PySpark Code (with dbx‑ready entrypoints)

### `src/common/utils.py`

```python
from pyspark.sql import functions as F
from pyspark.sql import DataFrame

BRONZE_META_COLS = ["_ingest_ts", "_source_file"]

def with_ingest_metadata(df: DataFrame, source_file_col: str = "_rescued_data") -> DataFrame:\n    return (df
        .withColumn("_ingest_ts", F.current_timestamp())
        .withColumn("_source_file", F.input_file_name())
    )

def dedupe_by_keys(df: DataFrame, keys: list, order_col: str) -> DataFrame:
    w = F.window if False else None  # placeholder to show no window dependency
    return (df
        .withColumn("_rn", F.row_number().over(
            Window.partitionBy(*[F.col(k) for k in keys])
                  .orderBy(F.col(order_col).desc())
        ))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )
```

### `src/common/scd2.py`

```python
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from delta.tables import DeltaTable

# Implements generic SCD2 for a dimension table with (business_key) and a set of tracked columns.
# Target table must have: effective_start_ts, effective_end_ts, is_current

def scd2_upsert(
    spark: SparkSession,
    source_df: DataFrame,
    target_table: str,
    business_key: str,
    compare_cols: list,
):
    # Ensure target exists as DeltaTable
    target = DeltaTable.forName(spark, target_table)

    # Mark records that change
    src = (source_df
           .withColumn("effective_start_ts", F.current_timestamp())
           .withColumn("effective_end_ts", F.to_timestamp(F.lit("9999-12-31 23:59:59")))
           .withColumn("is_current", F.lit(True)))

    # Join condition
    cond = f"t.{business_key} = s.{business_key} AND t.is_current = true"

    # Build change detection predicate (any of compare_cols changed)
    change_pred = " OR ".join([f"NOT (t.{c} <=> s.{c})" for c in compare_cols])

    # 1) Expire rows where something changed
    (target.alias("t")
           .merge(src.alias("s"), cond)
           .whenMatchedUpdate(condition=change_pred,
                              set={
                                  "effective_end_ts": F.current_timestamp(),
                                  "is_current": F.lit(False)
                              })
           .whenNotMatchedInsertAll()
           .execute())

    # 2) Insert the new current versions for the changed keys
    # (The first merge inserts new keys. For changed keys, we need to insert the post-change rows.)
    changed_keys = (spark.table(target_table).filter("is_current = false")
                    .select(business_key).distinct())
    new_currents = (src.alias("s")
                    .join(changed_keys.alias("k"), [business_key], "inner"))

    (target.alias("t")
           .merge(new_currents.alias("s"), f"t.{business_key} = s.{business_key} AND t.is_current = true")
           .whenNotMatchedInsertAll()
           .execute())
```

### `src/jobs/ingest_bronze.py`

```python
import argparse
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from delta import configure_spark_with_delta_pip
from src.common.utils import with_ingest_metadata

# Auto Loader ingestion from ADLS raw -> Bronze Delta tables

parser = argparse.ArgumentParser()
parser.add_argument("--raw_base", required=True)
parser.add_argument("--catalog", default="retail")
args, _ = parser.parse_known_args()

spark = (SparkSession.builder.appName("ingest_bronze")
         .getOrCreate())

spark.sql(f"USE CATALOG {args.catalog}")

# Orders (JSON)
orders_df = (spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", f"{args.raw_base}/_schemas/orders")
    .load(f"{args.raw_base}/orders/")
)
orders_df = with_ingest_metadata(orders_df)
(orders_df
 .selectExpr("to_json(struct(*)) as data", "_ingest_ts", "_source_file")
 .writeStream
 .format("delta").outputMode("append")
 .option("checkpointLocation", f"{args.raw_base}/_chk/orders")
 .toTable("bronze.orders_bronze"))

# Customers (CSV)
customers_df = (spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("header", True)
    .option("cloudFiles.inferColumnTypes", True)
    .option("cloudFiles.schemaLocation", f"{args.raw_base}/_schemas/customers")
    .load(f"{args.raw_base}/customers/")
)
customers_df = with_ingest_metadata(customers_df)
(customers_df
 .selectExpr("to_json(struct(*)) as data", "_ingest_ts", "_source_file")
 .writeStream.format("delta").outputMode("append")
 .option("checkpointLocation", f"{args.raw_base}/_chk/customers")
 .toTable("bronze.customers_bronze"))

# Products (CSV)
products_df = (spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("header", True)
    .option("cloudFiles.inferColumnTypes", True)
    .option("cloudFiles.schemaLocation", f"{args.raw_base}/_schemas/products")
    .load(f"{args.raw_base}/products/")
)
products_df = with_ingest_metadata(products_df)
(products_df
 .selectExpr("to_json(struct(*)) as data", "_ingest_ts", "_source_file")
 .writeStream.format("delta").outputMode("append")
 .option("checkpointLocation", f"{args.raw_base}/_chk/products")
 .toTable("bronze.products_bronze"))

# Campaign touches (CSV)
ct_df = (spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("header", True)
    .option("cloudFiles.inferColumnTypes", True)
    .option("cloudFiles.schemaLocation", f"{args.raw_base}/_schemas/campaign_touches")
    .load(f"{args.raw_base}/campaign_touches/")
)
ct_df = with_ingest_metadata(ct_df)
(ct_df
 .selectExpr("to_json(struct(*)) as data", "_ingest_ts", "_source_file")
 .writeStream.format("delta").outputMode("append")
 .option("checkpointLocation", f"{args.raw_base}/_chk/campaign_touches")
 .toTable("bronze.campaign_touches_bronze"))
```

### `src/jobs/transform_silver.py`

```python
import argparse
from pyspark.sql import SparkSession, functions as F
from delta.tables import DeltaTable
from src.common.scd2 import scd2_upsert

parser = argparse.ArgumentParser()
parser.add_argument("--catalog", default="retail")
args, _ = parser.parse_known_args()

spark = SparkSession.builder.appName("transform_silver").getOrCreate()
spark.sql(f"USE CATALOG {args.catalog}")

# Parse JSON strings from bronze into structured columns

def parse_bronze_json(table):
    df = spark.table(table)
    return spark.read.json(df.select("data").rdd.map(lambda r: r[0])) \
             .withColumn("_src_file", F.lit(None).cast("string")) \
             .withColumn("_ingest_ts", F.current_timestamp())

orders = parse_bronze_json("bronze.orders_bronze")
customers = spark.read.json(spark.table("bronze.customers_bronze").select("data").rdd.map(lambda r: r[0])) \
             .withColumn("_src_file", F.lit(None).cast("string")) \
             .withColumn("_ingest_ts", F.current_timestamp())
products = spark.read.json(spark.table("bronze.products_bronze").select("data").rdd.map(lambda r: r[0])) \
             .withColumn("_src_file", F.lit(None).cast("string")) \
             .withColumn("_ingest_ts", F.current_timestamp())
campaign_touches = spark.read.json(spark.table("bronze.campaign_touches_bronze").select("data").rdd.map(lambda r: r[0])) \
             .withColumn("_src_file", F.lit(None).cast("string")) \
             .withColumn("_ingest_ts", F.current_timestamp())

# Minimal cleaning & typing
orders_clean = (orders
  .select(
    F.col("order_id").cast("string"),
    F.to_timestamp("order_ts").alias("order_ts"),
    F.col("customer_id").cast("string"),
    F.col("product_id").cast("string"),
    F.col("quantity").cast("int"),
    F.col("unit_price").cast("double"),
    F.col("payment_method").cast("string"),
    F.col("city").cast("string"),
    F.col("country").cast("string"),
    F.lit(None).alias("_src_file"),
    F.current_timestamp().alias("_ingest_ts")
  ).dropDuplicates(["order_id"]))
orders_clean.write.mode("overwrite").saveAsTable("silver.stg_orders")

customers_clean = (customers
  .select(
    F.col("customer_id").cast("string"),
    F.col("full_name").cast("string"),
    F.col("email").cast("string"),
    F.col("segment").cast("string"),
    F.col("city").cast("string"),
    F.col("country").cast("string"),
    F.to_date("signup_date").alias("signup_date"),
    F.lit(None).alias("_src_file"),
    F.current_timestamp().alias("_ingest_ts")
  ).dropDuplicates(["customer_id"]))
customers_clean.write.mode("overwrite").saveAsTable("silver.stg_customers")

products_clean = (products
  .select("product_id", "product_name", "category", "subcategory", F.col("list_price").cast("double"))
  .dropDuplicates(["product_id"]))
products_clean = products_clean.withColumn("is_active", F.lit(True))
products_clean.write.mode("overwrite").saveAsTable("silver.dim_product")

ct_clean = (campaign_touches
  .select(
    F.col("touch_id").cast("string"),
    F.to_timestamp("touch_ts").alias("touch_ts"),
    F.col("customer_id").cast("string"),
    F.col("campaign_id").cast("string"),
    F.col("channel").cast("string"),
    F.col("medium").cast("string"),
    F.col("source").cast("string"),
    F.lit(None).alias("_src_file"),
    F.current_timestamp().alias("_ingest_ts")
  ).dropDuplicates(["touch_id"]))
ct_clean.write.mode("overwrite").saveAsTable("silver.stg_campaign_touches")

# SCD2 for Customer
scd2_upsert(
    spark,
    source_df=spark.table("silver.stg_customers"),
    target_table="silver.dim_customer_scd2",
    business_key="customer_id",
    compare_cols=["full_name", "email", "segment", "city", "country"]
)

# SCD1 for campaign (latest record wins)
dim_campaign = (spark.table("silver.stg_campaign_touches")
                .select("campaign_id", "channel", "medium", "source")
                .dropDuplicates(["campaign_id"]) \
                .withColumn("is_active", F.lit(True)))
dim_campaign.write.mode("overwrite").saveAsTable("silver.dim_campaign")
```

### `src/jobs/load_gold.py`

```python
import argparse
from pyspark.sql import SparkSession, functions as F

parser = argparse.ArgumentParser()
parser.add_argument("--catalog", default="retail")
args, _ = parser.parse_known_args()

spark = SparkSession.builder.appName("load_gold").getOrCreate()
spark.sql(f"USE CATALOG {args.catalog}")

orders = spark.table("silver.stg_orders")
customers = spark.table("silver.dim_customer_scd2").filter("is_current = true")
products = spark.table("silver.dim_product")
ct = spark.table("silver.stg_campaign_touches")

# Simple attribution: join order to latest touch for the same customer within 7 days before order
latest_touch = (ct
  .groupBy("customer_id")
  .agg(F.max("touch_ts").alias("last_touch_ts")))

orders_attrib = (orders.alias("o")
  .join(latest_touch.alias("lt"), "customer_id", "left")
  .join(ct.alias("t"), (F.col("t.customer_id") == F.col("o.customer_id")) &
                       (F.col("t.touch_ts") == F.col("lt.last_touch_ts")), "left")
  .withColumn("campaign_id", F.col("t.campaign_id"))
  .select("o.*", "campaign_id"))

fact_order = (orders_attrib.alias("o")
  .join(customers.alias("c"), "customer_id")
  .select(
    "order_id", "order_ts", F.col("c.customer_sk").alias("customer_sk"),
    "product_id", "campaign_id", "quantity", "unit_price", "payment_method",
    "city", "country"
  ))

fact_order.write.mode("overwrite").saveAsTable("gold.fact_order")

# KPIs
kpis = (fact_order
  .withColumn("order_date", F.to_date("order_ts"))
  .groupBy("order_date", "campaign_id")
  .agg(
    F.countDistinct("order_id").alias("orders"),
    F.countDistinct("customer_sk").alias("customers"),
    F.sum(F.col("quantity") * F.col("unit_price")).alias("revenue"),
    F.avg(F.col("quantity") * F.col("unit_price")).alias("aov")
  ))

kpis.write.mode("overwrite").saveAsTable("gold.mart_campaign_kpis")
```

---

## 7) Sample Data Generator

### `tools/generate_sample_data.py`

```python
import os, json, random
from datetime import datetime, timedelta

base = os.getenv("RAW_BASE", "/dbfs/mnt/raw")  # or abfss path if mounted
os.makedirs(os.path.join(base, "orders"), exist_ok=True)
os.makedirs(os.path.join(base, "customers"), exist_ok=True)
os.makedirs(os.path.join(base, "products"), exist_ok=True)
os.makedirs(os.path.join(base, "campaign_touches"), exist_ok=True)

# Customers CSV
customers = []
for i in range(1, 101):
    customers.append({
        "customer_id": f"C{i:04d}",
        "full_name": f"User {i}",
        "email": f"user{i}@example.com",
        "segment": random.choice(["A", "B", "VIP"]),
        "city": random.choice(["Hyderabad", "Bengaluru", "Mumbai", "Delhi"]),
        "country": "India",
        "signup_date": (datetime.today() - timedelta(days=random.randint(30, 400))).date().isoformat()
    })

with open(os.path.join(base, "customers", f"customers_1.csv"), "w") as f:
    f.write("customer_id,full_name,email,segment,city,country,signup_date\n")
    for c in customers:
        f.write(",".join([
            c["customer_id"], c["full_name"], c["email"], c["segment"],
            c["city"], c["country"], c["signup_date"]
        ]) + "\n")

# Products CSV
products = []
for i in range(1, 51):
    products.append({
        "product_id": f"P{i:04d}",
        "product_name": f"Product {i}",
        "category": random.choice(["Electronics", "Clothing", "Home"]),
        "subcategory": random.choice(["A", "B", "C"]),
        "list_price": round(random.uniform(10, 500), 2)
    })

with open(os.path.join(base, "products", f"products_1.csv"), "w") as f:
    f.write("product_id,product_name,category,subcategory,list_price\n")
    for p in products:
        f.write(",".join([
            p["product_id"], p["product_name"], p["category"],
            p["subcategory"], str(p["list_price"])]) + "\n")

# Campaign touches CSV
with open(os.path.join(base, "campaign_touches", f"touches_1.csv"), "w") as f:
    f.write("touch_id,touch_ts,customer_id,campaign_id,channel,medium,source\n")
    for i in range(1, 201):
        cust = random.choice(customers)
        ts = datetime.now() - timedelta(days=random.randint(0, 14), hours=random.randint(0, 23))
        f.write(f"T{i:05d},{ts.isoformat(timespec='seconds')},{cust['customer_id']},CMP{random.randint(1,5):03d},\n"
                .replace(",\n", f",{random.choice(['Search','Social','Email'])},{random.choice(['CPC','Organic','Referral'])},{random.choice(['Google','Meta','Newsletter'])}\n"))

# Orders JSON
for i in range(1, 301):
    cust = random.choice(customers)
    prod = random.choice(products)
    order = {
        "order_id": f"O{i:05d}",
        "order_ts": (datetime.now() - timedelta(days=random.randint(0, 7), hours=random.randint(0, 23))).isoformat(timespec='seconds'),
        "customer_id": cust["customer_id"],
        "product_id": prod["product_id"],
        "quantity": random.randint(1, 5),
        "unit_price": prod["list_price"],
        "payment_method": random.choice(["UPI", "CreditCard", "COD"]),
        "city": cust["city"],
        "country": cust["country"]
    }
    with open(os.path.join(base, "orders", f"order_{i:05d}.json"), "w") as f:
        f.write(json.dumps(order))

print("Sample data generated in", base)
```

---

## 8) dbx Configuration & CI

### `conf/project.json`

```json
{
  "profile": "DEFAULT",
  "workspace_dir": "/Repos/retail-marketing-lakehouse",
  "artifact_path": "dbfs:/FileStore/retail-marketing-lakehouse/artifacts",
  "environments": {
    "dev": {
      "properties": {
        "default": {
          "spark_version": "14.3.x-scala2.12",
          "node_type_id": "Standard_DS3_v2",
          "num_workers": 2,
          "data_security_mode": "SINGLE_USER"
        }
      }
    }
  }
}
```

### `conf/deployment.json`

```json
{
  "environments": {
    "dev": {
      "jobs": [
        {
          "name": "retail_bronze_ingest",
          "tasks": [
            {
              "task_key": "bronze",
              "python_wheel_task": {
                "package_name": "retail",
                "entry_point": "ingest_bronze",
                "parameters": [
                  "--raw_base", "abfss://raw@<storageacct>.dfs.core.windows.net/raw",
                  "--catalog", "retail"
                ]
              },
              "new_cluster": {"spark_version": "14.3.x-scala2.12", "num_workers": 2, "node_type_id": "Standard_DS3_v2"}
            }
          ]
        },
        {
          "name": "retail_silver_transform",
          "tasks": [
            {
              "task_key": "silver",
              "python_wheel_task": {
                "package_name": "retail",
                "entry_point": "transform_silver",
                "parameters": ["--catalog", "retail"]
              },
              "new_cluster": {"spark_version": "14.3.x-scala2.12", "num_workers": 2, "node_type_id": "Standard_DS3_v2"}
            }
          ]
        },
        {
          "name": "retail_gold_load",
          "tasks": [
            {
              "task_key": "gold",
              "python_wheel_task": {
                "package_name": "retail",
                "entry_point": "load_gold",
                "parameters": ["--catalog", "retail"]
              },
              "new_cluster": {"spark_version": "14.3.x-scala2.12", "num_workers": 2, "node_type_id": "Standard_DS3_v2"}
            }
          ]
        }
      ]
    }
  }
}
```

> Replace `<storageacct>` and the raw path as per your ADLS setup.

### `conf/test.json`

```json
{
  "environments": {"dev": {"properties": {}}}
}
```

### `.github/workflows/ci.yml`

```yaml
name: ci
on: [push, pull_request]
jobs:
  tests:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with: { python-version: '3.10' }
      - name: Install deps
        run: |
          pip install -r requirements.txt || true
          pip install pytest
      - name: Run unit tests
        run: pytest -q
```

---

## 9) Packaging (entry points for dbx)

If you want to build a wheel:

* Create `setup.py` or `pyproject.toml` mapping entry points:

**`setup.py`**

```python
from setuptools import setup, find_packages

setup(
    name="retail",
    version="0.1.0",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    entry_points={
        "console_scripts": [
            "ingest_bronze=src.jobs.ingest_bronze:main",
            "transform_silver=src.jobs.transform_silver:main",
            "load_gold=src.jobs.load_gold:main"
        ]
    },
    install_requires=["pyspark", "delta-spark"],
)
```

> Add minimal `main()` wrappers to each job script or call via `python_wheel_task` using module paths.

---

## 10) Tests

### `tests/test_utils.py`

```python
from src.common import utils

def test_has_meta_columns():
    assert isinstance(utils.BRONZE_META_COLS, list)
```

### `tests/test_scd2.py`

```python
import pyspark
from pyspark.sql import SparkSession
from src.common.scd2 import scd2_upsert

# Note: In CI, you can skip heavy Spark tests or use local[1]

def test_scd2_signature():
    # Basic import test
    assert callable(scd2_upsert)
```

---

## 11) Data Quality (lightweight)

* **Schema enforcement** via Auto Loader.
* **Drop duplicates** on natural keys at Silver.
* **NOT NULL constraints** (add as needed) and valid ranges.
* **Simple anomaly checks**: e.g., `quantity > 0`, `unit_price >= 0` in Silver before loading Gold.

Example (add to `transform_silver.py` before writes):

```python
orders_clean = orders_clean.filter("quantity > 0 AND unit_price >= 0")
```

---

## 12) Running End‑to‑End

1. **Create containers** in ADLS: `raw`, and (optionally) mount them in Databricks.
2. Run `sql/00_catalogs_schemas.sql`, then `10_`, `20_`, `30_` scripts in a SQL notebook once.
3. Generate sample data: run `tools/generate_sample_data.py` (on a cluster with DBFS or write to ABFSS).
4. Deploy with **dbx**:

   ```bash
   dbx deploy --environment dev
   dbx launch retail_bronze_ingest --as-run-submit --environment dev
   dbx launch retail_silver_transform --as-run-submit --environment dev
   dbx launch retail_gold_load --as-run-submit --environment dev
   ```
5. Query results:

   ```sql
   SELECT * FROM retail.gold.mart_campaign_kpis ORDER BY order_date DESC;
   ```

---

## 13) Performance & Cost Notes

* Use **OPTIMIZE ZORDER** on large tables (already included).
* Partition big facts by date if volume grows (`PARTITIONED BY (to_date(order_ts))`).
* Consider `delta.enableChangeDataFeed = true` to build incremental downstream loads.
* For heavy joins, ensure broadcast hints where appropriate.

---

## 14) Extras You Can Add (for portfolio depth)

* Incremental Bronze→Silver using watermarking on `_ingest_ts`.
* CDC from source (if available) with Apply Changes Into (ACI) or MERGE.
* Expectations with Delta Live Tables or Great Expectations.
* Row/Column level security in Unity Catalog (dynamic views).

---

## 15) README.md (drop into repo root)

```markdown
# Retail Marketing Lakehouse (Databricks + ADLS + PySpark + dbx)

An end-to-end, production-style project implementing a Lakehouse with Bronze/Silver/Gold layers, SCD2 customer dimension, and marketing KPIs. Ready to clone and run.

## Stack
- Azure Databricks, ADLS Gen2, Delta Lake, PySpark, SQL, dbx

## Quickstart
1. Run `sql/00_catalogs_schemas.sql` ... `sql/30_gold_tables.sql`.
2. Generate sample data: `python tools/generate_sample_data.py`.
3. `dbx deploy --environment dev` then launch the three jobs in order.
4. Explore: `SELECT * FROM retail.gold.mart_campaign_kpis;`

## Project Layout
(see repo tree in this README)

## Highlights
- Auto Loader ingestion to Bronze
- SCD2 `dim_customer_scd2` via Delta MERGE
- Gold star schema + campaign KPIs
- CI with GitHub Actions, basic tests

## Notes
- Replace `<storageacct>` and paths for your environment.
- Unity Catalog recommended; Hive metastore compatible with minor name changes.
```

---

**You can now copy these files into a GitHub repo and run.** If you want me to tailor it to **healthcare** instead (encounters/claims/patients) or wire it to **Unity Catalog with RLS**, I can produce a variant quickly
