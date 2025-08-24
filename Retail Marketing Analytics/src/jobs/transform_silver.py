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

