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

