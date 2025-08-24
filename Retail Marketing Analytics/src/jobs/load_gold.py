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

