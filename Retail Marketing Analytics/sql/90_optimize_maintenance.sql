```sql
-- Periodic optimization for performance
OPTIMIZE retail.silver.dim_customer_scd2 ZORDER BY (customer_id, city);
OPTIMIZE retail.gold.fact_order ZORDER BY (order_ts, customer_sk, product_id);

-- Optional: vacuum with retention policy respected
VACUUM retail.silver.dim_customer_scd2 RETAIN 168 HOURS;  -- 7 days
VACUUM retail.gold.fact_order RETAIN 168 HOURS;
```
