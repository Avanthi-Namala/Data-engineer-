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

