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
