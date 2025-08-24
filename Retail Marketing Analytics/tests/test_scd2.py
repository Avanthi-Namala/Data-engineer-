```python
import pyspark
from pyspark.sql import SparkSession
from src.common.scd2 import scd2_upsert

# Note: In CI, you can skip heavy Spark tests or use local[1]

def test_scd2_signature():
    # Basic import test
    assert callable(scd2_upsert)
```
