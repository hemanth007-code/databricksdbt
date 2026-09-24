from pyspark.sql import DataFrame
from typing import List
from pyspark.sql import Window
from pyspark.sql.functions import *

class transformations:
    def dedup(self, df: DataFrame, dedup_cols: List[str], cdc: str):
        df = df.withColumn("dedupKey", concat(*[col(c) for c in dedup_cols]))
        df = df.withColumn("dedupRank", row_number().over(Window.partitionBy("dedupKey").orderBy(col(cdc).desc())))
        df = df.filter(col("dedupRank") == 1)
        df = df.drop("dedupKey", "dedupRank")
        return df

    


