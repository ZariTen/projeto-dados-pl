from pyspark.sql import DataFrame
from pyspark.sql.functions import col

def check_no_nulls(df: DataFrame, columns: list):
    for c in columns:
        null_count = df.filter(col(c).isNull()).count()
        if null_count > 0:
            raise ValueError(f"Coluna {c} contém {null_count} nulos.")