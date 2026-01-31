from pyspark.sql import DataFrame
from pyspark.sql import functions as F

def sanitize_columns(df: DataFrame) -> DataFrame:
    """
    Padroniza nomes de colunas: minúsculas, sem espaços nas pontas,
    e substitui espaços internos por underscores.
    """
    new_columns = [
        F.col(c).alias(c.strip().lower().replace(" ", "_")) 
        for c in df.columns
    ]
    return df.select(*new_columns)

def count_nulls_per_column(df: DataFrame) -> DataFrame:
    null_counts = df.select([F.count(F.when(F.col(c).isNull(), c)).alias(c) for c in df.columns])
    return null_counts