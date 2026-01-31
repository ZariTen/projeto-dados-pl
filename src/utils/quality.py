from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import SparkSession

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

def count_nulls_per_column(df: DataFrame, camada: str, table_name: str, batch_size: int = 5) -> dict:
    """
    Conta valores nulos na tabela em lotes de colunas para otimizar performance.
    """
    columns = df.columns
    total_cols = len(columns)
    null_counts = {}

    print(f"Iniciando verificação de nulos na tabela {camada}: {table_name}")

    for i in range(0, total_cols, batch_size):
        batch_cols = columns[i : i + batch_size]
        
        expressions = [F.count(F.when(F.col(c).isNull(), 1)).alias(c) for c in batch_cols]
        
        row = df.select(*expressions).collect()[0]
        null_counts.update(row.asDict())

    for col_name, count in null_counts.items():
        if count > 0:
            print(f" - Coluna '{col_name}' possui {count} valores nulos.")

    return null_counts

def check_data_quality(spark: SparkSession, camada: str, table_name: str, batch_size: int = 5) -> dict:
    """
    Verifica a qualidade dos dados contando valores nulos por coluna.
    """
    df = spark.read.format("parquet").load(f"data/{camada}/{table_name}")
    count_nulls_per_column(df, camada, table_name, batch_size)