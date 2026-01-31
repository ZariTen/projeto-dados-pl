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

    for i in range(0, total_cols, batch_size):
        batch_cols = columns[i : i + batch_size]
        
        expressions = [F.count(F.when(F.col(c).isNull(), 1)).alias(c) for c in batch_cols]
        
        row = df.select(*expressions).collect()[0]
        null_counts.update(row.asDict())

    for col_name, count in null_counts.items():
        if count > 0:
            print(f" - Coluna '{col_name}' da tabela {table_name} da camada {camada} possui {count} valores nulos.")

    return null_counts

def count_duplicates(df: DataFrame, camada: str, table_name: str) -> int:
    """
    Conta registros duplicados. 
    """
    total_count = df.count()
    distinct_count = df.dropDuplicates().count()
    duplicate_count = total_count - distinct_count 
    if duplicate_count > 0:
        print(f"Atenção: Existem {duplicate_count} registros duplicados na tabela {table_name} da camada {camada}.")


def check_data_quality(spark: SparkSession, camada: str, table_name: str, batch_size: int = 5) -> dict:
    """
    Verifica a qualidade dos dados contando valores nulos por coluna.
    """
    df = spark.read.format("parquet").load(f"data/{camada}/{table_name}")
    count_nulls_per_column(df, camada, table_name, batch_size)
    count_duplicates(df, camada, table_name)