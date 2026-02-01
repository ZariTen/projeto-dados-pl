import os
from pyspark.sql import functions as F
from pyspark.sql import DataFrame
from src.config import BRONZE_DIR

def save_to_bronze(df: DataFrame, source_filename: str, output_folder: str):
    """
    Adiciona metadados de ingestão e salva o DataFrame na camada Bronze em formato Parquet.
    """
    df_final = df.withColumn("_ingestion_timestamp", F.current_timestamp()) \
                 .withColumn("_source_file", F.lit(source_filename))
    
    output_path = os.path.join(BRONZE_DIR, output_folder)
    
    (df_final.write
        .format("parquet")
        .mode("overwrite")
        .save(output_path)
    )

def save_to_silver(df: DataFrame, output_folder: str, partition_cols: list = None):
    """
    Salva o DataFrame na camada Silver em formato Delta Lake com particionamento opcional.
    
    Args:
        df: DataFrame a ser salvo.
        output_folder: Nome da pasta de destino em data/silver.
        partition_cols: Lista de colunas para particionamento. Se None, sem particionamento.
    """
    output_path = os.path.join("data/silver", output_folder)
    
    writer = (df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true"))
    
    if partition_cols:
        writer = writer.partitionBy(*partition_cols)
    
    writer.save(output_path)

def save_to_gold(df: DataFrame, output_folder: str, partition_cols: list = None):
    """
    Salva o DataFrame na camada Gold em formato Delta Lake com particionamento opcional.
    
    Args:
        df: DataFrame a ser salvo.
        output_folder: Nome da pasta de destino em data/gold.
        partition_cols: Lista de colunas para particionamento. Se None, sem particionamento.
    """
    output_path = os.path.join("data/gold", output_folder)
    
    writer = (df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true"))
    
    if partition_cols:
        writer = writer.partitionBy(*partition_cols)
    
    writer.save(output_path)