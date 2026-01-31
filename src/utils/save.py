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

def save_to_silver(df: DataFrame, output_folder: str):
    """
    Salva o DataFrame na camada Silver em formato Delta Lake.
    """
    output_path = os.path.join("data/silver", output_folder)
    
    (df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(output_path)
    )