import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import current_timestamp, lit, col, regexp_replace

def process_gps_to_silver(spark: SparkSession):
    """
    Processa os dados de GPS da camada Bronze para a camada Silver.
    """
    try:
        # 1. Leitura
        bronze_path = os.path.join("data/bronze", "gps")
        df_bronze = spark.read.format("parquet").load(bronze_path)

        # 2. Transformações
        df_silver = df_bronze.withColumn("numero_veiculo", F.col("NV").cast("integer")) \
                             .withColumn("latitude", regexp_replace(F.col("LT"), ",", ".").cast("double")) \
                             .withColumn("longitude", regexp_replace(F.col("LG"), ",", ".").cast("double")) \
                             .withColumn("timestamp", F.to_timestamp(F.col("HR"), "yyyyMMddHHmmss")) \
                             .withColumn("data_dia", F.date_format(F.col("timestamp"), "yyyy-MM-dd")) \
                             .withColumn("hora_dia", F.date_format(F.col("timestamp"), "HH:mm:ss")) \
                             .withColumn("velocidade", F.col("VL").cast("integer")) \
                             .withColumn("numero_linha", F.col("NL").cast("integer")) \
                             .withColumn("direcao", F.col("DG").cast("integer")) \
                             .withColumn("caminho", F.when(F.col("SV") == 1, "IDA").otherwise("VOLTA")) \
                             .withColumn("distancia_percorrida", F.col("DT").cast("integer")) \
                             .drop("LT", "LG", "HR", "timestamp", "SV", "_ingestion_timestamp", "_source_file", "NV", "VL", "NL", "DG", "DT", "EV")

        # 3. Escrita formato Delta Lake
        silver_path = os.path.join("data/silver", "gps")
        df_silver.write.format("delta").mode("overwrite").save(silver_path)

        print(f"GPS salvo na Silver: {silver_path}")

    except Exception as e:
        print(f"Falha no fluxo GPS para Silver: {e}")

def process_bronze_to_silver(spark: SparkSession):
    """
    Processa todos os dados da camada Bronze para a camada Silver.
    """
    process_gps_to_silver(spark)

def run_silver_layer(spark):
    """
    Executa o processo ETL para a camada Silver.
    """
    process_bronze_to_silver(spark)