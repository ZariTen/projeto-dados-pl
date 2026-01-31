import os
import requests
import shutil
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import current_timestamp, lit, col
from src.config import LANDING_ZONE, BRONZE_DIR, URL_MCO, URL_GPS, URL_LINHAS
from fake_useragent import UserAgent

def download_file(url: str, local_filename: str) -> str:
    """
    Baixa um arquivo da URL e salva na Landing Zone temporária.
    Retorna o caminho completo do arquivo salvo.
    """
    os.makedirs(LANDING_ZONE, exist_ok=True)
    local_path = os.path.join(LANDING_ZONE, local_filename)

    if os.path.exists(local_path):
        print(f"Arquivo já existe: {local_path}")
        return local_path
    
    print(f"Iniciando download de: {url}...")

    ua = UserAgent()
    headers = {'User-Agent': ua.random}
    

    try:
        with requests.get(url, headers=headers, stream=True) as r:
            r.raise_for_status()
            with open(local_path, 'wb') as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)
        print(f"Download concluído: {local_path}")
        return local_path
    except Exception as e:
        print(f"Erro ao baixar {url}: {e}")
        raise

def process_mco_to_bronze(spark: SparkSession):
    """
    Ingere dados do MCO (CSV) para a camada Bronze (Parquet).
    """
    try:
        # 1. Download
        raw_path = download_file(URL_MCO, "mco_raw.csv")
        
        # 2. Leitura
        df = spark.read.format("csv") \
            .option("header", "true") \
            .option("delimiter", ";") \
            .option("inferSchema", "true") \
            .load(raw_path)

        # 3. Metadados
        df_bronze = df.withColumn("_ingestion_timestamp", current_timestamp()) \
                      .withColumn("_source_file", lit("mco_consolidado.csv"))

        # Remover espacos e deixar minusculo nos nomes das colunas
        for c in df_bronze.columns:
            df_bronze = df_bronze.withColumnRenamed(c, c.strip().lower())

        # Adicionar underscore nas colunas com espaços
        for c in df_bronze.columns:
            new_c = c.replace(" ", "_")
            df_bronze = df_bronze.withColumnRenamed(c, new_c)

        # 4. Escrita (Overwrite para testes)
        output_path = os.path.join(BRONZE_DIR, "mco")
        df_bronze.write.format("parquet").mode("overwrite").save(output_path)
        
        print(f"MCO salvo na Bronze: {output_path}")
        
    except Exception as e:
        print(f"Falha no fluxo MCO: {e}")

def process_linhas_to_bronze(spark: SparkSession):
    """
    Ingere a tabela de conversão de linhas (CSV) para a camada Bronze.
    """
    try:
        # 1. Download 
        raw_path = download_file(URL_LINHAS, "linhas_sistema.csv")

        # 2. Leitura
        df = spark.read.format("csv") \
            .option("header", "true") \
            .option("delimiter", ",") \
            .option("inferSchema", "true") \
            .load(raw_path)
        
        # 3. Metadados
        df_bronze = df.withColumn("_ingestion_timestamp", current_timestamp()) \
                            .withColumn("_source_file", lit("linhas_sistema.csv"))

        # 4. Escrita
        output_path = os.path.join(BRONZE_DIR, "linhas")
        df_bronze.write.format("parquet").mode("overwrite").save(output_path)
        
        print(f"Linhas salvo na Bronze: {output_path}")
    except Exception as e:
        print(f"Falha no fluxo Linhas: {e}")

def process_gps_to_bronze(spark: SparkSession):
    """
    Ingere dados de GPS (JSON) para a camada Bronze (Parquet).
    """
    try:
        # 1. Download
        raw_path = download_file(URL_GPS, "gps_raw.csv")
        
        # 2. Leitura
        df_raw = spark.read.text(raw_path)
        df_filtered = df_raw.filter(~F.col("value").startswith("_id"))

        # 3. Ajuste de dados CSV
        df_step1 = df_filtered.withColumn("data_part", F.split(F.col("value"), ",", 2).getItem(1)) \
                 .withColumn("cols", F.split(F.col("data_part"), ";"))

        df_final = df_step1.select(
            F.col("cols").getItem(0).alias("EV"),
            F.col("cols").getItem(1).alias("HR"),
            F.col("cols").getItem(2).alias("LT"),
            F.col("cols").getItem(3).alias("LG"),
            F.col("cols").getItem(4).alias("NV"),
            F.col("cols").getItem(5).alias("VL"),
            F.col("cols").getItem(6).alias("NL"),
            F.col("cols").getItem(7).alias("DG"),
            F.col("cols").getItem(8).alias("SV"),
            F.col("cols").getItem(9).alias("DT")
        )

        df_final = df_final.filter(F.col("EV").isNotNull())

        # 3. Metadados
        df_bronze = df_final.withColumn("_ingestion_timestamp", current_timestamp()) \
                      .withColumn("_source_file", lit("tempo_real_gps.json"))

        # 4. Escrita
        output_path = os.path.join(BRONZE_DIR, "gps")
        df_bronze.write.format("parquet").mode("overwrite").save(output_path)
        
        print(f"GPS salvo na Bronze: {output_path}")

    except Exception as e:
        print(f"Falha no fluxo GPS: {e}")

def run_bronze_layer(spark: SparkSession):
    print("Iniciando Camada Bronze...")
    process_mco_to_bronze(spark)
    process_gps_to_bronze(spark)
    process_linhas_to_bronze(spark)
    shutil.rmtree(LANDING_ZONE)
