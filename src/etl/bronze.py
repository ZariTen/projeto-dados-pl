import os
import requests
import shutil
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit
from src.config import LANDING_ZONE, BRONZE_DIR, URL_MCO, URL_GPS

def download_file(url: str, local_filename: str) -> str:
    """
    Baixa um arquivo da URL e salva na Landing Zone temporária.
    Retorna o caminho completo do arquivo salvo.
    """
    os.makedirs(LANDING_ZONE, exist_ok=True)
    local_path = os.path.join(LANDING_ZONE, local_filename)
    
    print(f"Iniciando download de: {url}...")
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    }

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

        # 4. Escrita (Overwrite para testes)
        output_path = os.path.join(BRONZE_DIR, "mco")
        df_bronze.write.format("parquet").mode("overwrite").save(output_path)
        
        print(f"MCO salvo na Bronze: {output_path}")
        
    except Exception as e:
        print(f"Falha no fluxo MCO: {e}")

def process_gps_to_bronze(spark: SparkSession):
    """
    Ingere dados de GPS (JSON) para a camada Bronze (Parquet).
    """
    try:
        # 1. Download
        raw_path = download_file(URL_GPS, "gps_raw.json")
        
        # 2. Leitura (JSON pode ser multiline ou line-delimited)
        # Assumindo JSON padrão de API
        df = spark.read.option("multiline", "true").json(raw_path)
        
        # 3. Metadados
        df_bronze = df.withColumn("_ingestion_timestamp", current_timestamp()) \
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
    
    # Limpeza da Landing Zone
    shutil.rmtree(LANDING_ZONE)
