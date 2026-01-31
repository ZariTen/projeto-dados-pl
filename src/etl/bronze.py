import os
import shutil
import requests
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from fake_useragent import UserAgent
from src.config import LANDING_ZONE, BRONZE_DIR, URL_MCO, URL_GPS, URL_LINHAS


def download_file(url: str, local_filename: str) -> str:
    """
    Baixa um arquivo da URL e salva na Landing Zone temporária.
    
    Args:
        url (str): Endereço do arquivo.
        local_filename (str): Nome do arquivo no disco.

    Returns:
        str: Caminho completo do arquivo salvo.
    """
    os.makedirs(LANDING_ZONE, exist_ok=True)
    local_path = os.path.join(LANDING_ZONE, local_filename)

    if os.path.exists(local_path):
        return local_path
    
    ua = UserAgent()
    headers = {'User-Agent': ua.random}

    try:
        with requests.get(url, headers=headers, stream=True) as r:
            r.raise_for_status()
            with open(local_path, 'wb') as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)
        return local_path
    except Exception as e:
        print(f"Erro ao baixar o arquivo: {e}")
        raise e

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

def save_to_bronze(df: DataFrame, source_filename: str, output_folder: str):
    """
    Adiciona metadados de ingestão e salva o DataFrame na camada Bronze em formato Parquet.
    """
    df_final = df.withColumn("_ingestion_timestamp", F.current_timestamp()) \
                 .withColumn("_source_file", F.lit(source_filename))
    
    output_path = os.path.join(BRONZE_DIR, output_folder)
    
    df_final.write.format("parquet").mode("overwrite").save(output_path)

def process_mco_to_bronze(spark: SparkSession):
    """Ingere dados do MCO (CSV) para a camada Bronze."""
    try:
        raw_path = download_file(URL_MCO, "mco_raw.csv")
        
        df = spark.read.format("csv") \
            .option("header", "true") \
            .option("delimiter", ";") \
            .option("inferSchema", "true") \
            .load(raw_path)

        df_sanitized = sanitize_columns(df)
        save_to_bronze(df_sanitized, "mco_consolidado.csv", "mco")
        
    except Exception as e:
        print(f"Falha no fluxo MCO: {e}")
        raise e

def process_linhas_to_bronze(spark: SparkSession):
    """Ingere tabela de conversão de linhas (CSV) para a camada Bronze."""
    try:
        raw_path = download_file(URL_LINHAS, "linhas_sistema.csv")

        df = spark.read.format("csv") \
            .option("header", "true") \
            .option("delimiter", ",") \
            .option("inferSchema", "true") \
            .load(raw_path)
        
        save_to_bronze(df, "linhas_sistema.csv", "linhas")
        
    except Exception as e:
        print(f"Falha no fluxo Linhas: {e}")
        raise e

def process_gps_to_bronze(spark: SparkSession):
    """Ingere dados de GPS (Raw Text/CSV misto) para a camada Bronze."""
    try:
        raw_path = download_file(URL_GPS, "gps_raw.csv")
        
        # Leitura como texto devido à formatação irregular
        df_raw = spark.read.text(raw_path)
        df_filtered = df_raw.filter(~F.col("value").startswith("_id"))

        # Lógica de Parse
        df_parsed = df_filtered.withColumn(
            "data_part", F.split(F.col("value"), ",", 2).getItem(1)
        ).withColumn(
            "cols", F.split(F.col("data_part"), ";")
        )

        # Mapeamento limpo de colunas
        gps_columns = ["EV", "HR", "LT", "LG", "NV", "VL", "NL", "DG", "SV", "DT"]
        
        # Regex de seleção
        select_exprs = [
            F.col("cols").getItem(i).alias(col_name) 
            for i, col_name in enumerate(gps_columns)
        ]

        df_final = df_parsed.select(*select_exprs).filter(F.col("EV").isNotNull())

        save_to_bronze(df_final, "tempo_real_gps.json", "gps")

    except Exception as e:
        print(f"Falha no fluxo GPS: {e}")
        raise e

def run_bronze_layer(spark: SparkSession):
    """Orquestrador da camada Bronze."""
    try:
        process_mco_to_bronze(spark)
        process_gps_to_bronze(spark)
        process_linhas_to_bronze(spark)
    except Exception as e:
        raise
    finally:
        if os.path.exists(LANDING_ZONE):
            shutil.rmtree(LANDING_ZONE)