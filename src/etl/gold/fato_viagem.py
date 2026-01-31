import os 
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType

def run_fato_viagem(spark: SparkSession):
    """
    Processa dados de viagem para a camada Gold.
    """
    try:
        # 1. Leitura dos dados Silver
        silver_path = os.path.join("data", "silver")
        df_gps = spark.read.format("delta").load(os.path.join(silver_path, "gps"))
        df_linhas = spark.read.format("delta").load(os.path.join(silver_path, "linhas"))
        df_mco = spark.read.format("delta").load(os.path.join(silver_path, "mco"))

        # 2. Join GPS e Linhas por número da linha
        df_viagem = df_gps.join(
            df_linhas,
            df_gps.numero_linha == df_linhas.id_linha_interno,
            how="left"
        )


    except Exception as e:
        print(f"Falha no fluxo Fato Viagem para Gold: {e}")
        raise e
    