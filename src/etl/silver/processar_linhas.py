import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType
from src.utils.save import save_to_silver

def process_linhas_to_silver(spark: SparkSession):
    """
    Processa os dados da tabela de linhas da camada Bronze para a camada Silver.
    """
    try:
        # 1. Leitura
        bronze_path = os.path.join("data/bronze", "linhas")
        df_bronze = spark.read.format("parquet").load(bronze_path)

        df_bronze.show(5, truncate=False)

        # 2. Tratamento
        df_silver = df_bronze \
            .withColumn("numero_linha", F.col("NumeroLinha").cast(IntegerType())) \
            .withColumn("cod_bruto", F.trim(F.col("Linha"))) \
            .withColumn("nome_bruto", F.trim(F.upper(F.col("Nome")))) \
            .withColumn("cod_linha_publico", F.split(F.col("cod_bruto"), "-").getItem(0)) \
            .withColumn("num_sublinha", F.coalesce(F.split(F.col("cod_bruto"), "-").getItem(1).cast(IntegerType()), F.lit(0)))

        # 3. Lógica
        
        # Passo A: Normalização dos Separadores
        col_nome_normalizado = F.regexp_replace(F.col("nome_bruto"), "\\\\", "/")
        col_nome_normalizado = F.regexp_replace(col_nome_normalizado, " - ", "/") 
        
        # Passo B: Quebra em Array baseada no separador unificado "/"
        df_silver = df_silver.withColumn("arr_itinerario", F.split(col_nome_normalizado, "/"))
        
        # Passo C: Extração Posicional
        df_silver = df_silver \
            .withColumn("bairro_origem", F.trim(F.col("arr_itinerario").getItem(0))) \
            .withColumn("bairro_destino", F.trim(F.col("arr_itinerario").getItem(1))) \
            .withColumn("desc_variacao", F.coalesce(F.trim(F.col("arr_itinerario").getItem(2)), F.lit("NENHUMA")))

        cols_final = [
            "numero_linha",
            "cod_linha_publico",
            "num_sublinha",
            "nome_bruto",
            "bairro_origem",
            "bairro_destino",
            "desc_variacao"
        ]
        
        df_silver = df_silver.select(cols_final)
        df_silver = df_silver.dropDuplicates(["numero_linha"])

        # 5. Escrita
        save_to_silver(df_silver, "linhas")

    except Exception as e:
        print(f"Falha no fluxo Linhas para Silver: {e}")
        raise e
