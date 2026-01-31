import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType
from src.utils.save import save_to_gold 

def run_fato_performance_diaria(spark: SparkSession):
    """
    Cria a tabela Gold 'fato_performance_linha_dia'.
    """
    try:
        # 1. Leitura da Camada Silver
        silver_path = os.path.join("data", "silver")
        df_gps = spark.read.format("delta").load(os.path.join(silver_path, "gps"))
        df_linhas = spark.read.format("delta").load(os.path.join(silver_path, "linhas"))
        df_mco = spark.read.format("delta").load(os.path.join(silver_path, "mco"))

        # O GPS original é de Julho/2020. O MCO é de Setembro/2025.
        # Adicionar 5 anos e 47 dias para alinhar as distribuições de dias da semana.
        df_gps = df_gps.withColumn(
            "data_particao_original", F.col("data_particao")
        ).withColumn(
            "data_particao", 
            F.expr("data_particao + INTERVAL 5 YEARS + INTERVAL 47 DAYS")
        )
        

        # 1. PREPARAÇÃO DE CHAVES
        
        df_gps = df_gps.withColumn("join_cod_publico", F.col("numero_linha").cast("int"))

        df_linhas = df_linhas.withColumn("join_cod_publico", F.col("cod_linha_publico").cast("int"))

        # 2. AGREGAÇÃO DO GPS 
        df_gps_clean = df_gps.filter((F.col("velocidade_kmh") >= 0) & (F.col("velocidade_kmh") < 120))

        df_gps_agg = df_gps_clean.groupBy("data_particao", "join_cod_publico").agg(
            F.round(F.avg("velocidade_kmh"), 2).alias("vel_media_gps"),
            F.max("velocidade_kmh").alias("vel_maxima_gps"),
            F.sum("distancia_percorrida").alias("distancia_total_gps_metros")
        )

        # 3. ENRIQUECIMENTO DO MCO COM DADOS DE LINHA
        
        df_mco_enriched = df_mco.join(
            df_linhas.select("numero_linha", "join_cod_publico", "cod_linha_publico", "nome_bruto", "bairro_origem", "bairro_destino"),
            on="numero_linha",
            how="left"
        )
        
        # Garantir tratamento de nulos
        if "total_usuarios" not in df_mco_enriched.columns:
             df_mco_enriched = df_mco_enriched.withColumn("total_usuarios", F.lit(0))

        df_mco_agg = df_mco_enriched.groupBy(
            "data_viagem", 
            "numero_linha", 
            "join_cod_publico", 
            "cod_linha_publico", 
            "nome_bruto", 
            "nome_consorcio",
            "bairro_origem",
            "bairro_destino"
        ).agg(
            F.count("*").alias("qtd_viagens_realizadas"),
            F.sum(F.col("teve_falha_mecanica").cast("int")).alias("qtd_falhas_mecanicas"),
            F.round(F.avg("duracao_viagem_minutos"), 2).alias("duracao_media_viagem_min"),
            F.sum("total_usuarios").alias("total_passageiros")
        )

        # 4. JOIN FINAL (MCO + GPS)

        # O join usa a data projetada do GPS
        df_gold = df_mco_agg.alias("mco").join(
            df_gps_agg.alias("gps"),
            (F.col("mco.data_viagem") == F.col("gps.data_particao")) & 
            (F.col("mco.join_cod_publico") == F.col("gps.join_cod_publico")),
            how="left"
        )

        df_final = df_gold \
            .select(
                F.col("mco.data_viagem").alias("data_referencia"),
                F.col("mco.numero_linha"), 
                F.col("mco.cod_linha_publico"),
                F.col("mco.nome_bruto").alias("nome_linha"),
                F.col("mco.nome_consorcio"),
                
                "qtd_viagens_realizadas",
                "total_passageiros",
                "qtd_falhas_mecanicas",
                "duracao_media_viagem_min",
                
                F.col("gps.vel_media_gps"),
                F.col("gps.vel_maxima_gps"),
                F.col("gps.distancia_total_gps_metros"),
                
                "bairro_origem",
                "bairro_destino"
            )

        df_final = df_final.na.fill(0, subset=["vel_media_gps", "distancia_total_gps_metros"])

        df_final.write.option("header", True).csv("data/gold/fato_performance_diaria_csv", mode="overwrite")
        save_to_gold(df_final, "fato_performance_diaria")

    except Exception as e:
        print(f"Falha no fluxo Gold: {e}")
        raise e