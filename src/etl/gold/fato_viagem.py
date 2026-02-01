import os
from typing import Callable, List
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType
from src.utils.save import save_to_gold

# Constantes
FATO_FINAL_COLUMNS = [
    "data_referencia",
    "numero_linha",
    "cod_linha_publico",
    "nome_linha",
    "nome_consorcio",
    "qtd_viagens_realizadas",
    "qtd_falhas_mecanicas",
    "duracao_media_viagem_min",
    "vel_media_gps",
    "vel_maxima_gps",
    "distancia_total_gps_metros",
    "bairro_origem",
    "bairro_destino"
]

def prepare_gps_metrics(df_gps: DataFrame) -> DataFrame:
    """
    Limpa, prepara chaves e agrega os dados de GPS.
    """
    df_clean = df_gps \
        .withColumn("join_cod_publico", F.col("numero_linha").cast("int")) \
        .filter((F.col("velocidade_kmh") >= 0) & (F.col("velocidade_kmh") < 120))

    return df_clean.groupBy("data_particao", "join_cod_publico").agg(
        F.round(F.avg("velocidade_kmh"), 2).alias("vel_media_gps"),
        F.max("velocidade_kmh").alias("vel_maxima_gps"),
        F.sum("distancia_percorrida").alias("distancia_total_gps_metros")
    )


def prepare_linhas_lookup(df_linhas: DataFrame) -> DataFrame:
    """
    Prepara a tabela de linhas para enriquecimento (cria chave de join e seleciona colunas).
    """
    return df_linhas \
        .withColumn("join_cod_publico", F.col("cod_linha_publico").cast("int")) \
        .select(
            "numero_linha", 
            "join_cod_publico", 
            "cod_linha_publico", 
            "nome_bruto", 
            "bairro_origem", 
            "bairro_destino"
        )


def enrich_and_aggregate_mco(df_mco: DataFrame, df_linhas: DataFrame) -> DataFrame:
    """
    Realiza o join do MCO com dados de Linha e agrega os indicadores de viagem.
    """
    # Join MCO + Linhas
    df_enriched = df_mco.join(df_linhas, on="numero_linha", how="left")
    
    # Agregação
    return df_enriched.groupBy(
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
        F.round(F.avg("duracao_viagem_minutos"), 2).alias("duracao_media_viagem_min")
    )


def join_mco_gps_and_finalize(df_mco_agg: DataFrame, df_gps_agg: DataFrame) -> DataFrame:
    """
    Unifica os dados agregados de MCO e GPS, trata nulos e seleciona colunas finais.
    """
    # Join Left usando Alias para clareza
    df_gold = df_mco_agg.alias("mco").join(
        df_gps_agg.alias("gps"),
        (F.col("mco.data_viagem") == F.col("gps.data_particao")) & 
        (F.col("mco.join_cod_publico") == F.col("gps.join_cod_publico")),
        how="left"
    )

    # Seleção e Renomeação
    df_final = df_gold.select(
        F.col("mco.data_viagem").alias("data_referencia"),
        F.col("mco.numero_linha"), 
        F.col("mco.cod_linha_publico"),
        F.col("mco.nome_bruto").alias("nome_linha"),
        F.col("mco.nome_consorcio"),
        
        "qtd_viagens_realizadas",
        "qtd_falhas_mecanicas",
        "duracao_media_viagem_min",
        
        F.col("gps.vel_media_gps"),
        F.col("gps.vel_maxima_gps"),
        F.col("gps.distancia_total_gps_metros"),
        
        "bairro_origem",
        "bairro_destino"
    )

    # Tratamento de Nulos
    return df_final.na.fill(0, subset=["vel_media_gps", "distancia_total_gps_metros"])


def process_fato_performance_pipeline(
    df_gps: DataFrame, 
    df_linhas: DataFrame, 
    df_mco: DataFrame
) -> DataFrame:
    """
    Orquestra o pipeline de transformação da Fato Performance Diária.
    
    Args:
        df_gps: DataFrame Silver de GPS
        df_linhas: DataFrame Silver de Linhas
        df_mco: DataFrame Silver de MCO
        
    Returns:
        DataFrame Gold pronto
    """
    # 1. Preparação das fontes
    df_gps_agg = prepare_gps_metrics(df_gps)
    df_linhas_lookup = prepare_linhas_lookup(df_linhas)
    
    # 2. Processamento MCO
    df_mco_agg = enrich_and_aggregate_mco(df_mco, df_linhas_lookup)
    
    # 3. Unificação Final
    df_final = join_mco_gps_and_finalize(df_mco_agg, df_gps_agg)
    
    return df_final


def run_fato_performance_diaria(
    spark: SparkSession, 
    silver_base_path: str = "data/silver",
    save_function: Callable[[DataFrame, str], None] = save_to_gold
):
    """
    Executa o fluxo completo da Fato Performance Diária (Leitura -> Pipeline -> Escrita).
    """
    try:
        # Leitura
        df_gps = spark.read.format("delta").load(os.path.join(silver_base_path, "gps"))
        df_linhas = spark.read.format("delta").load(os.path.join(silver_base_path, "linhas"))
        df_mco = spark.read.format("delta").load(os.path.join(silver_base_path, "mco"))

        # Processamento
        df_final = process_fato_performance_pipeline(df_gps, df_linhas, df_mco)

        df_final.write.option("header", True).csv("data/gold/fato_performance_diaria_csv", mode="overwrite")
        
        save_function(df_final, "fato_performance_diaria")

    except Exception as e:
        print(f"Falha no fluxo Fato Performance Diária: {e}")
        raise e
