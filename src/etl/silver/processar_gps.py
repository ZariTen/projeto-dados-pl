import os
import logging
from typing import Callable, List
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, DoubleType
from src.utils.save import save_to_silver
from src.etl.silver.validar_silver import (
    validate_input_structure,
    validate_parsed_structure,
)

logger = logging.getLogger(__name__)

# Constantes
GPS_COLUMNS = ["EV", "HR", "LT", "LG", "NV", "VL", "NL", "DG", "SV", "DT"]
GPS_FINAL_COLUMNS = [
    "timestamp_gps", "numero_veiculo", "numero_linha", "sentido_viagem",
    "latitude", "longitude", "velocidade_kmh", "distancia_percorrida",
    "direcao_graus", "cod_evento", "data_particao", "hora"
]


def filter_raw_data(df: DataFrame) -> DataFrame:
    """
    Remove registros com identificadores inválidos.
    
    Args:
        df: DataFrame de entrada com coluna 'value'.
        
    Returns:
        DataFrame filtrado sem registros de metadados.
    """
    return df.filter(~F.col("value").startswith("_id"))


def parse_gps_data(df: DataFrame, columns: List[str]) -> DataFrame:
    """
    Extrai e mapeia as colunas de GPS da string bruta.
    
    Args:
        df: DataFrame com coluna 'value' contendo dados GPS em formato separado.
        columns: Lista de nomes de colunas esperadas.
        
    Returns:
        DataFrame com colunas extraídas e nomeadas.
    """
    df_parsed = df.withColumn(
        "data_part", F.split(F.col("value"), ",", 2).getItem(1)
    ).withColumn(
        "cols", F.split(F.col("data_part"), ";")
    )
    
    select_exprs = [
        F.col("cols").getItem(i).alias(col_name) 
        for i, col_name in enumerate(columns)
    ]
    
    return df_parsed.select(*select_exprs).filter(F.col("EV").isNotNull())


def cast_columns_to_types(df: DataFrame) -> DataFrame:
    """
    Converte colunas para tipos apropriados.
    
    Args:
        df: DataFrame com colunas em tipo string.
        
    Returns:
        DataFrame com tipos casting corretos.
    """
    return df \
        .withColumn("numero_veiculo", F.col("NV").cast(IntegerType())) \
        .withColumn("cod_evento", F.col("EV").cast(IntegerType())) \
        .withColumn("latitude", F.regexp_replace(F.col("LT"), ",", ".").cast(DoubleType())) \
        .withColumn("longitude", F.regexp_replace(F.col("LG"), ",", ".").cast(DoubleType())) \
        .withColumn("velocidade_kmh", F.col("VL").cast(IntegerType())) \
        .withColumn("numero_linha", F.col("NL").cast(IntegerType())) \
        .withColumn("direcao_graus", F.col("DG").cast(IntegerType())) \
        .withColumn("distancia_percorrida", F.col("DT").cast(IntegerType()))


def transform_sentido_viagem(df: DataFrame) -> DataFrame:
    """
    Converte código de sentido para descrição.
    
    Args:
        df: DataFrame com coluna 'SV' contendo códigos de sentido.
        
    Returns:
        DataFrame com coluna 'sentido_viagem' mapeada.
    """
    return df.withColumn(
        "sentido_viagem", 
        F.when(F.col("SV") == "1", "IDA")
         .when(F.col("SV") == "2", "VOLTA")
         .otherwise("INDEFINIDO")
    )


def add_timestamp(df: DataFrame, hr_format: str = "yyyyMMddHHmmss") -> DataFrame:
    """
    Cria coluna de timestamp a partir do horário.
    
    Args:
        df: DataFrame com coluna 'HR' em formato string.
        hr_format: Formato esperado do timestamp. Default: "yyyyMMddHHmmss".
        
    Returns:
        DataFrame com coluna 'timestamp_gps' adicionada.
    """
    return df.withColumn("timestamp_gps", F.to_timestamp(F.col("HR"), hr_format))


def add_auxiliary_columns(df: DataFrame) -> DataFrame:
    """
    Adiciona colunas auxiliares para particionamento e análise.
    
    Args:
        df: DataFrame com coluna 'timestamp_gps'.
        
    Returns:
        DataFrame com colunas 'data_particao' e 'hora' adicionadas.
    """
    return df \
        .withColumn("data_particao", F.to_date(F.col("timestamp_gps"))) \
        .withColumn("hora", F.hour(F.col("timestamp_gps")))


def clean_invalid_coordinates(df: DataFrame) -> DataFrame:
    """
    Remove registros com coordenadas inválidas (latitude/longitude zero).
    
    Args:
        df: DataFrame com colunas 'latitude' e 'longitude'.
        
    Returns:
        DataFrame sem registros de coordenadas inválidas.
    """
    return df.filter((F.col("latitude") != 0.0) & (F.col("longitude") != 0.0))


def remove_duplicates(df: DataFrame, subset: List[str]) -> DataFrame:
    """
    Remove duplicatas baseado em colunas chave.
    
    Args:
        df: DataFrame de entrada.
        subset: Lista de colunas para considerar na deduplicação.
        
    Returns:
        DataFrame sem duplicatas.
    """
    return df.dropDuplicates(subset)


def select_final_columns(df: DataFrame, columns: List[str]) -> DataFrame:
    """
    Seleciona e ordena as colunas finais.
    
    Args:
        df: DataFrame com todas as colunas.
        columns: Lista de nomes de colunas finais desejadas.
        
    Returns:
        DataFrame com apenas colunas selecionadas na ordem especificada.
    """
    return df.select(columns)


def process_gps_data_pipeline(df_bronze: DataFrame, gps_columns: List[str] = GPS_COLUMNS) -> DataFrame:
    """
    Orquestra o pipeline de transformação de dados GPS.
    
    Args:
        df_bronze: DataFrame de entrada (bronze)
        gps_columns: Lista de nomes de colunas GPS
        
    Returns:
        DataFrame processado e pronto para Silver
    """
    validate_input_structure(df_bronze, ["value"])
    df = filter_raw_data(df_bronze)
    df = parse_gps_data(df, gps_columns)
    validate_parsed_structure(df, gps_columns)
    df = cast_columns_to_types(df)
    df = transform_sentido_viagem(df)
    df = add_timestamp(df)
    df = add_auxiliary_columns(df)
    df = clean_invalid_coordinates(df)
    df = remove_duplicates(df, ["numero_veiculo", "timestamp_gps"])
    df = select_final_columns(df, GPS_FINAL_COLUMNS)
    
    return df


def process_gps_to_silver(
    spark: SparkSession,
    bronze_path: str = "data/bronze/gps",
    save_function: Callable[[DataFrame, str, list], None] = save_to_silver
):
    """
    Processa dados de GPS da camada Bronze para Silver.
    
    Args:
        spark: SparkSession ativa.
        bronze_path: Caminho dos dados bronze. Default: "data/bronze/gps".
        save_function: Função para salvar dados em Silver. Default: save_to_silver.
        
    Raises:
        Exception: Se houver erro no pipeline de processamento.
    """
    try:
        logger.info("Iniciando processamento de GPS para Silver")
        
        # Leitura
        df_bronze = spark.read.format("parquet").load(bronze_path)
        logger.info(f"Dados Bronze carregados: {df_bronze.count()} registros")
        
        # Processamento
        df_final = process_gps_data_pipeline(df_bronze, GPS_COLUMNS)
        logger.info(f"Pipeline GPS processado: {df_final.count()} registros após transformações")
        
        # Escrita com particionamento
        save_function(df_final, "gps", partition_cols=["data_particao"])
        logger.info("Dados GPS salvos em Silver com particionamento por data_particao")

    except Exception as e:
        logger.error(f"Falha no fluxo GPS para Silver: {e}", exc_info=True)
        raise e