import os
from typing import Callable, List
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, DoubleType
from src.utils.save import save_to_silver

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
    """
    return df.filter(~F.col("value").startswith("_id"))


def parse_gps_data(df: DataFrame, columns: List[str]) -> DataFrame:
    """
    Extrai e mapeia as colunas de GPS da string bruta.
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
    """
    return df.withColumn("timestamp_gps", F.to_timestamp(F.col("HR"), hr_format))


def add_auxiliary_columns(df: DataFrame) -> DataFrame:
    """
    Adiciona colunas auxiliares para particionamento.
    """
    return df \
        .withColumn("data_particao", F.to_date(F.col("timestamp_gps"))) \
        .withColumn("hora", F.hour(F.col("timestamp_gps")))


def clean_invalid_coordinates(df: DataFrame) -> DataFrame:
    """
    Remove registros com coordenadas inválidas
    """
    return df.filter((F.col("latitude") != 0.0) & (F.col("longitude") != 0.0))


def remove_duplicates(df: DataFrame, subset: List[str]) -> DataFrame:
    """
    Remove duplicatas baseado em colunas chave.
    """
    return df.dropDuplicates(subset)


def select_final_columns(df: DataFrame, columns: List[str]) -> DataFrame:
    """
    Seleciona e ordena as colunas finais.
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
    df = filter_raw_data(df_bronze)
    df = parse_gps_data(df, gps_columns)
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
    save_function: Callable[[DataFrame, str], None] = save_to_silver
):
    """
    Processa dados de GPS
    
    Args:
        spark: SparkSession
        bronze_path: Caminho dos dados bronze
        save_function: Função para salvar dados
    """
    try:
        # Leitura
        df_bronze = spark.read.format("parquet").load(bronze_path)
        
        # Processamento
        df_final = process_gps_data_pipeline(df_bronze, GPS_COLUMNS)
        
        # Escrita
        save_function(df_final, "gps")

    except Exception as e:
        print(f"Falha no fluxo GPS para Silver: {e}")
        raise e
