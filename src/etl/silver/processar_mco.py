from typing import Callable, List
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, LongType
from src.utils.save import save_to_silver
from src.utils.quality import sanitize_columns
from src.etl.silver.validar_silver import (
    validate_input_structure,
    validate_final_structure,
)

# Constantes
MCO_COLS_TO_CHECK = ["ocorrencia", "justificativa", "falha_mecanica", "evento_inseguro"]
MCO_COLS_TO_DROP = ["viagem", "data_fechamento", "extensao",
                    "concessionaria", "pc", "falha_mecanica", "evento_inseguro",
                    "saida", "chegada", "_ingestion_timestamp", "_source_file"]
MCO_SOURCE_COLUMNS = [
    "viagem", "data_fechamento", "extensao", "sublinha", "pc",
    "catraca_saida", "catraca_chegada", "saida", "chegada",
    "concessionaria", "falha_mecanica", "evento_inseguro",
    "ocorrencia", "justificativa"
]
MCO_FINAL_REQUIRED_COLUMNS = [
    "data_viagem", "dh_fechamento", "extensao_metros", "numero_linha",
    "catraca_saida", "catraca_chegada", "ts_saida", "ts_chegada",
    "duracao_viagem_minutos", "nome_consorcio", "desc_tipo_viagem",
    "teve_falha_mecanica", "teve_evento_inseguro", "ano", "mes",
    "ocorrencia", "justificativa"
]


def sanitize_and_cast_columns(df: DataFrame) -> DataFrame:
    """
    Sanitiza colunas e realiza casting de tipos.
    """
    df = sanitize_columns(df)
    
    return df.withColumn("data_viagem", F.to_date(F.col("viagem"), "dd/MM/yyyy")) \
        .withColumn("dh_fechamento", F.to_timestamp(F.col("data_fechamento"), "dd/MM/yyyy HH:mm")) \
        .withColumn("extensao_metros", F.col("extensao").cast(IntegerType())) \
        .withColumn("numero_linha", F.col("sublinha").cast(IntegerType())) \
        .withColumn("pc", F.col("pc").cast(IntegerType())) \
        .withColumn("catraca_saida", F.col("catraca_saida").cast(LongType())) \
        .withColumn("catraca_chegada", F.col("catraca_chegada").cast(LongType()))


def clean_empty_strings(df: DataFrame, cols: List[str]) -> DataFrame:
    """
    Converte strings vazias para nulos em colunas especificadas.
    """
    for col_name in cols:
        df = df.withColumn(col_name, F.when(F.col(col_name) == "", None).otherwise(F.col(col_name)))
    
    return df


def create_timestamps(df: DataFrame) -> DataFrame:
    """
    Cria timestamps de saída e chegada, ajustando para dia seguinte quando necessário.
    """
    df = df.withColumn("ts_saida", F.to_timestamp(F.concat(F.col("data_viagem"), F.lit(" "), F.col("saida")), "yyyy-MM-dd HH:mm")) \
        .withColumn("ts_chegada_temp", F.to_timestamp(F.concat(F.col("data_viagem"), F.lit(" "), F.col("chegada")), "yyyy-MM-dd HH:mm"))
    
    return df.withColumn("ts_chegada", 
        F.when(F.col("ts_chegada_temp") < F.col("ts_saida"), F.col("ts_chegada_temp") + F.expr("INTERVAL 1 DAY"))
         .otherwise(F.col("ts_chegada_temp"))
    ).drop("ts_chegada_temp", "hora_saida", "hora_chegada")


def calculate_trip_duration(df: DataFrame) -> DataFrame:
    """
    Calcula a duração da viagem em minutos.
    """
    return df.withColumn("duracao_viagem_minutos", 
        F.round((F.col("ts_chegada").cast("long") - F.col("ts_saida").cast("long")) / 60, 2)
    )


def map_consortium_names(df: DataFrame) -> DataFrame:
    """
    Mapeia códigos de concessionária para nomes de consórcio.
    """
    return df.withColumn("nome_consorcio", 
        F.when(F.col("concessionaria") == "801", "Consórcio Pampulha")
         .when(F.col("concessionaria") == "802", "Consórcio BHLeste")
         .when(F.col("concessionaria") == "803", "Consórcio Dez")
         .when(F.col("concessionaria") == "804", "Consórcio Dom Pedro II")
         .otherwise("Desconhecido")
    )


def map_trip_types(df: DataFrame) -> DataFrame:
    """
    Mapeia códigos de PC para descrição de tipo de viagem.
    """
    return df.withColumn("desc_tipo_viagem",
        F.when(F.col("pc") == 0, "Ociosa")
         .when(F.col("pc").isin(1, 2), "Normal")
         .when(F.col("pc") == 3, "Transferencia")
         .otherwise("Outros")
    )


def create_boolean_columns(df: DataFrame) -> DataFrame:
    """
    Cria colunas booleanas a partir de colunas de falha e evento.
    """
    return df \
        .withColumn("teve_falha_mecanica", F.coalesce(F.col("falha_mecanica").cast(IntegerType()), F.lit(0)) == 1) \
        .withColumn("teve_evento_inseguro", F.coalesce(F.col("evento_inseguro").cast(IntegerType()), F.lit(0)) == 1)


def add_partitioning_columns(df: DataFrame) -> DataFrame:
    """
    Adiciona colunas de ano e mês para particionamento.
    """
    return df.withColumn("ano", F.year("data_viagem")) \
        .withColumn("mes", F.month("data_viagem"))


def drop_unnecessary_columns(df: DataFrame, cols_to_drop: List[str]) -> DataFrame:
    """
    Remove colunas desnecessárias, incluindo coluna vazia se existir.
    """
    cols_to_remove = cols_to_drop.copy()
    
    if "" in df.columns:
        cols_to_remove.append("")
    
    return df.drop(*cols_to_remove)


def process_mco_data_pipeline(df_bronze: DataFrame) -> DataFrame:
    """
    Orquestra o pipeline de transformação de dados MCO.
    
    Args:
        df_bronze: DataFrame de entrada (bronze)
        
    Returns:
        DataFrame processado e pronto para Silver
    """
    df = sanitize_and_cast_columns(df_bronze)
    validate_input_structure(df, MCO_SOURCE_COLUMNS)
    df = clean_empty_strings(df, MCO_COLS_TO_CHECK)
    df = create_timestamps(df)
    df = calculate_trip_duration(df)
    df = map_consortium_names(df)
    df = map_trip_types(df)
    df = create_boolean_columns(df)
    df = add_partitioning_columns(df)
    df = drop_unnecessary_columns(df, MCO_COLS_TO_DROP)
    validate_final_structure(df, MCO_FINAL_REQUIRED_COLUMNS)
    
    return df


def process_mco_to_silver(
    spark: SparkSession,
    bronze_path: str = "data/bronze/mco",
    save_function: Callable[[DataFrame, str], None] = save_to_silver
):
    """
    Processa dados MCO da camada Bronze para a camada Silver.
    
    Args:
        spark: SparkSession
        bronze_path: Caminho dos dados bronze
        save_function: Função para salvar dados
    """
    try:
        # Leitura
        df_bronze = spark.read.format("parquet").load(bronze_path)
        
        # Processamento
        df_final = process_mco_data_pipeline(df_bronze)
        
        # Escrita
        save_function(df_final, "mco")

    except Exception as e:
        print(f"Falha no fluxo MCO para Silver: {e}")
        raise e

