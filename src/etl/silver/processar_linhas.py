from typing import Callable, List
import logging
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType
from src.utils.save import save_to_silver
from src.etl.silver.validar_silver import (
    validate_input_structure,
    validate_final_structure,
)

logger = logging.getLogger(__name__)

# Constantes
LINHAS_FINAL_COLUMNS = [
    "numero_linha",
    "cod_linha_publico",
    "num_sublinha",
    "nome_bruto",
    "bairro_origem",
    "bairro_destino",
    "desc_variacao"
]

LINHAS_SOURCE_COLUMNS = ["NumeroLinha", "Linha", "Nome"]


def cast_initial_columns(df: DataFrame) -> DataFrame:
    """
    Converte e limpa as colunas iniciais de entrada.
    
    Args:
        df: DataFrame com colunas brutas NumeroLinha, Linha, Nome.
        
    Returns:
        DataFrame com colunas casting e parseadas.
    """
    return df \
        .withColumn("numero_linha", F.col("NumeroLinha").cast(IntegerType())) \
        .withColumn("cod_bruto", F.trim(F.col("Linha"))) \
        .withColumn("nome_bruto", F.trim(F.upper(F.col("Nome")))) \
        .withColumn("cod_linha_publico", F.split(F.col("cod_bruto"), "-").getItem(0)) \
        .withColumn("num_sublinha", F.coalesce(F.split(F.col("cod_bruto"), "-").getItem(1).cast(IntegerType()), F.lit(0)))


def normalize_separators(df: DataFrame) -> DataFrame:
    """
    Normaliza os separadores de itinerário para um formato unificado (barra /). 
    Converte tanto barras invertidas quanto separadores " - " para "/".
    
    Args:
        df: DataFrame com coluna 'nome_bruto' contendo itinerário.
        
    Returns:
        DataFrame com coluna array 'arr_itinerario' criada.
    """
    col_nome_normalizado = F.regexp_replace(F.col("nome_bruto"), "\\\\", "/")
    col_nome_normalizado = F.regexp_replace(col_nome_normalizado, " - ", "/")
    
    return df.withColumn("arr_itinerario", F.split(col_nome_normalizado, "/"))


def extract_itinerary_details(df: DataFrame) -> DataFrame:
    """
    Extrai bairro de origem, destino e variação a partir do itinerário parseado.
    
    Args:
        df: DataFrame com coluna array 'arr_itinerario'.
        
    Returns:
        DataFrame com colunas 'bairro_origem', 'bairro_destino', 'desc_variacao'.
    """
    return df \
        .withColumn("bairro_origem", F.trim(F.col("arr_itinerario").getItem(0))) \
        .withColumn("bairro_destino", F.trim(F.col("arr_itinerario").getItem(1))) \
        .withColumn("desc_variacao", F.coalesce(F.trim(F.col("arr_itinerario").getItem(2)), F.lit("NENHUMA")))


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
    Seleciona e ordena as colunas finais para a saída.
    
    Args:
        df: DataFrame com todas as colunas.
        columns: Lista de nomes de colunas finais desejadas.
        
    Returns:
        DataFrame com apenas colunas selecionadas.
    """
    return df.select(columns)


def process_linhas_data_pipeline(df_bronze: DataFrame) -> DataFrame:
    """
    Orquestra o pipeline de transformação de dados de linhas.
    
    Args:
        df_bronze: DataFrame de entrada (bronze)
        
    Returns:
        DataFrame processado e pronto para Silver
    """
    validate_input_structure(df_bronze, LINHAS_SOURCE_COLUMNS)
    df = cast_initial_columns(df_bronze)
    df = normalize_separators(df)
    df = extract_itinerary_details(df)
    df = remove_duplicates(df, ["numero_linha"])
    validate_final_structure(df, LINHAS_FINAL_COLUMNS)
    df = select_final_columns(df, LINHAS_FINAL_COLUMNS)
    
    return df


def process_linhas_to_silver(
    spark: SparkSession,
    bronze_path: str = "data/bronze/linhas",
    save_function: Callable[[DataFrame, str, list], None] = save_to_silver
):
    """
    Processa dados de linhas da camada Bronze para Silver.
    
    Args:
        spark: SparkSession ativa.
        bronze_path: Caminho dos dados bronze. Default: "data/bronze/linhas".
        save_function: Função para salvar dados. Default: save_to_silver.
        
    Raises:
        Exception: Se houver erro no pipeline de processamento.
    """
    try:
        logger.info("Iniciando processamento de Linhas para Silver")
        
        # Leitura
        df_bronze = spark.read.format("parquet").load(bronze_path)
        logger.info(f"Dados Bronze carregados: {df_bronze.count()} registros")
        
        # Processamento
        df_final = process_linhas_data_pipeline(df_bronze)
        logger.info(f"Pipeline Linhas processado: {df_final.count()} registros após transformações")
        
        # Escrita (sem particionamento, tabela pequena)
        save_function(df_final, "linhas", partition_cols=None)
        logger.info("Dados Linhas salvos em Silver")

    except Exception as e:
        logger.error(f"Falha no fluxo Linhas para Silver: {e}", exc_info=True)
        raise e

