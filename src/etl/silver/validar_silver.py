from typing import List
from pyspark.sql import DataFrame


def validate_input_structure(df: DataFrame, expected_columns: List[str]) -> None:
    """
    Valida se a estrutura mínima do DataFrame bronze está correta.
    """
    missing = [col for col in expected_columns if col not in df.columns]
    if missing:
        raise ValueError(
            "Estrutura inválida no bronze. Colunas ausentes: " + ", ".join(missing)
        )


def validate_parsed_structure(df: DataFrame, expected_columns: List[str]) -> None:
    """
    Valida se as colunas esperadas foram geradas no parse.
    """
    missing = [col for col in expected_columns if col not in df.columns]
    if missing:
        raise ValueError(
            "Estrutura inválida após parse. Colunas ausentes: " + ", ".join(missing)
        )


def validate_final_structure(df: DataFrame, expected_columns: List[str]) -> None:
    """
    Valida se a estrutura final contém todas as colunas esperadas.
    """
    missing = [col for col in expected_columns if col not in df.columns]
    if missing:
        raise ValueError(
            "Estrutura inválida na tabela final. Colunas ausentes: " + ", ".join(missing)
        )
