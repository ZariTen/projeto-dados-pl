from typing import List
from pyspark.sql import DataFrame


def validate_fato_performance_structure(df: DataFrame, expected_columns: List[str]) -> None:
    """
    Valida se a estrutura da Fato Performance Diária contém todas as colunas esperadas.
    """
    missing = [col for col in expected_columns if col not in df.columns]
    if missing:
        raise ValueError(
            "Estrutura inválida na Fato Performance Diária. Colunas ausentes: " + ", ".join(missing)
        )


def validate_fato_performance_data_quality(df: DataFrame) -> None:
    """
    Valida a qualidade dos dados na Fato Performance Diária.
    """
    # Verificar se data_referencia não é nula
    invalid_dates = df.filter(df.data_referencia.isNull()).count()
    if invalid_dates > 0:
        raise ValueError(
            f"Estrutura inválida: encontradas {invalid_dates} linhas com data_referencia nula"
        )
    
    # Verificar se numero_linha não é nula
    invalid_lines = df.filter(df.numero_linha.isNull()).count()
    if invalid_lines > 0:
        raise ValueError(
            f"Estrutura inválida: encontradas {invalid_lines} linhas com numero_linha nula"
        )
    
    # Verificar se métricas numéricas não são negativas
    invalid_metrics = df.filter(
        (df.qtd_viagens_realizadas < 0) |
        (df.qtd_falhas_mecanicas < 0) |
        (df.duracao_media_viagem_min < 0) |
        (df.vel_media_gps < 0) |
        (df.vel_maxima_gps < 0) |
        (df.distancia_total_gps_metros < 0)
    ).count()
    if invalid_metrics > 0:
        raise ValueError(
            f"Estrutura inválida: encontradas {invalid_metrics} linhas com métricas negativas"
        )
