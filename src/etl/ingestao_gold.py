import os
import shutil
from src.etl.gold.fato_viagem import run_fato_performance_diaria
from pyspark.sql import SparkSession
from src.utils.quality import check_data_quality


def run_gold_layer(spark: SparkSession):
    """
    Executa o processo ETL para a camada Gold.
    """
    # Limpa a pasta gold antes de processar novos dados
    gold_path = "data/gold"
    if os.path.exists(gold_path):
        shutil.rmtree(gold_path)

    run_fato_performance_diaria(spark)
    check_data_quality(spark, "gold", "fato_performance_diaria")