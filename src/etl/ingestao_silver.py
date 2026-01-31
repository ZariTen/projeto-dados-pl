import os
import shutil
from pyspark.sql import SparkSession
from src.etl.silver.processar_gps import process_gps_to_silver
from src.etl.silver.processar_mco import process_mco_to_silver
from src.etl.silver.processar_linhas import process_linhas_to_silver
from src.utils.quality import check_data_quality
import logging


def process_bronze_to_silver(spark: SparkSession):
    """
    Processa todos os dados da camada Bronze para a camada Silver.
    """
    # Limpa a pasta silver antes de processar novos dados
    silver_path = "data/silver"
    if os.path.exists(silver_path):
        shutil.rmtree(silver_path)

    process_gps_to_silver(spark)
    process_mco_to_silver(spark)
    process_linhas_to_silver(spark)

    logging.info("Ingestão para camada Silver concluída.")

    check_data_quality(spark, "silver", "gps")
    check_data_quality(spark, "silver", "mco")
    check_data_quality(spark, "silver", "linhas")