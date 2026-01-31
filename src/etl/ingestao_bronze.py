import os
import shutil
from pyspark.sql import SparkSession
from src.config import LANDING_ZONE
from src.etl.bronze.ingestao_gps import process_gps_to_bronze
from src.etl.bronze.ingestao_mco import process_mco_to_bronze
from src.etl.bronze.ingestao_linhas import process_linhas_to_bronze
from src.utils.quality import check_data_quality

def run_bronze_layer(spark: SparkSession):
    """Orquestrador da camada Bronze."""
    process_gps_to_bronze(spark)
    process_mco_to_bronze(spark)
    process_linhas_to_bronze(spark)

    check_data_quality(spark, "bronze", "gps")
    check_data_quality(spark, "bronze", "mco")
    check_data_quality(spark, "bronze", "linhas")

    # Limpeza da landing zone após a ingestão
    if os.path.exists(LANDING_ZONE):
        shutil.rmtree(LANDING_ZONE)