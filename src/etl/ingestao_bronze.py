import os
import shutil
import logging
from pyspark.sql import SparkSession
from src.config import LANDING_ZONE
from src.etl.bronze.ingestao_gps import process_gps_to_bronze
from src.etl.bronze.ingestao_mco import process_mco_to_bronze
from src.etl.bronze.ingestao_linhas import process_linhas_to_bronze
from src.utils.quality import check_data_quality

logger = logging.getLogger(__name__)

def run_bronze_layer(spark: SparkSession):
    """Orquestrador da camada Bronze."""
    try:
        # Limpa a pasta bronze antes de processar novos dados
        bronze_path = "data/bronze"
        if os.path.exists(bronze_path):
            logger.info(f"Removendo dados anteriores: {bronze_path}")
            shutil.rmtree(bronze_path)

        logger.info("Processando GPS para Bronze")
        process_gps_to_bronze(spark)
        
        logger.info("Processando MCO para Bronze")
        process_mco_to_bronze(spark)
        
        logger.info("Processando Linhas para Bronze")
        process_linhas_to_bronze(spark)

        logger.info("Executando validações de qualidade em Bronze")
        check_data_quality(spark, "bronze", "gps")
        check_data_quality(spark, "bronze", "mco")
        check_data_quality(spark, "bronze", "linhas")

        # Limpeza da landing zone após a ingestão
        if os.path.exists(LANDING_ZONE):
            logger.info(f"Removendo landing zone: {LANDING_ZONE}")
            shutil.rmtree(LANDING_ZONE)
        
        logger.info("Camada Bronze concluída com sucesso")

    except Exception as e:
        logger.error(f"Erro ao processar camada Bronze: {e}", exc_info=True)
        raise e