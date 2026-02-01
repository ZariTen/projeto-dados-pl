import os
import shutil
import logging
from pyspark.sql import SparkSession
from src.etl.silver.processar_gps import process_gps_to_silver
from src.etl.silver.processar_mco import process_mco_to_silver
from src.etl.silver.processar_linhas import process_linhas_to_silver
from src.utils.quality import check_data_quality

logger = logging.getLogger(__name__)


def process_bronze_to_silver(spark: SparkSession):
    """
    Processa todos os dados da camada Bronze para a camada Silver com particionamento.
    """
    try:
        # Limpa a pasta silver antes de processar novos dados
        silver_path = "data/silver"
        if os.path.exists(silver_path):
            logger.info(f"Removendo dados anteriores: {silver_path}")
            shutil.rmtree(silver_path)

        logger.info("Processando GPS para Silver com particionamento")
        process_gps_to_silver(spark)
        
        logger.info("Processando MCO para Silver com particionamento")
        process_mco_to_silver(spark)
        
        logger.info("Processando Linhas para Silver")
        process_linhas_to_silver(spark)

        logger.info("Executando validações de qualidade em Silver")
        check_data_quality(spark, "silver", "gps")
        check_data_quality(spark, "silver", "mco")
        check_data_quality(spark, "silver", "linhas")
        
        logger.info("Camada Silver concluída com sucesso")

    except Exception as e:
        logger.error(f"Erro ao processar camada Silver: {e}", exc_info=True)
        raise e