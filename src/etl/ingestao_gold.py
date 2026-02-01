import os
import shutil
import logging
from src.etl.gold.fato_viagem import run_fato_performance_diaria
from pyspark.sql import SparkSession
from src.utils.quality import check_data_quality

logger = logging.getLogger(__name__)


def run_gold_layer(spark: SparkSession):
    """
    Executa o processo ETL para a camada Gold com particionamento.
    """
    try:
        # Limpa a pasta gold antes de processar novos dados
        gold_path = "data/gold"
        if os.path.exists(gold_path):
            logger.info(f"Removendo dados anteriores: {gold_path}")
            shutil.rmtree(gold_path)

        logger.info("Processando Fato Performance Diária para Gold com particionamento")
        run_fato_performance_diaria(spark)
        
        logger.info("Executando validações de qualidade em Gold")
        check_data_quality(spark, "gold", "fato_performance_diaria")
        
        logger.info("Camada Gold concluída com sucesso")

    except Exception as e:
        logger.error(f"Erro ao processar camada Gold: {e}", exc_info=True)
        raise e