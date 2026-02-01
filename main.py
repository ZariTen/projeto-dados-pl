import logging
import sys

from src.spark_session import get_spark_session
from src.etl.ingestao_bronze import run_bronze_layer
from src.etl.ingestao_silver import process_bronze_to_silver
from src.etl.ingestao_gold import run_gold_layer


def setup_logging():
    """Configura logging estruturado para todo o pipeline."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler('etl_pipeline.log')
        ]
    )


def main():
    """Executa o pipeline ETL completo."""
    setup_logging()
    logger = logging.getLogger(__name__)
    
    try:
        logger.info("INICIANDO PIPELINE ETL - Mobilidade Urbana")
        
        spark = get_spark_session()
        logger.info(f"SparkSession criada com sucesso: {spark}")
        
        # Camada Bronze
        logger.info("--- Iniciando Camada Bronze ---")
        run_bronze_layer(spark)
        logger.info("Camada Bronze concluída")
        
        # Camada Silver
        logger.info("--- Iniciando Camada Silver ---")
        process_bronze_to_silver(spark)
        logger.info("Camada Silver concluída")
        
        # Camada Gold
        logger.info("--- Iniciando Camada Gold ---")
        run_gold_layer(spark)
        logger.info("Camada Gold concluída")
        
        logger.info("PIPELINE ETL CONCLUÍDO COM SUCESSO")

        spark.stop()

    except Exception as e:
        logger.error(f"ERRO CRÍTICO NO PIPELINE: {e}", exc_info=True)
        raise e


if __name__ == "__main__":
    main()