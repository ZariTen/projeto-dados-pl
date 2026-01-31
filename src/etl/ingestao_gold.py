from src.etl.gold.fato_viagem import run_fato_viagem
from pyspark.sql import SparkSession
from src.utils.quality import check_data_quality


def run_gold_layer(spark: SparkSession):
    """
    Executa o processo ETL para a camada Gold.
    """
    run_fato_viagem(spark)
    check_data_quality(spark, "gold", "fato_viagem")