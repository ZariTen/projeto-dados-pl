from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
import os

def get_spark_session(app_name="DesafioMobilidade"):
    """
    Cria e retorna uma SparkSession configurada para Delta Lake local.
    """
    builder = SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.sql.warehouse.dir", os.path.join(os.getcwd(), "spark-warehouse")) \
        .config("spark.driver.memory", "8g")

    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    
    return spark