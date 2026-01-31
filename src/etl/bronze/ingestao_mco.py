from pyspark.sql import SparkSession
from src.config import URL_MCO
from src.utils.download import download_file
from src.utils.save import save_to_bronze
from src.utils.quality import sanitize_columns

def process_mco_to_bronze(spark: SparkSession):
    """Ingere dados do MCO (CSV) para a camada Bronze."""
    try:
        raw_path = download_file(URL_MCO, "mco_raw.csv")
        
        df = spark.read.format("csv") \
            .option("header", "true") \
            .option("delimiter", ";") \
            .option("inferSchema", "true") \
            .load(raw_path)

        df_sanitized = sanitize_columns(df)
        save_to_bronze(df_sanitized, "mco_consolidado.csv", "mco")
        
    except Exception as e:
        print(f"Falha no fluxo MCO: {e}")
        raise e