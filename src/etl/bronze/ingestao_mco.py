from pyspark.sql import SparkSession
from src.config import URL_MCO
from src.utils.download import download_file
from src.utils.save import save_to_bronze

def process_mco_to_bronze(spark: SparkSession):
    """Ingere dados do MCO (CSV) para a camada Bronze."""
    try:
        raw_path = download_file(URL_MCO, "mco_raw.csv")
        
        df_raw = spark.read.format("csv") \
            .option("header", "true") \
            .option("delimiter", ";") \
            .load(raw_path)

        save_to_bronze(df_raw, "mco_consolidado.csv", "mco")
        
    except Exception as e:
        print(f"Falha no fluxo MCO: {e}")
        raise e