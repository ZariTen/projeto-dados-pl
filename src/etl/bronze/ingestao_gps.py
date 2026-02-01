from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from src.config import URL_GPS
from src.utils.download import download_file
from src.utils.save import save_to_bronze

def process_gps_to_bronze(spark: SparkSession):
    """Ingere dados de GPS (Raw Text/CSV misto) para a camada Bronze."""
    try:
        raw_path = download_file(URL_GPS, "gps_raw.csv")
        
        # Leitura como texto devido à formatação irregular
        df_raw = spark.read.text(raw_path)

        save_to_bronze(df_raw, "tempo_real_gps.json", "gps")

    except Exception as e:
        print(f"Falha no fluxo GPS: {e}")
        raise e
