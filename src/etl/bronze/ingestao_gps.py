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
        df_filtered = df_raw.filter(~F.col("value").startswith("_id"))

        # Lógica de Parse
        df_parsed = df_filtered.withColumn(
            "data_part", F.split(F.col("value"), ",", 2).getItem(1)
        ).withColumn(
            "cols", F.split(F.col("data_part"), ";")
        )

        # Mapeamento limpo de colunas
        gps_columns = ["EV", "HR", "LT", "LG", "NV", "VL", "NL", "DG", "SV", "DT"]
        
        # Regex de seleção
        select_exprs = [
            F.col("cols").getItem(i).alias(col_name) 
            for i, col_name in enumerate(gps_columns)
        ]

        df_final = df_parsed.select(*select_exprs).filter(F.col("EV").isNotNull())

        save_to_bronze(df_final, "tempo_real_gps.json", "gps")

    except Exception as e:
        print(f"Falha no fluxo GPS: {e}")
        raise e