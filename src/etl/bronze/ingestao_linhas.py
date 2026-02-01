from pyspark.sql import SparkSession
from src.config import URL_LINHAS
from src.utils.download import download_file
from src.utils.save import save_to_bronze

def process_linhas_to_bronze(spark: SparkSession):
    """Ingere tabela de conversão de linhas (CSV) para a camada Bronze."""
    try:
        raw_path = download_file(URL_LINHAS, "linhas_sistema.csv")

        df = spark.read.format("csv") \
            .option("header", "true") \
            .option("delimiter", ",") \
            .load(raw_path)
        
        save_to_bronze(df, "linhas_sistema.csv", "linhas")
        
    except Exception as e:
        print(f"Falha no fluxo Linhas: {e}")
        raise e
