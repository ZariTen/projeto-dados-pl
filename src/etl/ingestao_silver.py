from pyspark.sql import SparkSession
from src.etl.silver.processar_gps import process_gps_to_silver
from src.etl.silver.processar_mco import process_mco_to_silver
from src.etl.silver.processar_linhas import process_linhas_to_silver
from src.utils.quality import count_nulls_per_column

def check_data_quality(spark: SparkSession, table_name: str):
    """
    Verifica a qualidade dos dados na tabela Silver especificada.
    """
    silver_path = f"data/silver/{table_name}"
    df_silver = spark.read.format("delta").load(silver_path)
    
    null_counts_df = count_nulls_per_column(df_silver)
    null_counts = null_counts_df.collect()[0].asDict()
    
    print(f"Verificação de qualidade para {table_name} na Silver:")
    for column, null_count in null_counts.items():
        if null_count > 0:
            print(f" - Coluna '{column}' possui {null_count} valores nulos.")


def process_bronze_to_silver(spark: SparkSession):
    """
    Processa todos os dados da camada Bronze para a camada Silver.
    """
    process_gps_to_silver(spark)
    process_mco_to_silver(spark)
    process_linhas_to_silver(spark)

    check_data_quality(spark, "gps")
    check_data_quality(spark, "mco")
    check_data_quality(spark, "linhas")