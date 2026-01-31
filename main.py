from src.spark_session import get_spark_session
from src.etl.bronze import run_bronze_layer

def main():
    spark = get_spark_session()
    
    run_bronze_layer(spark)
    
    print("\n--- Validação Rápida ---")
    try:
        spark.read.parquet("data/bronze/gps").show(5)
        spark.read.parquet("data/bronze/linhas").show(5)
        spark.read.parquet("data/bronze/mco").show(5)
    except:
        print("Tabela GPS ainda não criada.")

    spark.stop()

if __name__ == "__main__":
    main()