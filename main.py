from src.spark_session import get_spark_session
from src.etl.ingestao_bronze import run_bronze_layer
from src.etl.ingestao_silver import process_bronze_to_silver
from src.etl.ingestao_gold import run_gold_layer

def main():
    spark = get_spark_session()
    
    #run_bronze_layer(spark)
    process_bronze_to_silver(spark)
    #run_gold_layer(spark)

    spark.stop()

if __name__ == "__main__":
    main()