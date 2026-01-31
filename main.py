from src.spark_session import get_spark_session
from src.etl.bronze import run_bronze_layer
from src.etl.silver import run_silver_layer

def main():
    spark = get_spark_session()
    
    run_bronze_layer(spark)
    run_silver_layer(spark)
    
    spark.stop()

if __name__ == "__main__":
    main()