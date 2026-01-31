import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, DoubleType
from src.utils.save import save_to_silver

def process_gps_to_silver(spark: SparkSession):
    """
    Processa dados de GPS (Telemetria) com otimização para Geoespacial e Time Series.
    """
    try:
        # 1. Leitura
        bronze_path = os.path.join("data/bronze", "gps")
        df_bronze = spark.read.format("parquet").load(bronze_path)

        # 2. Tratamento e Tipagem
        df_filtered = df_bronze.filter(~F.col("value").startswith("_id"))

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

        df_bronze_final = df_parsed.select(*select_exprs).filter(F.col("EV").isNotNull())

        df_silver = df_bronze_final \
            .withColumn("numero_veiculo", F.col("NV").cast(IntegerType())) \
            .withColumn("cod_evento", F.col("EV").cast(IntegerType())) \
            .withColumn("latitude", F.regexp_replace(F.col("LT"), ",", ".").cast(DoubleType())) \
            .withColumn("longitude", F.regexp_replace(F.col("LG"), ",", ".").cast(DoubleType())) \
            .withColumn("velocidade_kmh", F.col("VL").cast(IntegerType())) \
            .withColumn("numero_linha", F.col("NL").cast(IntegerType())) \
            .withColumn("direcao_graus", F.col("DG").cast(IntegerType())) \
            .withColumn("distancia_percorrida", F.col("DT").cast(IntegerType())) \
            .withColumn("sentido_viagem", F.when(F.col("SV") == "1", "IDA")
                                           .when(F.col("SV") == "2", "VOLTA")
                                           .otherwise("INDEFINIDO"))

        df_silver = df_silver.withColumn("timestamp_gps", F.to_timestamp(F.col("HR"), "yyyyMMddHHmmss"))
        
        # Colunas auxiliares para particionamento
        df_silver = df_silver.withColumn("data_particao", F.to_date(F.col("timestamp_gps"))) \
                             .withColumn("hora", F.hour(F.col("timestamp_gps")))

        # Limpeza
        df_silver = df_silver.filter((F.col("latitude") != 0.0) & (F.col("longitude") != 0.0))
        df_silver = df_silver.dropDuplicates(["numero_veiculo", "timestamp_gps"])


        cols_final = [
            "timestamp_gps", "numero_veiculo", "numero_linha", "sentido_viagem",
            "latitude", "longitude", "velocidade_kmh", "distancia_percorrida",
            "direcao_graus", "cod_evento", "data_particao", "hora"
        ]
        
        df_final = df_silver.select(cols_final)

        # 3. Escrita formato Delta Lake
        save_to_silver(df_final, "gps")

    except Exception as e:
        print(f"Falha no fluxo GPS para Silver: {e}")
        raise e