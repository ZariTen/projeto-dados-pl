import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import current_timestamp, lit, col, regexp_replace
from pyspark.sql.types import IntegerType, LongType, DoubleType


def process_mco_to_silver(spark: SparkSession):
    """
    Processa os dados MCO da camada Bronze para a camada Silver.
    """
    try:
        # 1. Leitura
        bronze_path = os.path.join("data/bronze", "mco")
        df_bronze = spark.read.format("parquet").load(bronze_path)

        # 2. Limpeza e Tipagem
        df_clean = df_bronze.withColumn("data_viagem", F.to_date(F.col("viagem"), "dd/MM/yyyy")) \
                .withColumn("dh_fechamento", F.to_timestamp(F.col("data_fechamento"), "dd/MM/yyyy HH:mm")) \
                .withColumn("extensao_metros", F.col("extensao").cast(IntegerType())) \
                .withColumn("total_usuarios", F.col("total_usuarios").cast(IntegerType())) \
                .withColumn("sublinha", F.col("sublinha").cast(IntegerType())) \
                .withColumn("pc", F.col("pc").cast(IntegerType())) \
                .withColumn("catraca_saida", F.col("catraca_saida").cast(LongType())) \
                .withColumn("catraca_chegada", F.col("catraca_chegada").cast(LongType()))

        # Tratamento de strings vazias para nulos
        cols_to_check = ["ocorrencia", "justificativa", "falha_mecanica", "evento_inseguro"]
        for col_name in cols_to_check:
            df_clean = df_clean.withColumn(col_name, F.when(F.col(col_name) == "", None).otherwise(F.col(col_name)))

        # Tratamento de timestamp
        df_time = df_clean \
        .withColumn("ts_saida", F.to_timestamp(F.concat(F.col("data_viagem"), F.lit(" "), F.col("saida")), "yyyy-MM-dd HH:mm")) \
        .withColumn("ts_chegada_temp", F.to_timestamp(F.concat(F.col("data_viagem"), F.lit(" "), F.col("chegada")), "yyyy-MM-dd HH:mm"))

        # Logica para ajustar chegada no dia seguinte
        df_time = df_time.withColumn("ts_chegada", 
            F.when(F.col("ts_chegada_temp") < F.col("ts_saida"), F.col("ts_chegada_temp") + F.expr("INTERVAL 1 DAY"))
             .otherwise(F.col("ts_chegada_temp"))
        ).drop("ts_chegada_temp", "hora_saida", "hora_chegada")

        # Duração da viagem em minutos
        df_silver = df_time.withColumn("duracao_viagem_minutos", 
            F.round((F.col("ts_chegada").cast("long") - F.col("ts_saida").cast("long")) / 60, 2)
        )

        # Concessionarias
        df_silver = df_silver.withColumn("nome_consorcio", 
        F.when(F.col("concessionaria") == "801", "Consórcio Pampulha")
         .when(F.col("concessionaria") == "802", "Consórcio BHLeste")
         .when(F.col("concessionaria") == "803", "Consórcio Dez")
         .when(F.col("concessionaria") == "804", "Consórcio Dom Pedro II")
         .otherwise("Desconhecido")
        )

        # Tipo de Viagem (PC)
        df_silver = df_silver.withColumn("desc_tipo_viagem",
        F.when(F.col("pc") == 0, "Ociosa")
         .when(F.col("pc").isin(1, 2), "Normal")
         .when(F.col("pc") == 3, "Transferencia")
         .otherwise("Outros")
        )

        # Tratamento boolean
        df_silver = df_silver.withColumn("teve_falha_mecanica", F.col("falha_mecanica") == 1) \
                             .withColumn("teve_evento_inseguro", F.col("evento_inseguro") == 1)

        # Otimizaçao
        df_silver = df_silver.withColumn("ano", F.year("data_viagem")) \
                          .withColumn("mes", F.month("data_viagem"))

        df_silver = df_silver.drop("viagem", "data_fechamento", "extensao", "total_usuarios", "sublinha", "concessionaria", "pc"
                                   , "falha_mecanica", "evento_inseguro", "saida", "chegada", "_ingestion_timestamp", "_source_file")

        # 3. Escrita formato Delta Lake
        silver_path = os.path.join("data/silver", "mco")
        df_silver.write.format("delta").mode("overwrite").save(silver_path)

        print(f"MCO salvo na Silver: {silver_path}")

    except Exception as e:
        print(f"Falha no fluxo MCO para Silver: {e}")
        raise e

def process_gps_to_silver(spark: SparkSession):
    """
    Processa dados de GPS (Telemetria) com otimização para Geoespacial e Time Series.
    """
    try:
        # 1. Leitura
        bronze_path = os.path.join("data/bronze", "gps")
        df_bronze = spark.read.format("parquet").load(bronze_path)

        # 2. Tratamento e Tipagem
        df_silver = df_bronze \
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
        silver_path = os.path.join("data/silver", "gps")
        
        (df_final.write
            .format("delta")
            .mode("overwrite")
            .partitionBy("data_particao") 
            .option("overwriteSchema", "true")
            .save(silver_path)
        )

        print(f"GPS salvo na Silver: {silver_path}")

    except Exception as e:
        print(f"Falha no fluxo GPS para Silver: {e}")
        raise e

def process_bronze_to_silver(spark: SparkSession):
    """
    Processa todos os dados da camada Bronze para a camada Silver.
    """
    process_gps_to_silver(spark)
    process_mco_to_silver(spark)
def run_silver_layer(spark):
    """
    Executa o processo ETL para a camada Silver.
    """
    process_bronze_to_silver(spark)