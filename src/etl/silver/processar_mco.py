from pyspark.sql import SparkSession
import os
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, LongType
from src.utils.save import save_to_silver
from src.utils.quality import sanitize_columns

def process_mco_to_silver(spark: SparkSession):
    """
    Processa os dados MCO da camada Bronze para a camada Silver.
    """
    try:
        # 1. Leitura
        bronze_path = os.path.join("data/bronze", "mco")
        df_bronze = spark.read.format("parquet").load(bronze_path)

        df_bronze = sanitize_columns(df_bronze)

        # 2. Limpeza e Tipagem
        df_clean = df_bronze.withColumn("data_viagem", F.to_date(F.col("viagem"), "dd/MM/yyyy")) \
                .withColumn("dh_fechamento", F.to_timestamp(F.col("data_fechamento"), "dd/MM/yyyy HH:mm")) \
                .withColumn("extensao_metros", F.col("extensao").cast(IntegerType())) \
                .withColumn("total_usuarios", F.col("total_usuarios").cast(IntegerType())) \
                .withColumn("numero_linha", F.col("sublinha").cast(IntegerType())) \
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
        df_silver = df_silver \
            .withColumn("teve_falha_mecanica", F.coalesce(F.col("falha_mecanica").cast(IntegerType()), F.lit(0)) == 1) \
            .withColumn("teve_evento_inseguro", F.coalesce(F.col("evento_inseguro").cast(IntegerType()), F.lit(0)) == 1)

        # Otimizaçao
        df_silver = df_silver.withColumn("ano", F.year("data_viagem")) \
                          .withColumn("mes", F.month("data_viagem"))

        cols_to_drop = ["viagem", "data_fechamento", "extensao",
                        "concessionaria", "pc", "falha_mecanica", "evento_inseguro", 
                        "saida", "chegada", "_ingestion_timestamp", "_source_file"]
        
        if "" in df_silver.columns:
            cols_to_drop.append("")
        
        df_silver = df_silver.drop(*cols_to_drop)

        # 3. Escrita formato Delta Lake
        save_to_silver(df_silver, "mco")

    except Exception as e:
        print(f"Falha no fluxo MCO para Silver: {e}")
        raise e
