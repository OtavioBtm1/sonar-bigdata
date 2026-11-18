# Databricks notebook source
# MAGIC %md
# MAGIC # 01 - Camada Bronze: Ingestão Incremental e Idempotente (Auto Loader)
# MAGIC **Projeto:** Sonar (`https://sonar.rodrigoviana.app.br` | Repo: `https://github.com/RodrigoResendeViana/sonar-blind`)  
# MAGIC **Requisitos atendidos:** Requisito 04 (2 lotes de dias diferentes no Volume) e Requisito 05 (Auto Loader incremental e idempotente, sem duplicação, com contagens antes e depois).

# COMMAND ----------

from pyspark.sql import functions as F

BASE_VOLUME = "/Volumes/workspace/gs/chegada"
spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.gs")

# COMMAND ----------

def ingerir_bronze(subpasta: str, formato: str, tabela_destino: str):
    df_stream = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", formato)
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaLocation", f"{BASE_VOLUME}/_controle/schema_{subpasta}")
    )
    if formato == "json":
        df_stream = df_stream.option("multiLine", "true")
    elif formato == "csv":
        df_stream = df_stream.option("header", "true")

    query = (
        df_stream
        .load(f"{BASE_VOLUME}/{subpasta}")
        .withColumn("arquivo_origem", F.col("_metadata.file_path"))
        .withColumn("carregado_em", F.current_timestamp())
        .writeStream
        .option("checkpointLocation", f"{BASE_VOLUME}/_controle/checkpoint_{subpasta}")
        .option("mergeSchema", "true")
        .trigger(availableNow=True)
        .toTable(f"workspace.gs.{tabela_destino}")
    )
    query.awaitTermination()

# Ingestão das 3 fontes do Sonar
ingerir_bronze("olhovivo", "json", "bronze_olhovivo")
ingerir_bronze("gtfs", "csv", "bronze_gtfs")
ingerir_bronze("esperas_sonar", "json", "bronze_esperas_sonar")

# COMMAND ----------

# Auditoria de contagens (Antes e Depois do 2º Lote e após a 2ª execução seguida)
df_contagens = spark.sql("""
    SELECT 'bronze_olhovivo' AS tabela, COUNT(*) AS total_registros, COUNT(DISTINCT arquivo_origem) AS arquivos_lidos FROM workspace.gs.bronze_olhovivo
    UNION ALL
    SELECT 'bronze_gtfs' AS tabela, COUNT(*) AS total_registros, COUNT(DISTINCT arquivo_origem) AS arquivos_lidos FROM workspace.gs.bronze_gtfs
    UNION ALL
    SELECT 'bronze_esperas_sonar' AS tabela, COUNT(*) AS total_registros, COUNT(DISTINCT arquivo_origem) AS arquivos_lidos FROM workspace.gs.bronze_esperas_sonar
""")
display(df_contagens)
