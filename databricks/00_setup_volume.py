# Databricks notebook source
# MAGIC %md
# MAGIC # 00 - Setup Rápido: Criação do Schema `workspace.gs` e do Volume `chegada`
# MAGIC Rode este notebook primeiro (ou rode apenas a célula abaixo) para criar automaticamente o Schema e o Volume com as 3 pastas no Databricks Free Edition!

# COMMAND ----------

spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.gs")
spark.sql("CREATE VOLUME IF NOT EXISTS workspace.gs.chegada")

# Criar as 3 subpastas dentro do Volume automaticamente
for sub in ["olhovivo", "gtfs", "esperas_sonar"]:
    dbutils.fs.mkdirs(f"/Volumes/workspace/gs/chegada/{sub}")

print("Schema workspace.gs, Volume chegada e subpastas [olhovivo, gtfs, esperas_sonar] criados com sucesso!")
display(dbutils.fs.ls("/Volumes/workspace/gs/chegada"))
