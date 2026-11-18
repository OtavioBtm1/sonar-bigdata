# Databricks notebook source
# MAGIC %md
# MAGIC # 02 - Camada Prata: Tratamento, 3 Regras de Qualidade e MERGE INTO
# MAGIC **Projeto:** Sonar (`https://sonar.rodrigoviana.app.br` | Repo: `https://github.com/RodrigoResendeViana/sonar-blind`)  
# MAGIC **Requisito atendido:** Requisito 06 (Tipos corretos, nulos e inválidos tratados, `MERGE INTO` pela chave e 3 regras de qualidade com total barrado por cada uma).

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Tratamento e MERGE da Tabela GTFS + Paradas (`prata_gtfs`)

# COMMAND ----------

df_gtfs_raw = spark.table("workspace.gs.bronze_gtfs")

df_gtfs_limpo = (
    df_gtfs_raw
    .filter(F.col("codigo_id").isNotNull() & F.col("tipo_registro").isin("LINHA", "PARADA"))
    .withColumn("codigo_id", F.col("codigo_id").cast("string"))
    .withColumn("codigo_letreiro", F.col("codigo_letreiro").cast("string"))
    .withColumn("latitude", F.col("latitude").cast("double"))
    .withColumn("longitude", F.col("longitude").cast("double"))
    .withColumn("possui_sonar_ponto", F.col("possui_sonar_ponto").cast("boolean"))
    .withColumn("data_referencia", F.to_date("data_referencia"))
)

w_gtfs = Window.partitionBy("tipo_registro", "codigo_id").orderBy(F.col("data_referencia").desc())
df_gtfs_dedup = df_gtfs_limpo.withColumn("rn", F.row_number().over(w_gtfs)).filter("rn = 1").drop("rn")
df_gtfs_dedup.createOrReplaceTempView("lote_prata_gtfs")

spark.sql("""
CREATE TABLE IF NOT EXISTS workspace.gs.prata_gtfs (
    tipo_registro STRING,
    codigo_id STRING,
    codigo_letreiro STRING,
    nome_descritivo STRING,
    bairro_ou_operadora STRING,
    latitude DOUBLE,
    longitude DOUBLE,
    possui_sonar_ponto BOOLEAN,
    data_referencia DATE
) USING DELTA
""")

spark.sql("""
MERGE INTO workspace.gs.prata_gtfs AS alvo
USING lote_prata_gtfs AS novo
ON alvo.tipo_registro = novo.tipo_registro AND alvo.codigo_id = novo.codigo_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Explosão do JSON Olho Vivo, 3 Regras de Qualidade e MERGE INTO (`prata_olhovivo_posicao`)

# COMMAND ----------

df_ov_bronze = spark.table("workspace.gs.bronze_olhovivo")

df_ov_exploded = (
    df_ov_bronze
    .withColumn("linha", F.explode("l"))
    .withColumn("veiculo", F.explode("linha.vs"))
    .select(
        F.to_date("data_coleta").alias("data_coleta"),
        F.col("linha.c").cast("string").alias("linha_letreiro"),
        F.col("linha.cl").cast("long").alias("codigo_cl"),
        F.col("linha.lt0").cast("string").alias("destino"),
        F.col("veiculo.p").cast("string").alias("prefixo_veiculo"),
        F.col("veiculo.a").cast("boolean").alias("acessivel"),
        F.to_timestamp("veiculo.ta").alias("horario_captura"),
        F.col("veiculo.py").cast("double").alias("latitude"),
        F.col("veiculo.px").cast("double").alias("longitude"),
        F.col("veiculo.intervalo_observado_min").cast("double").alias("intervalo_observado_min")
    )
)

linhas_gtfs = [r["codigo_letreiro"] for r in spark.table("workspace.gs.prata_gtfs").filter("tipo_registro = 'LINHA'").select("codigo_letreiro").distinct().collect()]

# 3 Regras de Qualidade com contagem explícita de barrados:
# Regra 1: Coordenada dentro de São Paulo (lat entre -24.05 e -23.30, lon entre -46.85 e -46.30)
# Regra 2: Timestamp não futuro e não nulo
# Regra 3: Linha válida cadastrada no GTFS (`PADRAO_LINHA` em `api/sonar_api/paradas.py`)
df_ov_regras = (
    df_ov_exploded
    .withColumn("regra_1_coord_sp_ok", F.col("latitude").between(-24.05, -23.30) & F.col("longitude").between(-46.85, -46.30))
    .withColumn("regra_2_timestamp_valido_ok", F.col("horario_captura").isNotNull() & (F.col("horario_captura") <= F.to_timestamp(F.lit("2026-11-30T23:59:59"))))
    .withColumn("regra_3_linha_no_gtfs_ok", F.col("linha_letreiro").isin(linhas_gtfs))
)

df_auditoria_ov = df_ov_regras.agg(
    F.count("*").alias("total_bruto_explodido"),
    F.sum(F.when(~F.col("regra_1_coord_sp_ok"), 1).otherwise(0)).alias("barrados_regra_1_coord_fora_sp"),
    F.sum(F.when(~F.col("regra_2_timestamp_valido_ok"), 1).otherwise(0)).alias("barrados_regra_2_horario_futuro"),
    F.sum(F.when(~F.col("regra_3_linha_no_gtfs_ok"), 1).otherwise(0)).alias("barrados_regra_3_linha_inexistente_gtfs"),
    F.sum(F.when(F.col("regra_1_coord_sp_ok") & F.col("regra_2_timestamp_valido_ok") & F.col("regra_3_linha_no_gtfs_ok"), 1).otherwise(0)).alias("total_aprovados_prata")
)
display(df_auditoria_ov)

w_ov = Window.partitionBy("prefixo_veiculo", "horario_captura").orderBy(F.col("data_coleta").desc())
df_ov_aprovados = (
    df_ov_regras
    .filter("regra_1_coord_sp_ok AND regra_2_timestamp_valido_ok AND regra_3_linha_no_gtfs_ok")
    .drop("regra_1_coord_sp_ok", "regra_2_timestamp_valido_ok", "regra_3_linha_no_gtfs_ok")
    .withColumn("rn", F.row_number().over(w_ov))
    .filter("rn = 1")
    .drop("rn")
)
df_ov_aprovados.createOrReplaceTempView("lote_prata_olhovivo")

spark.sql("""
CREATE TABLE IF NOT EXISTS workspace.gs.prata_olhovivo_posicao (
    data_coleta DATE,
    linha_letreiro STRING,
    codigo_cl BIGINT,
    destino STRING,
    prefixo_veiculo STRING,
    acessivel BOOLEAN,
    horario_captura TIMESTAMP,
    latitude DOUBLE,
    longitude DOUBLE,
    intervalo_observado_min DOUBLE
) USING DELTA
""")

spark.sql("""
MERGE INTO workspace.gs.prata_olhovivo_posicao AS alvo
USING lote_prata_olhovivo AS novo
ON alvo.prefixo_veiculo = novo.prefixo_veiculo
   AND alvo.horario_captura = novo.horario_captura
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Tratamento e 3 Regras de Qualidade nas Esperas/Ocorrências da API Sonar + MERGE INTO (`prata_esperas_sonar`)

# COMMAND ----------

df_esp_bronze = spark.table("workspace.gs.bronze_esperas_sonar")

df_esp_tipado = (
    df_esp_bronze
    .select(
        F.col("id").cast("string").alias("espera_id"),
        F.to_date("data_coleta").alias("data_coleta"),
        F.to_timestamp("criado_em").alias("criado_em"),
        F.col("parada_id").cast("string"),
        F.col("codigo_sptrans").cast("long"),
        F.col("linha_avisada").cast("string"),
        F.col("prefixo_avisado").cast("string"),
        F.col("status").cast("string"),
        F.col("origem").cast("string"),
        F.col("usuario_id_bruto").cast("string"),
        F.col("aviso_enviado").cast("boolean"),
        F.col("motorista_ciente").cast("boolean"),
        F.col("passagem_linha").cast("string"),
        F.col("passagem_confianca").cast("double"),
        F.col("passagem_tempo_parado_s").cast("double"),
        F.col("passagem_parou").cast("boolean"),
        F.col("validado_por_camera").cast("boolean"),
        F.col("tempo_espera_min").cast("double"),
        F.col("ocorrencia_consentimento").cast("boolean"),
        F.col("ocorrencia_protocolo").cast("string"),
        F.col("lat_ponto").cast("double"),
        F.col("lon_ponto").cast("double")
    )
    .withColumn("regra_1_coord_sp_ok", F.col("lat_ponto").between(-24.05, -23.30) & F.col("lon_ponto").between(-46.85, -46.30))
    .withColumn("regra_2_timestamp_valido_ok", F.col("criado_em").isNotNull() & (F.col("criado_em") <= F.to_timestamp(F.lit("2026-11-30T23:59:59"))))
    .withColumn("regra_3_linha_no_gtfs_ok", F.col("passagem_linha").isin(linhas_gtfs))
)

df_auditoria_esp = df_esp_tipado.agg(
    F.count("*").alias("total_bruto_esperas"),
    F.sum(F.when(~F.col("regra_1_coord_sp_ok"), 1).otherwise(0)).alias("barrados_regra_1_coord_fora_sp"),
    F.sum(F.when(~F.col("regra_2_timestamp_valido_ok"), 1).otherwise(0)).alias("barrados_regra_2_horario_futuro"),
    F.sum(F.when(~F.col("regra_3_linha_no_gtfs_ok"), 1).otherwise(0)).alias("barrados_regra_3_linha_inexistente_gtfs"),
    F.sum(F.when(F.col("regra_1_coord_sp_ok") & F.col("regra_2_timestamp_valido_ok") & F.col("regra_3_linha_no_gtfs_ok"), 1).otherwise(0)).alias("total_aprovados_prata")
)
display(df_auditoria_esp)

w_esp = Window.partitionBy("espera_id").orderBy(F.col("criado_em").desc())
df_esp_aprovados = (
    df_esp_tipado
    .filter("regra_1_coord_sp_ok AND regra_2_timestamp_valido_ok AND regra_3_linha_no_gtfs_ok")
    .drop("regra_1_coord_sp_ok", "regra_2_timestamp_valido_ok", "regra_3_linha_no_gtfs_ok")
    .withColumn("rn", F.row_number().over(w_esp))
    .filter("rn = 1")
    .drop("rn")
)
df_esp_aprovados.createOrReplaceTempView("lote_prata_esperas")

spark.sql("""
CREATE TABLE IF NOT EXISTS workspace.gs.prata_esperas_sonar (
    espera_id STRING,
    data_coleta DATE,
    criado_em TIMESTAMP,
    parada_id STRING,
    codigo_sptrans BIGINT,
    linha_avisada STRING,
    prefixo_avisado STRING,
    status STRING,
    origem STRING,
    usuario_id_bruto STRING,
    aviso_enviado BOOLEAN,
    motorista_ciente BOOLEAN,
    passagem_linha STRING,
    passagem_confianca DOUBLE,
    passagem_tempo_parado_s DOUBLE,
    passagem_parou BOOLEAN,
    validado_por_camera BOOLEAN,
    tempo_espera_min DOUBLE,
    ocorrencia_consentimento BOOLEAN,
    ocorrencia_protocolo STRING,
    lat_ponto DOUBLE,
    lon_ponto DOUBLE
) USING DELTA
""")

spark.sql("""
MERGE INTO workspace.gs.prata_esperas_sonar AS alvo
USING lote_prata_esperas AS novo
ON alvo.espera_id = novo.espera_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
""")
