# Databricks notebook source
# MAGIC %md
# MAGIC # 03 - Gold dimensions
# MAGIC Assumptions documented here:
# MAGIC - `dim_cliente.vendedor` mixes people and channel categories; kept as is.
# MAGIC - Some client codes in the sales data have no row in `dim_cliente` (shown as `SEM CADASTRO` in the KPIs).
# MAGIC - `TOTAL` in the planning table is treated as a subtotal and excluded.

# COMMAND ----------

# MAGIC %run ./00_setup

# COMMAND ----------

# dim_cliente: natural key (an8), no surrogate needed
df = spark.table("dsa_projeto.silver.relacao_de_clientes").select(
    col("an8").alias("codigo_cliente"),
    col("razao_social"),
    col("nome_grp").alias("nome_grupo"),
    col("cnpj"),
    col("vendedor"),
    col("channel").alias("canal"),
    col("sub_channel").alias("subcanal"),
    col("cidade"), col("uf"), col("regiao"), col("pais"),
    col("classificacao_rnsa"))
exigir_chave_unica(df, "codigo_cliente", "dim_cliente")
gravar_gold(df, "dim_cliente")

# COMMAND ----------

# dim_vendedor
df = (spark.table("dsa_projeto.silver.meta_vendedor")
      .select(trim(col("vendedor")).alias("vendedor")).distinct()
      .filter(col("vendedor").isNotNull())
      .withColumn("id_vendedor", chave_hash(col("vendedor")))
      .select("id_vendedor", "vendedor"))
exigir_chave_unica(df, "id_vendedor", "dim_vendedor")
gravar_gold(df, "dim_vendedor")

# COMMAND ----------

# dim_canal: grain is the (canal, canal_2) pair
metas = spark.table("dsa_projeto.silver.metas")
CATEGORIAS = ["ACCESSORIES", "CONSUMABLES", "SPARE PARTS", "HC&SDA"]
AJUSTE = "OUTROS ZERA EXPORTS GROUP"

df = (metas.select(upper(trim(col("canal"))).alias("canal"),
                   upper(trim(col("canal_2"))).alias("canal_2")).distinct()
      .withColumn("id_canal", chave_hash(col("canal"), col("canal_2")))
      .withColumn("tipo_canal", F.when(col("canal").isin(CATEGORIAS), "CATEGORIA").otherwise("CANAL"))
      .withColumn("eh_ajuste", F.coalesce(col("canal_2") == AJUSTE, F.lit(False)))
      .select("id_canal", "canal", "canal_2", "tipo_canal", "eh_ajuste"))
exigir_chave_unica(df, "id_canal", "dim_canal")
gravar_gold(df, "dim_canal")

# COMMAND ----------

# dim_versao: names starting with '#' are Excel errors and go to quarantine
df_v = (metas.select(upper(trim(col("versao"))).alias("versao"), col("target_value"))
        .filter(~col("versao").startswith("#"))
        .groupBy("versao")
        .agg(F.sum(F.when(col("target_value").isNotNull() & (col("target_value") != 0), 1)
                   .otherwise(0)).alias("qtd_alvos_nao_zero")))

df = (df_v.withColumn("id_versao", chave_hash(col("versao")))
      .withColumn("familia", F.when(col("versao").startswith("SOP "), "SOP").otherwise("OUTRA"))
      .withColumn("numero_sop", F.when(col("versao").startswith("SOP "),
                  F.regexp_extract("versao", r"(\d+)", 1).cast("int")))
      .withColumn("tem_alvo", col("qtd_alvos_nao_zero") > 0)
      .select("id_versao", "versao", "familia", "numero_sop", "tem_alvo"))
exigir_chave_unica(df, "id_versao", "dim_versao")
gravar_gold(df, "dim_versao")

(metas.filter(col("versao").startswith("#"))
    .write.mode("overwrite").saveAsTable("dsa_projeto.silver.metas_quarentena"))

# COMMAND ----------

# dim_sales_group
sg_plan = (spark.table("dsa_projeto.silver.planning_book")
           .select(upper(trim(col("sales_group"))).alias("sales_group"))
           .filter(col("sales_group") != "TOTAL"))
sg_vend = (spark.table("dsa_projeto.silver.sales_monitoring")
           .select(upper(trim(col("sgrp"))).alias("sales_group")))

df = (sg_plan.union(sg_vend).filter(col("sales_group").isNotNull()).distinct()
      .withColumn("id_sales_group", chave_hash(col("sales_group")))
      .select("id_sales_group", "sales_group"))
exigir_chave_unica(df, "id_sales_group", "dim_sales_group")
gravar_gold(df, "dim_sales_group")

# COMMAND ----------

# dim_calendario: the date range is an assumption, adjust if your data goes beyond it
df = (spark.sql("SELECT explode(sequence(to_date('2024-01-01'), to_date('2027-12-31'), interval 1 day)) AS data")
      .select("data", F.year("data").alias("ano"), F.month("data").alias("mes"),
              F.quarter("data").alias("trimestre"), F.dayofweek("data").alias("dia_semana")))
gravar_gold(df, "dim_calendario")
