# Databricks notebook source
# MAGIC %md
# MAGIC # 04 - Gold facts
# MAGIC Assumptions documented here:
# MAGIC - `fact_planning` is built from the **Bronze** table because the Silver layer does not keep
# MAGIC   `latam_sales_quota` and `apo_product`.
# MAGIC - `fact_vendas` keeps `curr`: export rows are in USD, the rest in BRL, so revenue is never summed across currencies.
# MAGIC - The 12 rows with version `#REF!` are an Excel error and stay out of `fact_metas`.

# COMMAND ----------

# MAGIC %run ./00_setup

# COMMAND ----------

# fact_metas
MESES = {"JAN": 1, "FEV": 2, "MAR": 3, "ABR": 4, "MAI": 5, "JUN": 6,
         "JUL": 7, "AGO": 8, "SET": 9, "OUT": 10, "NOV": 11, "DEZ": 12}
mapa_mes = F.create_map([F.lit(x) for kv in MESES.items() for x in kv])

df = (spark.table("dsa_projeto.silver.metas")
      .filter(~upper(trim(col("versao"))).startswith("#"))
      .select(chave_hash(col("versao")).alias("id_versao"),
              chave_hash(col("canal"), col("canal_2")).alias("id_canal"),
              upper(trim(col("canal_meta"))).alias("canal_meta"),
              trim(col("segmento")).alias("segmento"),
              trim(col("servico")).alias("servico"),
              mapa_mes[upper(trim(col("mes")))].alias("mes_num"),
              col("target_value")))

if df.filter(col("mes_num").isNull()).count() > 0:
    raise Exception("fact_metas: month name not mapped")
gravar_gold(df, "fact_metas")

# COMMAND ----------

# fact_meta_vendedor
df = (spark.table("dsa_projeto.silver.meta_vendedor")
      .select(chave_hash(col("vendedor")).alias("id_vendedor"),
              trim(col("canal")).alias("canal"),
              "meta", "meta_canal", "spare_parts", "accessories", "consumables"))
gravar_gold(df, "fact_meta_vendedor")

# COMMAND ----------

# fact_planning: unpivot m-3..m5 from the Bronze table, keeping the measure (latam_sales_quota)
b = spark.table("dsa_projeto.bronze.planning_book")

df = (b.filter(upper(trim(col("sales_group"))) != "TOTAL")
      .select(chave_hash(col("sales_group")).alias("id_sales_group"),
              trim(col("latam_sales_quota")).alias("medida"),
              F.regexp_replace(trim(col("apo_product")), "^0+", "").alias("produto"),
              trim(col("unit")).alias("unit"),
              F.expr("""
                  stack(9,
                      '-3', `m-3`, '-2', `m-2`, '-1', `m-1`,
                      '0', m0, '1', m1, '2', m2, '3', m3, '4', m4, '5', m5
                  ) as (mes_offset_txt, value_txt)
              """))
      .select("id_sales_group", "medida", "produto", "unit",
              col("mes_offset_txt").cast("int").alias("mes_offset"),
              col("value_txt").cast("double").alias("value")))
gravar_gold(df, "fact_planning")

# COMMAND ----------

# fact_vendas: grain is document + item (checked unique), full 64-bit key
vendas = spark.table("dsa_projeto.silver.sales_monitoring")
MEDIDAS = {
    "venda_pendente_qtd": "venda_pend",   "venda_pendente_net": "vnd_pndnet",
    "venda_futura_qtd":   "vend_futur",   "venda_futura_net":   "vndfutunet",
    "alocado_qtd":        "aloc_total",   "alocado_net":        "alc_totnet",
    "bloq_credito_qtd":   "vend_blqcr",   "bloq_credito_net":   "vdblqcrnet",
    "faturado_qtd":       "faturado",     "faturado_net":       "faturadnet",
}
CONTEXTO = ["sorg", "dchl", "plnt", "ph1", "saty", "curr"]

df = (vendas.select(
    chave_hash_full(col("document"), col("item")).alias("id_venda"),
    trim(col("document")).alias("documento"),
    trim(col("item")).alias("item"),
    trim(col("cli_emorde")).alias("codigo_cliente"),
    chave_hash(col("sgrp")).alias("id_sales_group"),
    trim(col("material")).alias("material"),
    col("doc_date").cast("date").alias("data_pedido"),
    col("billing_date").cast("date").alias("data_faturamento"),
    *[trim(col(c)).alias(c) for c in CONTEXTO],
    col("net_price"),
    *[col(v).cast("double").alias(k) for k, v in MEDIDAS.items()]))

exigir_chave_unica(df, "id_venda", "fact_vendas")
gravar_gold(df, "fact_vendas")
