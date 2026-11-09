# Databricks notebook source
# MAGIC %md
# MAGIC # 06 - Validation (quality gate)
# MAGIC Raises an exception when a check fails, so the Job stops with a clear message.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.functions import col, trim, upper

G = "dsa_projeto.gold"
erros = []


def checar(nome, ok, detalhe=""):
    print(("OK   " if ok else "FAIL "), nome, detalhe)
    if not ok:
        erros.append(nome)


def orfaos(fato, dim, chave):
    return spark.table(f"{G}.{fato}").join(spark.table(f"{G}.{dim}"), chave, "left_anti").count()


# COMMAND ----------

for dim, chave in [("dim_cliente", "codigo_cliente"), ("dim_vendedor", "id_vendedor"),
                   ("dim_canal", "id_canal"), ("dim_versao", "id_versao"),
                   ("dim_sales_group", "id_sales_group")]:
    t = spark.table(f"{G}.{dim}")
    checar(f"{dim}: unique key", t.count() == t.select(chave).distinct().count())

vendas_s = spark.table("dsa_projeto.silver.sales_monitoring")
checar("fact_vendas rows = Silver", spark.table(f"{G}.fact_vendas").count() == vendas_s.count())

metas_s = spark.table("dsa_projeto.silver.metas")
esperado = metas_s.filter(~upper(trim(col("versao"))).startswith("#")).count()
checar("fact_metas rows = Silver minus quarantine", spark.table(f"{G}.fact_metas").count() == esperado)

# COMMAND ----------

checar("fact_metas -> dim_canal", orfaos("fact_metas", "dim_canal", "id_canal") == 0)
checar("fact_metas -> dim_versao", orfaos("fact_metas", "dim_versao", "id_versao") == 0)
checar("fact_meta_vendedor -> dim_vendedor", orfaos("fact_meta_vendedor", "dim_vendedor", "id_vendedor") == 0)
checar("fact_planning -> dim_sales_group", orfaos("fact_planning", "dim_sales_group", "id_sales_group") == 0)

nulos_sgrp = vendas_s.filter(col("sgrp").isNull()).count()
checar("fact_vendas -> dim_sales_group (only null sgrp may be orphan)",
       orfaos("fact_vendas", "dim_sales_group", "id_sales_group") <= nulos_sgrp)

tot = spark.table(f"{G}.fact_vendas").agg(F.sum("faturado_net")).first()[0]
for vw in ["kpi_ranking_canal_comercial", "kpi_distribuicao_canal_regiao"]:
    s = spark.table(f"{G}.{vw}").agg(F.sum("faturado_net")).first()[0]
    checar(f"{vw} total = fact total", abs(tot - s) < 0.01, f"diff={abs(tot - s):.6f}")

# COMMAND ----------

if erros:
    raise Exception("Quality gate failed: " + ", ".join(erros))
print("All checks passed")
