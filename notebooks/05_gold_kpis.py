# Databricks notebook source
# MAGIC %md
# MAGIC # 05 - Gold KPI views
# MAGIC Assumptions documented here:
# MAGIC - KPI 2 uses one fixed target version (`VERSAO_ALVO`), year 2026, NS goals only, BRL actuals only.
# MAGIC - KPI 2 needs a sales-group -> channel mapping that is NOT in the source files; it is a documented assumption
# MAGIC   (dominant client channel of each sales group). Coverage is about 98% of revenue.
# MAGIC - KPI 4 uses the measure `Quota` and assumes `m0` = August 2026 (from the ingestion date).
# MAGIC - KPI 3 uses `dim_cliente.vendedor` as the commercial channel; the column mixes people and categories.

# COMMAND ----------

# MAGIC %run ./00_setup

# COMMAND ----------

# KPI 1, 3 and 5 (currency-aware, LEFT JOIN from the fact table)
spark.sql("""
CREATE OR REPLACE VIEW dsa_projeto.gold.kpi_faturamento_mensal AS
SELECT date_trunc('month', data_faturamento) AS mes, curr,
       SUM(faturado_qtd) AS faturado_qtd, SUM(faturado_net) AS faturado_net
FROM dsa_projeto.gold.fact_vendas
WHERE data_faturamento IS NOT NULL
GROUP BY 1, 2
""")

spark.sql("""
CREATE OR REPLACE VIEW dsa_projeto.gold.kpi_ranking_canal_comercial AS
SELECT canal_comercial, curr, faturado_net, faturado_qtd,
       RANK() OVER (PARTITION BY curr ORDER BY faturado_net DESC) AS ranking
FROM (
  SELECT CASE WHEN c.codigo_cliente IS NULL THEN 'SEM CADASTRO'
              ELSE COALESCE(c.vendedor, 'NÃO INFORMADO') END AS canal_comercial,
         f.curr AS curr,
         SUM(f.faturado_net) AS faturado_net, SUM(f.faturado_qtd) AS faturado_qtd
  FROM dsa_projeto.gold.fact_vendas f
  LEFT JOIN dsa_projeto.gold.dim_cliente c ON f.codigo_cliente = c.codigo_cliente
  GROUP BY 1, 2
)
""")

spark.sql("""
CREATE OR REPLACE VIEW dsa_projeto.gold.kpi_distribuicao_canal_regiao AS
SELECT CASE WHEN c.codigo_cliente IS NULL THEN 'SEM CADASTRO'
            ELSE COALESCE(c.canal, 'NÃO INFORMADO') END AS canal,
       CASE WHEN c.codigo_cliente IS NULL THEN 'SEM CADASTRO'
            ELSE COALESCE(c.regiao, 'NÃO INFORMADO') END AS regiao,
       f.curr AS curr,
       SUM(f.faturado_net) AS faturado_net, SUM(f.faturado_qtd) AS faturado_qtd
FROM dsa_projeto.gold.fact_vendas f
LEFT JOIN dsa_projeto.gold.dim_cliente c ON f.codigo_cliente = c.codigo_cliente
GROUP BY 1, 2, 3
""")

# COMMAND ----------

# KPI 2: mapping table (assumption) and view
MAPA_SGRP_CANAL = [
    ("B42", "SEGURADORA"),
    ("B43", "RNSA"), ("B53", "RNSA"),
    ("B44", "RETAIL ON/OFF"), ("B45", "RETAIL ON/OFF"), ("B54", "RETAIL ON/OFF"), ("B55", "RETAIL ON/OFF"),
    ("B46", "EXPORTS"), ("B56", "EXPORTS"),
    ("B58", "DISTRIBUTORS"), ("B60", "DISTRIBUTORS"),
    ("B62", "CANAL DIRETO - PEÇAS"),
    ("B67", "OUTLETS"),
]
df_map = (spark.createDataFrame(MAPA_SGRP_CANAL, ["sales_group", "canal"])
          .select(upper(trim(col("sales_group"))).alias("sales_group"),
                  upper(trim(col("canal"))).alias("canal")))

validos = spark.table("dsa_projeto.gold.dim_canal").select("canal").distinct()
if df_map.join(validos, "canal", "left_anti").count() > 0:
    raise Exception("map_sgrp_canal: a mapped channel is missing from dim_canal")
gravar_gold(df_map, "map_sgrp_canal")

# COMMAND ----------

VERSAO_ALVO = "SOP 08"
ANO_META = 2026

spark.sql(f"""
CREATE OR REPLACE VIEW dsa_projeto.gold.kpi_atingimento_meta_canal AS
WITH meta AS (
  SELECT c.canal, m.mes_num, SUM(m.target_value) AS meta_net
  FROM dsa_projeto.gold.fact_metas m
  JOIN dsa_projeto.gold.dim_versao ver ON m.id_versao = ver.id_versao
  JOIN dsa_projeto.gold.dim_canal c ON m.id_canal = c.id_canal
  WHERE ver.versao = '{VERSAO_ALVO}' AND m.canal_meta = 'NS'
    AND c.tipo_canal = 'CANAL' AND NOT c.eh_ajuste
  GROUP BY c.canal, m.mes_num
),
real AS (
  SELECT mp.canal, month(f.data_faturamento) AS mes_num, SUM(f.faturado_net) AS faturado_net
  FROM dsa_projeto.gold.fact_vendas f
  JOIN dsa_projeto.gold.dim_sales_group sg ON f.id_sales_group = sg.id_sales_group
  JOIN dsa_projeto.gold.map_sgrp_canal mp ON sg.sales_group = mp.sales_group
  WHERE year(f.data_faturamento) = {ANO_META} AND f.curr = 'BRL'
  GROUP BY mp.canal, month(f.data_faturamento)
)
SELECT COALESCE(meta.canal, real.canal) AS canal,
       COALESCE(meta.mes_num, real.mes_num) AS mes_num,
       meta.meta_net, real.faturado_net,
       real.faturado_net / NULLIF(meta.meta_net, 0) AS atingimento
FROM meta FULL OUTER JOIN real ON meta.canal = real.canal AND meta.mes_num = real.mes_num
""")

# COMMAND ----------

# KPI 4
MEDIDA = "Quota"
REF_MES = "2026-08-01"

spark.sql(f"""
CREATE OR REPLACE VIEW dsa_projeto.gold.kpi_planejado_vs_realizado AS
WITH plan AS (
  SELECT id_sales_group, add_months(to_date('{REF_MES}'), mes_offset) AS mes, SUM(value) AS planejado
  FROM dsa_projeto.gold.fact_planning
  WHERE medida = '{MEDIDA}' AND upper(unit) = 'EA'
  GROUP BY 1, 2
),
real AS (
  SELECT id_sales_group, trunc(data_faturamento, 'MM') AS mes, SUM(faturado_qtd) AS realizado
  FROM dsa_projeto.gold.fact_vendas
  WHERE data_faturamento IS NOT NULL
  GROUP BY 1, 2
)
SELECT sg.sales_group, COALESCE(plan.mes, real.mes) AS mes,
       plan.planejado, real.realizado, real.realizado - plan.planejado AS diferenca
FROM plan FULL OUTER JOIN real
  ON plan.id_sales_group = real.id_sales_group AND plan.mes = real.mes
LEFT JOIN dsa_projeto.gold.dim_sales_group sg
  ON sg.id_sales_group = COALESCE(plan.id_sales_group, real.id_sales_group)
""")
