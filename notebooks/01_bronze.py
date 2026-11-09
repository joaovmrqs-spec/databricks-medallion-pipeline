# Databricks notebook source
# MAGIC %md
# MAGIC # 01 - Bronze ingestion
# MAGIC Reads every `.xlsx` in the Volume and writes one Delta table per file, all columns as text.
# MAGIC Real nulls stay null (an earlier version turned them into the string "nan").

# COMMAND ----------

# MAGIC %pip install openpyxl

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import pandas as pd
from pyspark.sql.functions import lit, current_timestamp

caminho_volume = "/Volumes/dsa_projeto/bronze/arquivos_origem/"
arquivos = [a for a in dbutils.fs.ls(caminho_volume) if a.name.endswith(".xlsx")]

# COMMAND ----------

resumo = []

for arquivo in arquivos:
    nome_arquivo = arquivo.name
    nome_tabela = nome_arquivo.replace(".xlsx", "").lower()

    df_pandas = pd.read_excel(caminho_volume + nome_arquivo, sheet_name=0)
    df_pandas.columns = [str(c).strip().lower().replace(" ", "_") for c in df_pandas.columns]

    # everything as text, but keep real nulls as null
    df_pandas = df_pandas.apply(lambda c: c.map(lambda x: None if pd.isna(x) else str(x)))

    df_spark = (spark.createDataFrame(df_pandas)
                .withColumn("_arquivo_origem", lit(nome_arquivo))
                .withColumn("_ingestion_timestamp", current_timestamp()))

    df_spark.write.mode("overwrite").saveAsTable(f"dsa_projeto.bronze.{nome_tabela}")
    resumo.append((nome_tabela, df_pandas.shape[0], df_pandas.shape[1]))
    print(f"ok {nome_tabela}: {df_pandas.shape[0]} rows")

# COMMAND ----------

if len(resumo) != 13:
    raise Exception(f"Expected 13 Bronze tables, ingested {len(resumo)}")
print("Bronze finished:", len(resumo), "tables")
