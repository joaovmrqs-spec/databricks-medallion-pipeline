# Databricks notebook source
# MAGIC %md
# MAGIC # 00 - Shared setup
# MAGIC Helpers used by the Gold notebooks. Load them with `%run ./00_setup`.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.functions import col, trim, upper

FAIXA_ID = 1_000_000_000  # dimension keys fall in 0..FAIXA_ID-1; -1 is reserved for "unknown"


def chave_hash(*colunas, faixa=FAIXA_ID):
    """Deterministic key for small dimensions. Range-limited, so uniqueness is checked in every build."""
    partes = [F.coalesce(upper(trim(c.cast("string"))), F.lit("<NULL>")) for c in colunas]
    return F.pmod(F.xxhash64(F.concat_ws("||", *partes)), F.lit(faixa)).cast("int")


def chave_hash_full(*colunas):
    """Full 64-bit key for large tables (one key per fact row)."""
    partes = [F.coalesce(upper(trim(c.cast("string"))), F.lit("<NULL>")) for c in colunas]
    return F.xxhash64(F.concat_ws("||", *partes))


def gravar_gold(df, nome):
    (df.write.mode("overwrite").option("overwriteSchema", "true")
       .saveAsTable(f"dsa_projeto.gold.{nome}"))
    print(nome, "->", spark.table(f"dsa_projeto.gold.{nome}").count(), "rows")


def exigir_chave_unica(df, chave, nome):
    """Quality gate: stops the task if the key is not unique."""
    n = df.count()
    ids = df.select(chave).distinct().count()
    if n != ids:
        raise Exception(f"{nome}: duplicated keys ({n} rows, {ids} unique)")
    print(f"{nome}: key '{chave}' is unique ({n} rows)")
