# Databricks notebook source

# COMMAND ----------

# 1. Trazer a tabela bronze e olhar o schema atual da meta_vendedor

df_bronze = spark.table("dsa_projeto.bronze.meta_vendedor")
df_bronze.printSchema()     #mostra o tipo de cada coluna
display(df_bronze)  #printa na tela a tabela


# COMMAND ----------

# 2. Transformação da silver
from pyspark.sql.functions import col, trim, to_timestamp

df_silver = df_bronze.select(   #select() permite criar novo dataframe e não modificar as tabelas aonde elas estão armazenadas.
    trim(col("canal")).alias("canal"),  #trim remove espaços em branco no início e fim do texto
    trim(col("vendedor")).alias("vendedor"),    #alias renomeia a coluna
    col("meta").cast("double").alias("meta"),   #cast(double) converte o texto para decimal.
    col("meta_canal").cast("double").alias("meta_canal"),
    col("spare_parts").cast("double").alias("spare_parts"),
    col("accessories").cast("double").alias("accessories"),
    col("consumables").cast("double").alias("consumables"),
    to_timestamp(col("_ingestion_timestamp")).alias("data_ingestao_fonte"), #to_timestamp() converte texto para timestamp.
    col("_arquivo_origem"),
    col("_ingestion_timestamp")
)

df_silver.printSchema()
display(df_silver)


# COMMAND ----------

# 3. Validação importante antes da gravação
from pyspark.sql.functions import count, when

df_silver.select([
    count(when(col(c).isNull(), c)).alias(c) for c in ["meta", "meta_canal", "spare_parts", "accessories", "consumables"]
]).show()

# aqui estamos contando nulos, pois .cast() que falha, não dá erro.


# COMMAND ----------

# 4. Gravar como tabela silver
df_silver.write.mode("overwrite").saveAsTable("dsa_projeto.silver.meta_vendedor")


# COMMAND ----------

# 5. Validação final da tabela gravada
df_check = spark.table("dsa_projeto.silver.meta_vendedor")
df_check.printSchema()
print(f"Total de linhas: {df_check.count()}")
display(df_check)


# COMMAND ----------

# 1. Trazer a tabela bronze e olhar o schema atual da relacao_de_clientes

df_bronze_clientes = spark.table("dsa_projeto.bronze.relacao_de_clientes")
df_bronze_clientes.printSchema()
display(df_bronze_clientes)


# COMMAND ----------

# 3. Colocando linhas suspeitas da an8 em quarentena (linhas com '"')

#linhas contaminadas
df_quarentena_clientes = df_bronze_clientes.filter(col("an8").contains('"'))

#linhas boas:
df_bronze_clientes_ok = df_bronze_clientes.filter(~col("an8").contains('"'))

print("linhas em quarentena: ", df_quarentena_clientes.count())
print("linhas boas: ", df_bronze_clientes_ok.count())

#gravamos a quarentena numa tabela separada para alguém revisar manualmente depois:
df_quarentena_clientes.write.mode("overwrite").saveAsTable("dsa_projeto.bronze.relacao_de_clientes_quarentena")


# COMMAND ----------

# 2. Transformação da silver para a relacao_de_clientes
from pyspark.sql.functions import col, trim, to_timestamp

df_silver_clientes_ok = df_bronze_clientes_ok.select(
    trim(col("an8")).alias("an8"),
    trim(col("razao_social")).alias("razao_social"),
    trim(col("nome_grp")).alias("nome_grp"),
    trim(col("cnpj")).alias("cnpj"),
    trim(col("vendedor")).alias("vendedor"),
    trim(col("channel")).alias("channel"),
    trim(col("sub_channel")).alias("sub_channel"),
    trim(col("cidade")).alias("cidade"),
    trim(col("uf")).alias("uf"),
    trim(col("regiao")).alias("regiao"),
    trim(col("pais")).alias("pais"),
    trim(col("payer")).alias("payer"),
    trim(col("tabela_de_preco_c_a")).alias("tabela_de_preco_c_a"),
    trim(col("tabela_de_preco_spt")).alias("tabela_de_preco_spt"),
    trim(col("tabela_de_preco_sda")).alias("tabela_de_preco_sda"),
    col("desconto_c_a").try_cast("double").alias("desconto_c_a"),
    col("desconto_spt").try_cast("double").alias("desconto_spt"),
    col("desconto_sda").try_cast("double").alias("desconto_sda"),
    trim(col("kam_code")).alias("kam_code"),
    trim(col("sales_group_wg")).alias("sales_group_wg"),
    trim(col("sales_group_fc")).alias("sales_group_fc"),
    col("5o_no"),
    col("verificacao_grupo"),
    col("supervisor"),
    col("endereco"),
    col("cep"),
    col("status_vtex"),
    col("nome_org_vtex"),
    col("e_mail"),
    col("tabela_acessorios"),
    col("tabela_consumiveis"),
    col("tabela_pecas"),
    col("desconto_acessorios").try_cast("double").alias("descontos_acessorios"),
    col("descontos_consumiveis").try_cast("double").alias("descontos_consumiveis"),
    col("descontos_pecas").try_cast("double").alias("descontos_pecas"),
    col("tag_acessorios"),
    col("tag_consumiveis"),
    col("tag_pecas"),
    col("classificacao_rnsa"),
    to_timestamp(col("_ingestion_time")).alias("data_ingestao_fonte"),
    col("_arquivo_origem"),
    col("_ingestion_timestamp")
)

df_silver_clientes_ok.printSchema()
display(df_silver_clientes_ok)


# COMMAND ----------

# 3. gravando tabela na silver

df_silver_clientes_ok.write.mode("overwrite").saveAsTable("dsa_projeto.silver.relacao_de_clientes")


# COMMAND ----------

# checagem final:

df_check = spark.table("dsa_projeto.silver.relacao_de_clientes")
print("Total de linhas na tabela silver gravada: ", df_check.count())
df_check.printSchema()


# COMMAND ----------

# 1. Trazer nova tabela: 
df_bronze = spark.table("dsa_projeto.bronze.sales_monitoring")
df_bronze.printSchema() #mostra tipo de cada coluna
display(df_bronze)  #printa tabela na tela


# COMMAND ----------

# 2. transformação da silver
from pyspark.sql.functions import col, trim, to_timestamp, regexp_replace, concat_ws

df_silver = df_bronze.select(
    trim(col("document")).alias("document"),
    trim(col("item")).alias("item"),
    trim(col("cocd")).alias("cocd"),
    trim(col("sorg")).alias("sorg"),
    trim(col("dchl")).alias("dchl"),
    trim(col("dv")).alias("dv"),
    trim(col("sgrp")).alias("sgrp"),
    trim(col("saty")).alias("saty"),
    to_timestamp(col("doc_date"),"dd.MM.yyyy").alias("doc_date"),
    trim(col("material")).alias("material"),
    trim(col("mtyp")).alias("mtyp"),
    trim(col("plnt")).alias("plnt"),
    trim(col("cli_emorde")).alias("cli_emorde"),
    trim(col("customer_nome")).alias("customer_nome"),
    trim(col("city")).alias("city"),
    trim(col("rg")).alias("region"),
    trim(col("ctr")).alias("ctr"),
    trim(col("key_manage")).alias("key_manage"),
    trim(col("kunnr_desc")).alias("kunnr_desc"),
    trim(col("product_hierarchy")).alias("product_hierarchy"),
    trim(col("descricao_hierarquia")).alias("descricao_hierarquia"),
    trim(col("ph1")).alias("ph1"),
    regexp_replace(regexp_replace(col("net_price"), r"\.",""), ",", ".").cast("double").alias("net_price"),
    trim(col("curr")).alias("curr"),
    to_timestamp(col("gi_date"), "dd.MM.yyyy").alias("gi_date"),
    to_timestamp(col("bill_date"), "dd.MM.yyyy").alias("billing_date"),
    regexp_replace(regexp_replace(col("vend_blqcr"), r"\.",""), ",", ".").cast("double").alias("vend_blqcr"),
    regexp_replace(regexp_replace(col("vdblqcrnet"), r"\.",""), ",", ".").cast("double").alias("vdblqcrnet"),
    regexp_replace(regexp_replace(col("vnd_bloqtt"), r"\.",""), ",", ".").cast("double").alias("vnd_bloqtt"),
    regexp_replace(regexp_replace(col("vndbttnet"), r"\.",""), ",", ".").cast("double").alias("vndbttnet"),
    regexp_replace(regexp_replace(col("vend_futur"), r"\.",""), ",", ".").cast("double").alias("vend_futur"),
    regexp_replace(regexp_replace(col("vndfutunet"), r"\.",""), ",", ".").cast("double").alias("vndfutunet"),
    regexp_replace(regexp_replace(col("venda_pend"), r"\.",""), ",", ".").cast("double").alias("venda_pend"),
    regexp_replace(regexp_replace(col("vnd_pndnet"), r"\.",""), ",", ".").cast("double").alias("vnd_pndnet"),
    regexp_replace(regexp_replace(col("aloc_total"), r"\.",""), ",", ".").cast("double").alias("aloc_total"),
    regexp_replace(regexp_replace(col("alc_totnet"), r"\.",""), ",", ".").cast("double").alias("alc_totnet"),
    regexp_replace(regexp_replace(col("faturado"), r"\.",""), ",", ".").cast("double").alias("faturado"),
    regexp_replace(regexp_replace(col("faturadnet"), r"\.",""), ",", ".").cast("double").alias("faturadnet"),
    regexp_replace(regexp_replace(col("fat_liquid"), r"\.",""), ",", ".").cast("double").alias("fat_liquid"),
    regexp_replace(regexp_replace(col("fatliqnet"), r"\.",""), ",", ".").cast("double").alias("fatliqnet"),
    regexp_replace(regexp_replace(col("projecao_1"), r"\.",""), ",", ".").cast("double").alias("projecao_1"),
    regexp_replace(regexp_replace(col("projec1net"), r"\.",""), ",", ".").cast("double").alias("projec1net"),
    regexp_replace(regexp_replace(col("venda_tota"), r"\.",""), ",", ".").cast("double").alias("venda_tota"),
    regexp_replace(regexp_replace(col("vend_t_net"), r"\.",""), ",", ".").cast("double").alias("vend_t_net"),
    regexp_replace(regexp_replace(col("vendliquid"), r"\.",""), ",", ".").cast("double").alias("vendliquid"),
    regexp_replace(regexp_replace(col("vndliqnet"), r"\.",""), ",", ".").cast("double").alias("vndliqnet"),
    regexp_replace(regexp_replace(col("projecao_3"), r"\.",""), ",", ".").cast("double").alias("projecao_3"),
    regexp_replace(regexp_replace(col("proje3net"), r"\.",""), ",", ".").cast("double").alias("proje3net"),
    regexp_replace(regexp_replace(col("fat_contab"), r"\.",""), ",", ".").cast("double").alias("fat_contab"),
    regexp_replace(regexp_replace(col("projeccont"), r"\.",""), ",", ".").cast("double").alias("projeccont"),
    regexp_replace(regexp_replace(col("projctbnet"), r"\.",""), ",", ".").cast("double").alias("projctbnet"),
    to_timestamp(concat_ws(" ", col("date"), col("time")),"dd.MM.yyyy HH:mm:ss").alias("data_hora"),
    regexp_replace(regexp_replace(col("pend_ordme"), r"\.",""), ",", ".").cast("double").alias("pend_ordme"),
    regexp_replace(regexp_replace(col("pndordment"), r"\.",""), ",", ".").cast("double").alias("pndordment")


)
df_silver.printSchema()
display(df_silver)


# COMMAND ----------

# 3. Gravando tabela na silver:

df_silver.write.mode("overwrite").saveAsTable("dsa_projeto.silver.sales_monitoring")


# COMMAND ----------

# 4. Checagem final
df_check = spark.table("dsa_projeto.silver.sales_monitoring")
print("Total de linhas na tabela silver gravada: ", df_check.count())


# COMMAND ----------

#1. Trazer nova tabela da bronze (metas NS)

df_bronze_NS = spark.table("dsa_projeto.bronze.metas_ns")
df_bronze_NS.printSchema()
display(df_bronze_NS)

#1.01. Trazer nova tabela da bronze (metas CTF)

df_bronze_CTF = spark.table("dsa_projeto.bronze.metas_ctf")
df_bronze_CTF.printSchema()
display(df_bronze_CTF)


# COMMAND ----------

from pyspark.sql.functions import col, trim, lit, to_timestamp, count, when, expr

#2. Incluindo colunas flag NS e CTF e transformando demais colunas 

df_silver_NS = df_bronze_NS.select(
    trim(col("versao")).alias("versao"),
    trim(col("segmento")).alias("segmento"),
    trim(col("servico")).alias("servico"),
    trim(col("canal")).alias("canal"),
    trim(col("canal_2")).alias("canal_2"),
    col("janeiro").cast("double").alias("Jan"),
    col("fevereiro").cast("double").alias("Fev"),
    col("marco").cast("double").alias("Mar"),
    col("abril").cast("double").alias("Abr"),
    col("maio").cast("double").alias("Mai"),
    col("junho").cast("double").alias("Jun"),
    col("julho").cast("double").alias("Jul"),
    col("agosto").cast("double").alias("Ago"),
    col("setembro").cast("double").alias("Set"),
    col("outubro").cast("double").alias("Out"),
    col("novembro").cast("double").alias("Nov"),
    col("dezembro").cast("double").alias("Dez"),
    lit("NS").alias("canal_meta")
)
df_silver_NS_long = df_silver_NS.select(
    "versao", "segmento", "servico", "canal", "canal_2", "canal_meta",
    expr("""
    stack(12, 'Jan', Jan, 'Fev', Fev, 'Mar', Mar, 'Abr', Abr, 'Mai', Mai, 'Jun', Jun, 'Jul', Jul, 'Ago', Ago, 'Set', Set, 'Out', Out, 'Nov', Nov, 'Dez', Dez)
    as (mes, target_value)
    """)
)

df_silver_CTF = df_bronze_CTF.select(
    trim(col("versao")).alias("versao"),
    trim(col("segmento")).alias("segmento"),
    trim(col("servico")).alias("servico"),
    trim(col("canal")).alias("canal"),
    trim(col("canal_2")).alias("canal_2"),
    col("janeiro").cast("double").alias("Jan"),
    col("fevereiro").cast("double").alias("Fev"),
    col("marco").cast("double").alias("Mar"),
    col("abril").cast("double").alias("Abr"),
    col("maio").cast("double").alias("Mai"),
    col("junho").cast("double").alias("Jun"),
    col("julho").cast("double").alias("Jul"),
    col("agosto").cast("double").alias("Ago"),
    col("setembro").cast("double").alias("Set"),
    col("outubro").cast("double").alias("Out"),
    col("novembro").cast("double").alias("Nov"),
    col("dezembro").cast("double").alias("Dez"),
    lit("CTF").alias("canal_meta")
)
df_silver_CTF_long = df_silver_CTF.select(
    "versao", "segmento", "servico", "canal", "canal_2", "canal_meta",
    expr("""
    stack(12, 'Jan', Jan, 'Fev', Fev, 'Mar', Mar, 'Abr', Abr, 'Mai', Mai, 'Jun', Jun, 'Jul', Jul, 'Ago', Ago, 'Set', Set, 'Out', Out, 'Nov', Nov, 'Dez', Dez)
    as (mes, target_value)
    """)
)

display(df_silver_NS_long)
display(df_silver_CTF_long)


# COMMAND ----------

#3. Unindo as tabelas LONG

df_silver_metas = df_silver_NS_long.unionByName(df_silver_CTF_long)
print("NS:", df_silver_NS_long.count())
print("CTF:", df_silver_CTF_long.count())
print("Esperado:", df_silver_NS_long.count() + df_silver_CTF_long.count())
print("Real (union):", df_silver_metas.count())
display(df_silver_metas)


# COMMAND ----------

#4. Gravando a tabela na Silver
df_silver_metas.write.mode("overwrite").saveAsTable("dsa_projeto.silver.metas")


# COMMAND ----------

# 1. Chamando a planning_book

df_bronze_planning = spark.read.table("dsa_projeto.bronze.planning_book")

df_bronze_planning.printSchema()
display(df_bronze_planning)


# COMMAND ----------

from pyspark.sql.functions import col, trim, lit, to_timestamp, count, when, expr

# 2. Tratando colunas da planning_book
df_silver_planning = df_bronze_planning.select(
    trim(col("sales_group")).alias("sales_group"),
    trim(col("unit")).alias("unit"),
    col("m-3").cast("double").alias("valor_m-3"),
    col("m-2").cast("double").alias("valor_m-2"),
    col("m-1").cast("double").alias("valor_m-1"),
    col("m0").cast("double").alias("valor_m0"),
    col("m1").cast("double").alias("valor_m1"),
    col("m2").cast("double").alias("valor_m2"),
    col("m3").cast("double").alias("valor_m3"),
    col("m4").cast("double").alias("valor_m4"),
    col("m5").cast("double").alias("valor_m5")
)

display(df_silver_planning)


# COMMAND ----------

# 3. Pivotando colunas mX
df_silver_planning_long = df_silver_planning.select(
    "sales_group", "unit",
    expr("""
        stack(9, 
            'm-3', `valor_m-3`, 
            'm-2', `valor_m-2`, 
            'm-1', `valor_m-1`,
            'm0', valor_m0,
            'm1', valor_m1,
            'm2', valor_m2,
            'm3', valor_m3,
            'm4', valor_m4,
            'm5', valor_m5
        ) as (mX, value)
    """)
)
display(df_silver_planning_long)


# COMMAND ----------

# 4. Gravar tabela na silver

df_silver_planning_long.write.mode("overwrite").saveAsTable("dsa_projeto.silver.planning_book")

