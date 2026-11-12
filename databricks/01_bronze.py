# Databricks notebook source
# MAGIC %md
# MAGIC # 01 - Bronze (Auto Loader: incremental e idempotente)
# MAGIC Fluxo: suba o **Lote 1** -> `ETAPA="lote1"` -> rode -> suba o **Lote 2** -> `ETAPA="lote2"` -> rode -> `ETAPA="rerun_lote2"` -> rode de novo (sem arquivo novo): a contagem não muda.

# COMMAND ----------
from pyspark.sql import functions as F

BASE = "/Volumes/workspace/gs/chegada"
FONTES = {"sp156": "bronze_sp156", "chuva": "bronze_chuva", "ibge": "bronze_ibge", "ocorrencias": "bronze_ocorrencias"}

def registrar(etapa, tabela):
    qtd = spark.table(f"workspace.gs.{tabela}").count()
    spark.sql(f"INSERT INTO workspace.gs.log_contagens VALUES (current_timestamp(), '{etapa}', '{tabela}', {qtd})")
    print(f"{etapa:<22} {tabela:<22} {qtd}")

def ingerir(pasta, tabela):
    q = (spark.readStream.format("cloudFiles")
         .option("cloudFiles.format", "json")
         .option("cloudFiles.schemaLocation", f"{BASE}/_controle/schema_{pasta}")
         .option("cloudFiles.inferColumnTypes", "true")
         .load(f"{BASE}/{pasta}")
         .withColumn("arquivo_origem", F.col("_metadata.file_path"))
         .withColumn("carregado_em", F.current_timestamp())
         .writeStream
         .option("checkpointLocation", f"{BASE}/_controle/checkpoint_{pasta}")
         .option("mergeSchema", "true")
         .trigger(availableNow=True)
         .toTable(f"workspace.gs.{tabela}"))
    q.awaitTermination()

# COMMAND ----------
ETAPA = "lote1"   # <- mude a cada execução: lote1, lote2, rerun_lote2

for pasta, tab in FONTES.items():
    if not dbutils.fs.ls(f"{BASE}/{pasta}"):
        print(f"(pasta {pasta} vazia, pulando)")
        continue
    if spark.catalog.tableExists(f"workspace.gs.{tab}"):
        registrar(f"antes_{ETAPA}", tab)
    ingerir(pasta, tab)
    registrar(f"depois_{ETAPA}", tab)

# COMMAND ----------
# MAGIC %sql
# MAGIC SELECT * FROM workspace.gs.log_contagens ORDER BY momento, tabela;
