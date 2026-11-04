# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Bronze — preserva o bruto
# MAGIC Lê a landing com **Auto Loader** e grava os eventos sem transformar nada.
# MAGIC Cada linha guarda de qual arquivo veio (proveniência) e o que não bateu com o
# MAGIC contrato vai para `_rescued`, em vez de quebrar o pipeline.

# COMMAND ----------

dbutils.widgets.text("catalogo", "workspace")
catalogo = dbutils.widgets.get("catalogo")

landing = f"/Volumes/{catalogo}/bronze/landing/loja/"
checkpoints = f"/Volumes/{catalogo}/bronze/landing/_checkpoints/bronze_loja"
tabela_bronze = f"{catalogo}.bronze.loja_eventos"

spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalogo}.bronze.landing")

# COMMAND ----------

# Schema explícito: é o contrato do evento. Assim o Auto Loader não precisa adivinhar
# tipos e funciona até com a landing vazia.
SCHEMA = """
    lsn STRING, lsn_ordem BIGINT, operacao STRING, tabela STRING,
    dados STRUCT<id_loja: INT, nome: STRING, segmento: STRING, atualizado_em: STRING>,
    dados_anteriores STRUCT<id_loja: INT, nome: STRING, segmento: STRING, atualizado_em: STRING>,
    capturado_em STRING
"""

from pyspark.sql import functions as F

consulta = (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("rescuedDataColumn", "_rescued")
    .schema(SCHEMA)
    .load(landing)
    .select(
        "*",
        F.col("_metadata.file_path").alias("_arquivo_origem"),
        F.current_timestamp().alias("_ingerido_em"),
    )
    .writeStream
    .option("checkpointLocation", checkpoints)
    .trigger(availableNow=True)  # processa o que chegou e para: é um job, não fica ligado
    .toTable(tabela_bronze)
)
consulta.awaitTermination()

# COMMAND ----------

display(spark.sql(f"SELECT operacao, dados, _arquivo_origem FROM {tabela_bronze} ORDER BY lsn_ordem"))
