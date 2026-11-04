# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Silver — estado atual com MERGE idempotente
# MAGIC Lê a bronze como stream e aplica cada lote com `MERGE`. O DELETE da origem
# MAGIC vira DELETE aqui: é a prova de que o CDC funcionou.

# COMMAND ----------

import os
import sys

# o notebook roda de dentro do Git folder; o código testado fica em src/
sys.path.append(os.path.abspath("../src"))
from lab_cdc.silver import aplicar_lote, criar_silver

dbutils.widgets.text("catalogo", "workspace")
catalogo = dbutils.widgets.get("catalogo")

tabela_bronze = f"{catalogo}.bronze.loja_eventos"
tabela_silver = f"{catalogo}.silver.loja"
checkpoints = f"/Volumes/{catalogo}/bronze/landing/_checkpoints/silver_loja"

criar_silver(spark, tabela_silver)

# COMMAND ----------

consulta = (
    spark.readStream.table(tabela_bronze)
    .writeStream
    .foreachBatch(lambda df, _id: aplicar_lote(spark, df, tabela_silver))
    .option("checkpointLocation", checkpoints)
    .trigger(availableNow=True)
    .start()
)
consulta.awaitTermination()

# COMMAND ----------

# MAGIC %md
# MAGIC ## A prova do delete
# MAGIC A loja 2 foi apagada na origem: ela não pode aparecer aqui. A loja 1 tem que estar como PF.

# COMMAND ----------

display(spark.sql(f"SELECT * FROM {tabela_silver} ORDER BY id_loja"))
