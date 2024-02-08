# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
df_carros = spark.read.format("csv").option("header", True).load("/lab_pyspark_dados/modelo_carro.csv")

df_carros = df_carros.withColumn("preco", regexp_replace("preco","\$","").cast(DoubleType()))
display(df_carros)

# COMMAND ----------

df_carros.createOrReplaceTempView("carros")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from carros

# COMMAND ----------

# MAGIC %sql
# MAGIC select SUM(preco) as soma_preco, max(preco) as preco_maximo, min(preco) as preco_minimo from carros

# COMMAND ----------

# MAGIC %sql
# MAGIC select modelo_carro, SUM(preco) as soma_preco, max(preco) as preco_maximo, min(preco) as preco_minimo from carros group by modelo_carro

# COMMAND ----------

display(
    df_carros.groupBy("modelo_carro").agg(sum("preco").alias("soma_preco"),max("preco"),min("preco"))
)

# COMMAND ----------

