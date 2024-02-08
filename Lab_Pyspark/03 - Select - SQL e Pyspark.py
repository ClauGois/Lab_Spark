# Databricks notebook source
df = spark.read.format("csv").option("header", True).load("/lab_pyspark_dados/modelo_carro.csv")
display(df)

# COMMAND ----------

#criando uma view temporaria no spark
df.createOrReplaceTempView("carros")

# COMMAND ----------

# MAGIC %sql /* lendo com sql a view temporaria do spark */
# MAGIC select id_carro, modelo_carro AS modelo from carros 

# COMMAND ----------

# sql com spark
df_sql = spark.sql("select id_carro, modelo_carro AS modelo from carros")
display(df_sql) 

# COMMAND ----------

#selecionando coluna com spark
df_carros_spark = df.select("id_carro","modelo_carro")
display(df_carros_spark)

# COMMAND ----------

#renomeando coluna 1
df_carros_spark = df.selectExpr("id_carro","modelo_carro AS modelo")
display(df_carros_spark)

# COMMAND ----------

#renomeando coluna 2
from pyspark.sql.functions import col
df_carros_spark = df.select("id_carro",col("modelo_carro").alias("modelo"))
display(df_carros_spark)