# Databricks notebook source
df_carros = spark.read.format("csv").option("header", "true").option("enconding", "utf-8").load("/lab_pyspark_dados/modelo_carro.csv", sep=",")

# COMMAND ----------

display(df_carros)

# COMMAND ----------

df_carros.write.format("parquet").save("/lab_pyspark_dados/modelo_carro_parquet")
df_carros.write.format("avro").save("/lab_pyspark_dados/modelo_carro_avro")
df_carros.write.format("json").save("/lab_pyspark_dados/modelo_carro_json")

# COMMAND ----------

df_json = spark.read.format("json").load("/lab_pyspark_dados/modelo_carro_json")
display(df_json)