# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
df = spark.read.format("csv").option("header", True).load("/lab_pyspark_dados/modelo_carro.csv")
df = df.withColumn("preco", regexp_replace("preco","\$",""))
df.createOrReplaceTempView("carros")
display(df.head(5))

# COMMAND ----------

df.write.format("delta").mode("overwrite").saveAsTable("loans_parquet")

# COMMAND ----------

