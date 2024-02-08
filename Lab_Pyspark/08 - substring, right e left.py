# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
df = spark.read.format("csv").option("header", True).load("/lab_pyspark_dados/modelo_carro.csv")
df = df.withColumn("preco", regexp_replace("preco","\$",""))
df.createOrReplaceTempView("carros")
display(df.head(5))

# COMMAND ----------

# MAGIC %sql
# MAGIC select modelo_carro, SUBSTRING(modelo_carro, 2,3) from carros

# COMMAND ----------

# MAGIC %sql
# MAGIC select modelo_carro, SUBSTRING(modelo_carro, 2,3), LEFT(modelo_carro, 2), RIGHT(modelo_carro, 4) from carros

# COMMAND ----------

display( 
    df.withColumn("modelo_sub", substring("modelo_carro", 2,3)).withColumn("modelo_right", expr("RIGHT(modelo_carro, 2)"))
)

# COMMAND ----------

