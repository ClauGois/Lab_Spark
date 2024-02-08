# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
df = spark.read.format("csv").option("header", True).load("/lab_pyspark_dados/modelo_carro.csv")
df = df.withColumn("preco", regexp_replace("preco","\$",""))
display(df.head(5))

# COMMAND ----------

df = df.select(col("id_carro").cast("int"),"modelo_carro", col("preco").cast("double"), col("cod_marca").cast("int"))
df.printSchema()

# COMMAND ----------

df.createOrReplaceTempView("carros")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from carros where modelo_carro like "%ager"

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from carros where modelo_carro like "Ava%"

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from carros where preco between 50000 and 750000

# COMMAND ----------

# MAGIC %md # #   PySpark
# MAGIC

# COMMAND ----------

df_pyspark = df
display(
    df_pyspark.filter(col("modelo_carro").like("%rt%"))
)

# COMMAND ----------

display(
    df_pyspark.filter(col("preco").between(50000,75000))
)

# COMMAND ----------

