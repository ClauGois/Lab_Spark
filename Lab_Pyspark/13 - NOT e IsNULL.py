# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
df_carros = spark.read.format("csv").option("header", True).load("/lab_pyspark_dados/modelo_carro.csv")

df_carros_2 = df_carros.filter( 
    (col("id_carro") == '1' ) | (col("id_carro") == '2' ) | (col("id_carro") == '3' )
)

df_carros_3 = df_carros.filter(col("id_carro") == 4).withColumn("preco",lit(None))

display(df_carros_2)
display(df_carros_3)

# COMMAND ----------

df_carros = df_carros_2.union(df_carros_3)
display(df_carros)

# COMMAND ----------

df_carros.createOrReplaceTempView("carros")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from carros where preco is NULL

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from carros where preco is NOT NULL

# COMMAND ----------

display(
    df_carros.filter((col("preco").isNull()))
)

# COMMAND ----------

display(
    df_carros.filter(~(col("preco").isNull()))
)