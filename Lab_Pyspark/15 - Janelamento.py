# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
df  = spark.read.format("csv").option("header", True).load("/lab_pyspark_dados/modelo_carro.csv")
df = df.withColumn("preco", regexp_replace("preco","\$","").cast(DoubleType()))
df.createOrReplaceTempView("carros")

# COMMAND ----------

# MAGIC %sql
# MAGIC select *, row_number() over(partition by modelo_carro order by preco asc) row_number from carros

# COMMAND ----------

from pyspark.sql.window import Window
display(
    df.withColumn( "row_number", row_number().over(
        Window.partitionBy("modelo_carro").orderBy(col("preco").asc()) ) )
)

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

