# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
df_carros = spark.read.format("csv").option("header", True).load("/lab_pyspark_dados/modelo_carro.csv")

display(df_carros.head(5))

# COMMAND ----------

df_source = df_carros.filter( 
    (col("id_carro") == '1' ) | (col("id_carro") == '2' ) | (col("id_carro") == '3' )
)

df_final = df_carros.filter( 
    (col("id_carro") == '1' ) | (col("id_carro") == '2' ) | (col("id_carro") == '3') | (col("id_carro") == '4')
)

# COMMAND ----------

display(df_source)
display(df_final)

# COMMAND ----------

df_final.createOrReplaceTempView("carros_source")
df_source.createOrReplaceTempView("carros_final")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from carros_final f where exists(
# MAGIC   select * from carros_source s where s.id_carro = f.id_carro
# MAGIC )

# COMMAND ----------

display(
    df_carros.join( df_source, df_final.id_carro == df_source.id_carro, "leftsemi" )
)

# COMMAND ----------

