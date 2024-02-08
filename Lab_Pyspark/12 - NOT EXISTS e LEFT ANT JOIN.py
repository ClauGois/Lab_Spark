# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
df_carros = spark.read.format("csv").option("header", True).load("/lab_pyspark_dados/modelo_carro.csv")

df_source = df_carros.filter( 
    (col("id_carro") == '1' ) | (col("id_carro") == '2' ) | (col("id_carro") == '3' )
)

df_final = df_carros.filter( 
    (col("id_carro") == '1' ) | (col("id_carro") == '2' ) | (col("id_carro") == '3') | (col("id_carro") == '4')
)

# COMMAND ----------


df_source.createOrReplaceTempView("carros_source")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from carros_final

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from carros_source

# COMMAND ----------


select * from carros_final f where not exists(
  select 1 from carros_source s where s.id_carro == f.id_carro
)

# COMMAND ----------

display(
    df_final.join( df_source, (df_final.id_carro == df_source.id_carro), "leftanti" )
)

# COMMAND ----------

