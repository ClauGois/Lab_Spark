# Databricks notebook source
df = spark.read.format("csv").option("header", True).load("/lab_pyspark_dados/modelo_carro.csv")

# COMMAND ----------

display(df)

# COMMAND ----------

df.createOrReplaceTempView("carros")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from carros where id_carro = 1 or modelo_carro = "Avalon"

# COMMAND ----------

#criando um dataframe a partir de um select de uma view
df_carros_sql = spark.sql("select * from carros where id_carro = 1 or modelo_carro = 'Golf' ")
display(df_carros_sql)

# COMMAND ----------

df_carros_spark = df.filter((df["id_carro"]=='1') | (df["modelo_carro"]=="Golf")) # filter e where são a mesma coisa
display(df_carros_spark)

# COMMAND ----------

#modelo 1 
display(
    df.where("id_carro = '1'")
)

# COMMAND ----------

#modelo 2 
display(
    df.filter("id_carro = '1'")
)

# COMMAND ----------

#modelo 2
from pyspark.sql.functions import *
display(
    df.where((col("id_carro")=="1") | (col("modelo_carro")=="Golf"))
)

# COMMAND ----------

#modelo 2
from pyspark.sql.functions import *
display(
    df.where((df["id_carro"]=='1') | (df["modelo_carro"]=="Golf"))
)