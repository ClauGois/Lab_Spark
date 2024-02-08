# Databricks notebook source
from pyspark.sql.functions import *
df = spark.read.format("csv").option("header", True).load("/lab_pyspark_dados/modelo_carro.csv")
df = df.withColumn("preco", regexp_replace("preco","\$",""))
display(df)

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df.createOrReplaceTempView("carros")

# COMMAND ----------

# MAGIC %sql /* tipando dados com sql*/ 
# MAGIC select cast(id_carro as INT), modelo_carro, cast(preco as double), cast(cod_marca as int) from carros

# COMMAND ----------

df_sql = spark.sql("select cast(id_carro as INT), modelo_carro, cast(preco as double), cast(cod_marca as int) from carros")
display(df_sql)

# COMMAND ----------

df_sql.printSchema()

# COMMAND ----------

df_pyspark = df
df_pyspark.printSchema()

# COMMAND ----------

#MANEIRA 1 PYSPARK
df_pyspark = df_pyspark.withColumn("id_carro", col("id_carro").cast("int")).withColumn("preco", col("preco").cast("double"))
df_pyspark.printSchema()

# COMMAND ----------

.withColumn("", col("").cast("int"))

# COMMAND ----------

#MANEIRA 2 PYSPARK
df_pyspark = df_pyspark.select(col("id_carro").cast("int"), col("preco").cast("double"))
df_pyspark.printSchema()

# COMMAND ----------

