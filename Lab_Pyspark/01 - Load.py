# Databricks notebook source
df1 = spark.read.format("csv").option("header", "true").load("dbfs:/FileStore/shared_uploads/claudio_locoxd1@hotmail.com/marcas_duplicadas.csv")
df2 = spark.read.format("csv").option("header", "true").load("dbfs:/FileStore/shared_uploads/claudio_locoxd1@hotmail.com/marca_carro.csv")
df3 = spark.read.format("csv").option("header", "true").load("dbfs:/FileStore/shared_uploads/claudio_locoxd1@hotmail.com/modelo_carro.csv")

# COMMAND ----------

display(df1)

# COMMAND ----------

# overwrite sobrescreve : df1.write.format("csv").mode("overwrite").save("/lab_pyspark_dados/marcas_duplicadas.csv")
# append adiciona ao arquivo : df1.write.format("csv").mode("append").save("/lab_pyspark_dados/marcas_duplicadas.csv")
df1.write.format("csv").option("header", "true").mode("overwrite").save("/lab_pyspark_dados/marcas_duplicadas.csv")
df2.write.format("csv").option("header", "true").mode("overwrite").save("/lab_pyspark_dados/marca_carro.csv")
df3.write.format("csv").option("header", "true").mode("overwrite").save("/lab_pyspark_dados/modelo_carro.csv")

# COMMAND ----------

display(df3)

# COMMAND ----------

df = spark.read.format("csv").option("header", True).load("/lab_pyspark_dados/modelo_carro.csv")
display(df)