# Databricks notebook source
dados =[("Grimaldo","Oliveira","Brasileira","Professor","M",3000),
        ("Ana ","Santos","Portuguesa","Atriz","F",4000),
        ("Roberto","Carlos","Francesa","Analista","M",4000),
        ("Maria","Santanna","Italiana","Dentista","F",6000),
        ("Jeane","Andrade","Portuguesa","Medica","F",7000)]
colunas=["Primeiro_Nome","Ultimo_nome","Nacionalidade","Trabalho","Genero","Salario"]
datafparquet=spark.createDataFrame(dados,colunas)
datafparquet.show()

# COMMAND ----------

#criando o arquivo parquet
datafparquet.write.parquet("/FileStore/tables/parquet/pessoal.parquet")


# COMMAND ----------

#Permite uma atualização do arquivo parquet
datafparquet.write.mode('overwrite').parquet('/FileStore/tables/parquet/pessoal.parquet')


# COMMAND ----------

datafleitura=spark.read.parquet("/FileStore/tables/parquet/pessoal.parquet")
datafleitura.show()


# COMMAND ----------

datafleitura=spark.read.parquet("/FileStore/tables/parquet/pessoal.parquet")
datafleitura.show()

# COMMAND ----------

datafleitura.createOrReplaceTempView("datafl")

# COMMAND ----------

consulta = spark.sql('select * from datafl')
consulta.show()

# COMMAND ----------

datafleitura.write.partitionBy("Nacionalidade","Salario").mode("Overwrite").parquet("FileStore/tables/parquet/pessoal.parquet")

# COMMAND ----------

datafnacional=spark.read.parquet("/FileStore/tables/parquet/pessoal.parquet/Nacionalidade=Portuguesa")
datafnacional.show(truncate=False)
