-- Databricks notebook source
-- MAGIC %python
-- MAGIC Clientes = spark.read.format('csv').options(header='true', inferSchema='true', delimiter=';').load('/FileStore/tables/carga/clientes_cartao.csv')

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(Clientes)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC Clientes.createOrReplaceTempView("dados_cliente")

-- COMMAND ----------

select count(*) AS qnt_gen, Gender AS genero from dados_cliente group by Gender

-- COMMAND ----------



-- COMMAND ----------

