# Databricks notebook source
# MAGIC %fs ls /databricks-datasets/structured-streaming/events/

# COMMAND ----------

# MAGIC %fs head /databricks-datasets/structured-streaming/events/file-1.json

# COMMAND ----------

dataf = spark.read.json("/databricks-datasets/structured-streaming/events/file-1.json")
dataf.printSchema()
dataf.show()

# COMMAND ----------

#Lendo 2 dataframes
dataf2 = spark.read.json(["/databricks-datasets/structured-streaming/events/file-1.json", "/databricks-datasets/structured-streaming/events/file-2.json"])
dataf2.printSchema()
dataf2.show()

# COMMAND ----------

dataf.count()

# COMMAND ----------

dataf2.count()

# COMMAND ----------

dataf3 = spark.read.json("/databricks-datasets/structured-streaming/events/*.json")
dataf3.show()


# COMMAND ----------

dataf3.count()

# COMMAND ----------

dataf3.write.json("/FileStore/tables/JSON_EVENTOS/eventos.json")

# COMMAND ----------

spark.sql("CREATE OR REPLACE TEMPORARY VIEW view_evento USING json OPTIONS"+
         " (path '/FileStore/tables/JSON_EVENTOS/eventos.json')")
spark.sql("select action from view_evento").show()

# COMMAND ----------

dataf5 = spark.read.json('/FileStore/tables/JSON_EVENTOS/eventos.json')
dataf5.show()

# COMMAND ----------

dataf5.count()