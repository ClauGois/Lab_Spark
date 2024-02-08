# Databricks notebook source
df = spark.read.load("/databricks-datasets/learning-spark-v2/people/people-10m.delta")

# COMMAND ----------

display(df)

# COMMAND ----------

table_name = "people_10m"
df.write.saveAsTable(table_name)

# COMMAND ----------

people_df = spark.read.table(table_name)
display(people_df)

# COMMAND ----------

df2 = spark.read.format('delta').option('versionAsOf', 0).table("people_10m")

display(df2)

# COMMAND ----------



# COMMAND ----------

