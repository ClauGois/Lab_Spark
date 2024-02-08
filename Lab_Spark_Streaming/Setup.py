# Databricks notebook source
# MAGIC %md
# MAGIC ###Mount Data Lake Gen2 to DBFS

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
           "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
           "fs.azure.account.oauth2.client.id": "<application id>",
           "fs.azure.account.oauth2.client.secret": "<client secret>",
           "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/<directory id>/oauth2/token"}

dbutils.fs.mount(
  source = "abfss://<container name>@<storage account name>.dfs.core.windows.net/",
  mount_point = "/mnt/datalake",
  extra_configs = configs)

# COMMAND ----------

display(
  dbutils.fs.ls("/mnt/datalake")
)

# COMMAND ----------

