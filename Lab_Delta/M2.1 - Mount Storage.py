# Databricks notebook source
# MAGIC %md ###Replace Values 
# MAGIC <br/>
# MAGIC
# MAGIC 1. Replace Client Id, Client Secret, Tenant Id values in configs <br/>
# MAGIC 2. Replace container name and data lake name in mount function
# MAGIC
# MAGIC <i>Note: Make sure to remove 3 stars before and after values

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
           "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
           "fs.azure.account.oauth2.client.id": "*** client id ***",
           "fs.azure.account.oauth2.client.secret": "*** client secret ***",
           "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/*** tenant id ***/oauth2/token"}

# Mount the Data Lake Gen2 account
dbutils.fs.mount(
  source = "abfss://*** container name ***@***data lake name ***.dfs.core.windows.net/",
  mount_point = "/mnt/datalake",
  extra_configs = configs)

# COMMAND ----------

# MAGIC %fs ls /mnt/datalake/Raw