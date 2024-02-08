# Databricks notebook source


# COMMAND ----------

import requests
url = "https://swapi.dev/api/people"
payload = {}
header = {}
response = requests.request("GET", url, headers=header, data=payload)
print(response)

# COMMAND ----------

https://swapi.dev/api/people