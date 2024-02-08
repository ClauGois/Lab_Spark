-- Databricks notebook source
DESCRIBE HISTORY TaxisDB.YellowTaxis_BronzeLive

-- COMMAND ----------

DELETE FROM TaxisDB.YellowTaxis_BronzeLive
WHERE RideId = 5530102

-- COMMAND ----------

SELECT *
FROM TaxisDB.YellowTaxis_BronzeLive
LIMIT 10