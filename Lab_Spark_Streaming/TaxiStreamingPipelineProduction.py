# Databricks notebook source
dbutils.widgets.text("EventHubNamespaceConnectionString", "<add connection string>", "Connection String")

dbutils.widgets.text("StartEventHubName", "<add event hub name>", "Event Hub")

# COMMAND ----------

# MAGIC %md
# MAGIC ###Event Hub Configuration
# MAGIC
# MAGIC Define Event Hub Namespace connection string and name of Event Hub. Join both of them to create final connection string.

# COMMAND ----------

# Namespace Connection String
namespaceConnectionString = dbutils.widgets.get("EventHubNamespaceConnectionString")

# Event Hub Name
startEventHubName = dbutils.widgets.get("StartEventHubName")

# Event Hub Connection String
startEventHubConnectionString = namespaceConnectionString + ";EntityPath=" + startEventHubName

# Event Hub Configuration
startEventHubConfiguration = {
  'eventhubs.connectionString' : sc._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(startEventHubConnectionString)  
}

print(namespaceConnectionString)
print(startEventHubName)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Extracting from Event Hub
# MAGIC
# MAGIC Reading ride pickup events from Event Hub

# COMMAND ----------

# Create a Streaming DataFrame

startInputDF = (
                  spark
                      .readStream
                      .format("eventhubs")
                      .options(**startEventHubConfiguration)
                      .load()
               )

# COMMAND ----------

# MAGIC %md
# MAGIC ###Applying Transformations
# MAGIC
# MAGIC Converting event hub binary data to taxi ride pickup columns

# COMMAND ----------

from pyspark.sql.functions import *

startRawDF = (
                startInputDF
                    .withColumn(
                                  "RawTaxiData",
                                  col("body").cast("string")
                               )

                    .select("RawTaxiData")
             )

# COMMAND ----------

from pyspark.sql.types import *
  
rideStartSchema = (
                      StructType()
                         .add("Id", "integer")
                         .add("VendorId", "integer")
                         .add("PickupTime", "timestamp")
                         .add("CabLicense", "string")
                         .add("DriverLicense", "string")
                         .add("PickupLocationId", "integer")
                         .add("PassengerCount", "integer")
                         .add("RateCodeId", "integer")
                  )

# COMMAND ----------

startRawDF = (
                startRawDF
                    .select(
                              from_json(
                                          col("RawTaxiData"),
                                          rideStartSchema
                                       )                      
                                  .alias("TaxiData")
                           )
  
                    .select(
                              "TaxiData.Id",
                              "TaxiData.VendorId",
                              "TaxiData.PickupTime",
                              "TaxiData.CabLicense",
                              "TaxiData.DriverLicense",
                              "TaxiData.PickupLocationId",
                              "TaxiData.PassengerCount",
                              "TaxiData.RateCodeId",
                           )
             )

# COMMAND ----------

# MAGIC %md
# MAGIC ###Loading to Azure Data Lake
# MAGIC
# MAGIC Loading raw data to Azure Data Lake folder

# COMMAND ----------

startRawStreamingFileQuery = (
                                startRawDF                             
                                    .writeStream
                                    .queryName("RawTaxiQuery")
                                    .format("csv")
                                    .option("path", "/mnt/datalake/Raw/")   
                                    .option("checkpointLocation", "/mnt/datalake/RawCheckpoint")
                                    .trigger(processingTime = '10 seconds')
                                    .start()  
                             )

# COMMAND ----------

# MAGIC %md
# MAGIC ###Loading to Azure Event Hub
# MAGIC
# MAGIC Configuring another Event Hub instance, finding anomalies and loading to it

# COMMAND ----------

# Namespace Connection String
anomaliesNamespaceConnectionString = "<namespace connection string>"

# Event Hub Name
anomaliesEventHubName = "<anomalies event hub name>"

# Event Hub Connection String
anomaliesEventHubConnectionString = anomaliesNamespaceConnectionString + ";EntityPath=" + anomaliesEventHubName

# Event Hub Configuration
anomaliesEventHubConfiguration = {
  'eventhubs.connectionString' : sc._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(anomaliesEventHubConnectionString) 
}

# COMMAND ----------

anomaliesDF = (
                  startRawDF
                    .where("PassengerCount > 5")
              )

anomaliesDF = (
                  anomaliesDF
                    .select(
                              to_json(
                                        struct(
                                                  col("Id"), 
                                                  col("PickupTime"), 
                                                  col("PassengerCount")
                                              )
                                      )
                                .alias("body")
                           )
              )

# COMMAND ----------

anomaliesStreamingQuery = (
                              anomaliesDF
                                  .writeStream
                                  .queryName("AnomaliesQuery")
                                  .format("eventhubs")                                  
                                  .option("checkpointLocation", "/mnt/datalake/AnomaliesCheckpoint")                                 
                                  .trigger(processingTime = '3 seconds')  
                                  .options(**anomaliesEventHubConfiguration)
                                  .start()  
                           )

# COMMAND ----------

# MAGIC %md
# MAGIC ###Loading to Azure SQL
# MAGIC
# MAGIC Configuring Azure SQL instance, aggregating data and loading to SQL Database

# COMMAND ----------

sqlServerName = "<sql server name>"
sqlDatabase = "<database name>"
sqlPort = 1433
userName = "<server admin name>"
password = "<server admin password>"

sqlServerUrl = "jdbc:sqlserver://{0}:{1};database={2}".format(sqlServerName, sqlPort, sqlDatabase)
connectionProperties = {
  "user" : userName,
  "password" : password,
  "driver" : "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql import *

def writeToSqlDb(batchDF, batchId): 
  
  (
    batchDF
      .select("Id", "PickupTime", "PickupLocationId", "CabLicense", "DriverLicense", "PassengerCount")
      
      .write      
      .jdbc(sqlServerUrl, "Rides", "overwrite", connectionProperties)
  )
  
  pass

# COMMAND ----------

aggregatedStreamingQuery = (
                              startRawDF
                                .writeStream
                                .foreachBatch(writeToSqlDb)
  
                                .queryName("AggregatedTaxiQuery")
                                .option("checkpointLocation", "/mnt/datalake/AggregatedCheckpoint")
                                .trigger(processingTime = '10 seconds')

                                .outputMode("append")
                                .start()
                           )