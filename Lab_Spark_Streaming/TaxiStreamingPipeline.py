# Databricks notebook source
# MAGIC %scala
# MAGIC
# MAGIC Class.forName("org.apache.spark.sql.eventhubs.EventHubsSource")

# COMMAND ----------

# Namespace Connection String
namespaceConnectionString = "<namespace connection string>"

# Event Hub Name
startEventHubName = "<start event hub name>"

# Event Hub Connection String
startEventHubConnectionString = namespaceConnectionString + ";EntityPath=" + startEventHubName

# Event Hub Configuration
startEventHubConfiguration = {
  'eventhubs.connectionString' : sc._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(startEventHubConnectionString)  
}

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

startInputDF.isStreaming

# COMMAND ----------

# Add the sink

startMemoryQuery = (
                      startInputDF
                          .writeStream
                          .queryName("MemoryQuery")
                          .format("memory")
                          .trigger(processingTime = '10 seconds')
                          .start()
                   )

# COMMAND ----------

startMemoryQuery.lastProgress

# COMMAND ----------

display(
      startInputDF,
      streamName = "DisplayMemoryQuery",
      processingTime = '10 seconds'  
)

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

display(
      startRawDF,
      streamName = "DisplayMemoryQuery",
      processingTime = '10 seconds'  
)

# COMMAND ----------

startTransformedDF = (
                        startRawDF
                            .withColumn("TripType",
                                            when(
                                                    col("RateCodeId") == "6",
                                                        "SharedTrip"
                                                )
                                            .otherwise("SoloTrip")
                                       )
  
                            .drop("RateCodeId")
                     )

# COMMAND ----------

startTransformedDF = (
                        startTransformedDF
                            .where("PassengerCount > 0")
                     )

# COMMAND ----------

# Add the console sink

startConsoleQuery = (
                        startTransformedDF
                            .writeStream                            
                            .queryName("ConsoleQuery")
                            .format("console")
                            .trigger(processingTime = '10 seconds')
                            .start()
                    )

# COMMAND ----------

from pyspark.sql.functions import *

startTransformedDF = (
                        spark
                            .readStream
                            .format("eventhubs")
                            .options(**startEventHubConfiguration)
                            .load()

                            .withColumn(
                                          "RawTaxiData",
                                          col("body").cast("string")
                                       )

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

                            .withColumn("TripType",
                                            when(
                                                    col("RateCodeId") == "6",
                                                        "SharedTrip"
                                                )
                                            .otherwise("SoloTrip")
                                       )
  
                            .drop("RateCodeId")

                            .where("PassengerCount > 0")
                     )

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

(
   spark
      .read
      .csv("/mnt/datalake/Raw/")
      
      .count()
)

# COMMAND ----------

startTransformedDF = (
                        startTransformedDF
                            .withColumn("PickupYear", year("PickupTime"))
                            .withColumn("PickupMonth", month("PickupTime"))
                            .withColumn("PickupDay", dayofmonth("PickupTime"))
                     )

# COMMAND ----------

startProcessedStreamingFileQuery = (
                                      startTransformedDF
                                          .writeStream
                                          .queryName("ProcessedTaxiQuery")
                                          .format("parquet")
                                          .option("path", "/mnt/datalake/Processed/")
                                          .option("checkpointLocation", "/mnt/datalake/ProcessedCheckpoint")

                                          .partitionBy("PickupYear", "PickupMonth", "PickupDay")

                                          .trigger(processingTime = '3 seconds')  
                                          .start()  
                                   )

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

(
   spark
      .read
      .csv("/mnt/datalake/Raw/")
      
      .count()
)

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

# Anomalies

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

# DBTITLE 1,Not shown in the video. Used to extract from Anomalies event hub
from pyspark.sql.types import *
  
anomaliesSchema = (
                      StructType()
                         .add("Id", "integer")                         
                         .add("PickupTime", "timestamp")
                         .add("PassengerCount", "integer")                         
                  )

anomaliesOutputDF = (
                          spark
                              .readStream
                              .format("eventhubs")
                              .options(**anomaliesEventHubConfiguration)
                              .load()
                       )

anomaliesOutputDF = (
                      anomaliesOutputDF
                          .withColumn(
                                  "RawTaxiData",
                                  col("body").cast("string")
                               )

                          .select("RawTaxiData")
  
                          .select(
                                    from_json(
                                                col("RawTaxiData"),
                                                rideStartSchema
                                             )                      
                                        .alias("TaxiData")
                                 )

                          .select(
                                    "TaxiData.Id",
                                    "TaxiData.PickupTime",
                                    "TaxiData.PassengerCount"
                                 )
                   )

display(
      anomaliesOutputDF,
      streamName = "DisplayMemoryQuery",
      processingTime = '10 seconds'  
)

# COMMAND ----------

# MAGIC %scala
# MAGIC Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver")

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
  
  (
    batchDF
      .withColumn("BatchId", lit(batchId))
    
      .groupBy("BatchId")
      .agg(count("*").alias("TripCount"))
    
      .write      
      .jdbc(sqlServerUrl, "RidesByBatch", "append", connectionProperties)
  )
  
  pass

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

# COMMAND ----------

