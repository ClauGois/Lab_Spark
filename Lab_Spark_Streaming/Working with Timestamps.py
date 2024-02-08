# Databricks notebook source
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

from pyspark.sql.functions import *
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

inputDF = (
              spark
                .readStream
                .format("eventhubs")
                .options(**startEventHubConfiguration)
                .load()
          )

rawDF = (
            inputDF
              .withColumn(
                            "TaxiData",
                            from_json(
                                        col("body").cast("string"),
                                        rideStartSchema
                                     )
                         )
        )

# COMMAND ----------

timestampsDF = (
                  rawDF
                    
                    #Unique ride id
                    .withColumn("RideId", col("TaxiData.Id"))
  
                    #Event Time
                    .withColumn("EventTime", col("TaxiData.PickupTime"))
  
                    #Ingestion Time
                    .withColumnRenamed("enqueuedTime", "IngestionTime")
  
                    #Processing Time
                    .withColumn("ProcessingTime", current_timestamp())
    
                    .select(
                              "RideId",
                              "EventTime",
                              "IngestionTime",
                              "ProcessingTime"
                           )
               )

# COMMAND ----------

display(
      timestampsDF,
      streamName = "TimestampMemoryQuery",
      processingTime = '5 seconds'  
)

# COMMAND ----------

