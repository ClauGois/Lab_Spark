# Databricks notebook source
# MAGIC %md ###Set shuffle partitions

# COMMAND ----------

spark.conf.get("spark.sql.shuffle.partitions")

# COMMAND ----------

spark.conf.set("spark.sql.shuffle.partitions", 2)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Extracting from Data Lake
# MAGIC
# MAGIC Reading ride pickup events from Data Lake

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
                  .schema(rideStartSchema)                    
                  .option("maxFilesPerTrigger", 1)
                  .option("multiline", "true")    
                  .json("/mnt/datalake/TaxiSourceStateData/")  #Already created in Data Lake
            )

# COMMAND ----------

# MAGIC %md ###State Management Using Checkpointing
# MAGIC
# MAGIC <b>Problem Statement</b><br/>
# MAGIC Get the count of trips by year

# COMMAND ----------

ridesDF = (
              inputDF
                  .withColumn("TripYear", year("PickupTime"))
  
                  .groupBy("TripYear")
                  .count()
          )

display(
      ridesDF,
      streamName = "StateMemoryQuery",
      processingTime = "5 seconds",
      checkpointLocation = "/mnt/datalake/StateCheckpoint"
)

# COMMAND ----------

# MAGIC %md ###Watermarking
# MAGIC
# MAGIC <b>Problem Statement</b></br>
# MAGIC Find total rides starting every 5 minutes, with 8 minute watermark

# COMMAND ----------

ridesWithWatermarkDF = (
                          inputDF
                              .withWatermark("PickupTime", "8 minutes") 

                              .groupBy(window("PickupTime", "5 minutes"))  
                              .count()
  
                              .select("window.start", "window.end", "count")
                      )

# COMMAND ----------

streamingWatermarkQuery = (
                              ridesWithWatermarkDF
                                  .writeStream
                                  .queryName("ConsoleQuery")                              
                                  .format("console")
                                  .outputMode("update")
                                  .option("checkpointLocation", "/mnt/datalake/WatermarkCheckpoint")                              
                                  .trigger(processingTime = '10 seconds')
                                  .start()  
                          )

# COMMAND ----------

# MAGIC %md ###Deduplicating the stream
# MAGIC
# MAGIC Removing duplicates based on 'Id' column

# COMMAND ----------

ridesDeduplicatedDF = (
                          inputDF
                            .dropDuplicates(["Id"])
                      )

display(
      ridesDeduplicatedDF,
      streamName = "DuplicatesMemoryQuery",
      processingTime = "3 seconds",
      checkpointLocation = "/mnt/datalake/DuplicateCheckpoint"
)

# COMMAND ----------

