# Databricks notebook source
# MAGIC %md ###Tumbling Windows
# MAGIC
# MAGIC <b>Problem Statement: </b><br/>
# MAGIC Find total number of rides starting every 5 minutes

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
                  .json("/mnt/datalake/TaxiSource/")
          )

tumblingRidesDF = (
                      inputDF
                          .groupBy(
                                      window("PickupTime", "5 minutes")
                                  )
                          .count()

                          .select("window.start", "window.end", "count")
  
                          .orderBy("window.start")
                  )

display(
      tumblingRidesDF,
      streamName = "TumblingWindowMemoryQuery"
)

# COMMAND ----------

# MAGIC %md ###Sliding Windows
# MAGIC
# MAGIC <b>Problem Statement: </b><br/>
# MAGIC Every 5 minutes, get total number of rides over last 10 minutes

# COMMAND ----------

slidingRidesDF = (
                      inputDF
                          .groupBy(
                                      window("PickupTime", "10 minutes", "5 minutes")
                                  )
                          .count()
  
                          .select("window.start", "window.end", "count")
  
                          .orderBy("window.start")
                 )

display(
      slidingRidesDF,
      streamName = "SlidingWindowMemoryQuery"      
)

# COMMAND ----------

