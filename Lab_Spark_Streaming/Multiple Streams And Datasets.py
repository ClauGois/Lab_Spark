# Databricks notebook source
# MAGIC %md ###Preparing Ride Start Stream

# COMMAND ----------

spark.conf.set("spark.sql.shuffle.partitions", 1)

# COMMAND ----------

#Configuration Properties
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

rideStartInputDF = (
                        spark
                          .readStream
                          .format("eventhubs")
                          .options(**startEventHubConfiguration)
                          .load()
                   )


rideStartDF = (
                  rideStartInputDF
                      .select(
                                from_json(
                                              col("body").cast("string"),
                                              rideStartSchema
                                         )
                                  .alias("taxidata")
                             )
  
                      .select(
                                "taxidata.Id",
                                "taxidata.VendorId",
                                "taxidata.PickupTime",
                                "taxidata.CabLicense",
                                "taxidata.DriverLicense",
                                "taxidata.PickupLocationId",
                                "taxidata.PassengerCount",
                                "taxidata.RateCodeId",
                             )

                      .withColumnRenamed("Id", "RideStartId")
              )

# COMMAND ----------

# MAGIC %md ###Prepare Taxi Zones Static Dataset

# COMMAND ----------

taxiZones = (
                spark
                    .read
                    .option("header", "true")
                    .option("inferSchema", "true")
                    .csv("/mnt/datalake/StaticData/TaxiZones.csv")
            )

display(taxiZones)

# COMMAND ----------

# MAGIC %md ###Joining Taxi Ride Start Stream with Taxi Zones

# COMMAND ----------

ridesWithZonesDF = (
                      rideStartDF
                            .join(taxiZones, 
                                      rideStartDF.PickupLocationId == taxiZones.LocationID
                                 )
  
                            .groupBy("Zone")
                            .count()
                   )

# COMMAND ----------

display(
      ridesWithZonesDF,
      streamName = "StreamStaticMemoryQuery",
      processingTime = '5 seconds'  
)

# COMMAND ----------

# MAGIC %md ###Using SQL for queries

# COMMAND ----------

rideStartDF.createOrReplaceTempView("TaxiRideStart")

taxiZones.createOrReplaceTempView("TaxiZones")

# COMMAND ----------

rideStartSqlDF = spark.sql("SELECT z.Zone, COUNT(*) AS TripCount FROM TaxiRideStart p INNER JOIN TaxiZones z ON p.PickupLocationId = z.LocationID GROUP BY z.Zone")

display(rideStartSqlDF)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT z.Zone
# MAGIC     , COUNT(*) AS TripCount
# MAGIC FROM TaxiRideStart p
# MAGIC   INNER JOIN TaxiZones z ON p.PickupLocationId = z.LocationID
# MAGIC GROUP BY z.Zone

# COMMAND ----------

# MAGIC %md ###Preparing Ride End Stream

# COMMAND ----------

# Event Hub Name
endEventHubName = "<end event hub name>"

# Event Hub Connection String
endEventHubConnectionString = namespaceConnectionString + ";EntityPath=" + endEventHubName


# Event Hub Configuration
endEventHubConfiguration = {
  'eventhubs.connectionString' : sc._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(endEventHubConnectionString) 
}

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

rideEndSchema = (
                    StructType()
                       .add("Id", "integer")
                       .add("DropTime", "timestamp")
                       .add("DropLocationId", "integer")
                       .add("TripDistance", "double")
                       .add("PaymentType", "integer")
                       .add("TotalAmount", "double")
                )

rideEndInputDF = (
                      spark
                        .readStream
                        .format("eventhubs")
                        .options(**endEventHubConfiguration)
                        .load()
                 )


rideEndDF = (
                rideEndInputDF
                    .select(
                              from_json(
                                            col("body").cast("string"),
                                            rideEndSchema
                                       )
                                .alias("taxidata")
                           )
  
                    .select(
                              "taxidata.Id",
                              "taxidata.DropTime",
                              "taxidata.DropLocationId",
                              "taxidata.TripDistance",
                              "taxidata.PaymentType",
                              "taxidata.TotalAmount",
                           )

                    .withColumnRenamed("Id", "RideEndId")
           )

# COMMAND ----------

# MAGIC %md
# MAGIC ###Joining ride start and ride end streams

# COMMAND ----------

ridesDF = (
              rideStartDF
                  .join(rideEndDF, 
                        expr("""
                                  rideStartId = rideEndId
                            """)
                   )
          )

# COMMAND ----------

display(
      ridesDF,
      streamName = "StreamStreamMemoryQuery",
      processingTime = '5 seconds'  
)

# COMMAND ----------

