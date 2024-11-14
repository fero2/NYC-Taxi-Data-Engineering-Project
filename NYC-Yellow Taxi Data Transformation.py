# Databricks notebook source

spark.conf.set(
    "fs.azure.account.key.storageaccproj1azure.dfs.core.windows.net",
    dbutils.secrets.get(scope="Key-Vault-secret-scope", key = "ADLS-access-key")
    )

# COMMAND ----------

display(dbutils.fs.ls("abfss://raw-data@storageaccproj1azure.dfs.core.windows.net/"))

# COMMAND ----------

nyc_taxi_trip = spark.read.parquet("abfss://raw-data@storageaccproj1azure.dfs.core.windows.net/nyc_data_1L/*.parquet", header = True, inferSchema = True)

display(nyc_taxi_trip)

# COMMAND ----------

nyc_taxi_zone = spark.read.csv("abfss://raw-data@storageaccproj1azure.dfs.core.windows.net/taxi_zone_lookup.csv", header = True, inferSchema = True)

display(nyc_taxi_zone)

# COMMAND ----------

nyc_taxi_trip = nyc_taxi_trip.withColumnRenamed("tpep_pickup_datetime", "Pickup_datetime")\
    .withColumnRenamed("tpep_dropoff_datetime", "Dropoff_datetime")

display(nyc_taxi_trip.limit(20))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Renaming the column name's first letter only to Capital

# COMMAND ----------


from pyspark.sql.functions import col

# Function to capitalize only the first letter if it starts with a lowercase letter
def capitalize_first_letter(col_name):
    if col_name[0].islower(): 
        return (col_name[0].upper() + col_name[1:])  # Capitalize first letter and keep the rest as it is
    else:
        return col_name

# Rename columns based on the function
nyc_taxi_trip = nyc_taxi_trip.select([col(c).alias(capitalize_first_letter(c)) for c in nyc_taxi_trip.columns])

# Show results
display(nyc_taxi_trip.distinct().limit(20))


# COMMAND ----------

nyc_taxi_trip = nyc_taxi_trip.drop_duplicates()
display(nyc_taxi_trip.limit(30))


# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Creating a new column "Index" with the row number function 
# MAGIC
# MAGIC ~ will use this "Index" as the Primary Key for necessary dimension tables with their respective PK column

# COMMAND ----------

from pyspark.sql import Window
from pyspark.sql.functions import row_number

window_spec = Window.orderBy("VendorID") 

# Add a new column 'Index' with row number function and using it as Primary Key
nyc_taxi_trip = nyc_taxi_trip.withColumn("Index", row_number().over(window_spec) - 1)

display(nyc_taxi_trip.limit(20))

nyc_taxi_trip.cache()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Creating "Pickup_datetime_Dim" Dimension Table

# COMMAND ----------

from pyspark.sql.functions import hour, day, month, weekday, year  

Pickup_datetime_Dim = nyc_taxi_trip.withColumn("Pickup_ID", nyc_taxi_trip.Index)\
    .select("Pickup_ID", "Pickup_datetime")


#Adding new column and values to it using the Pre-planned Data Model
Pickup_datetime_Dim = Pickup_datetime_Dim.withColumn("Pickup_hour", hour("Pickup_datetime"))\
    .withColumn("Pickup_day", day("Pickup_datetime"))\
    .withColumn("Pickup_month", month("Pickup_datetime"))\
    .withColumn("Pickup_year", year("Pickup_datetime"))\
    .withColumn("Pickup_weekday", weekday("Pickup_datetime"))


display(Pickup_datetime_Dim.limit(30))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Creating "Drop_datetime_Dim" Dimension Table

# COMMAND ----------

from pyspark.sql.functions import row_number, hour, day, month, weekday, year  

Drop_datetime_Dim = nyc_taxi_trip.withColumn("Drop_ID", nyc_taxi_trip.Index)\
    .select("Drop_ID", "Dropoff_datetime")

#Adding new column and values to it using the Pre-planned Data Model
Drop_datetime_Dim = Drop_datetime_Dim.withColumn("Drop_hour", hour("Dropoff_datetime"))\
    .withColumn("Drop_day", day("Dropoff_datetime"))\
    .withColumn("Drop_month", month("Dropoff_datetime"))\
    .withColumn("Drop_year", year("Dropoff_datetime"))\
    .withColumn("Drop_weekday", weekday("Dropoff_datetime"))


display(Drop_datetime_Dim.limit(30))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Creating "Pickup_Location_Dim" Dimension Table

# COMMAND ----------

Pickup_Location_Dim = nyc_taxi_zone.withColumn("Pickup_Location_ID", nyc_taxi_zone.LocationID)\
    .withColumn("Pickup_zone", nyc_taxi_zone.Zone)\
    .withColumn("Pickup_borough", nyc_taxi_zone.Borough)\
    .withColumn("Pickup_servicezone", nyc_taxi_zone.service_zone)\
    .select("Pickup_Location_ID", "Pickup_zone", "Pickup_borough", "Pickup_servicezone")

Pickup_Location_Dim = Pickup_Location_Dim.drop_duplicates()

display(Pickup_Location_Dim.limit(30))

#.withColumn("Pickup_Location_ID", nyc_taxi_zone.Index)\

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Creating "Drop_Location_Dim" Dimension Table

# COMMAND ----------

Drop_Location_Dim = nyc_taxi_zone.withColumn("Drop_Location_ID", nyc_taxi_zone.LocationID)\
    .withColumn("Drop_zone", nyc_taxi_zone.Zone)\
    .withColumn("Drop_borough", nyc_taxi_zone.Borough)\
    .withColumn("Drop_servicezone", nyc_taxi_zone.service_zone)\
    .select("Drop_Location_ID", "Drop_zone", "Drop_borough", "Drop_servicezone")

Drop_Location_Dim = Drop_Location_Dim.drop_duplicates()

display(Drop_Location_Dim.limit(30))

#withColumn("Drop_Location_ID", nyc_taxi_zone.Index)\

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Creating "Passenger_Dim" Dimension Table

# COMMAND ----------

Passenger_Dim = nyc_taxi_trip.withColumn("Passenger_ID", nyc_taxi_trip.Index)\
    .select("Passenger_ID", "Passenger_count")

display(Passenger_Dim.limit(30))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Creating "Trip_Dim" Dimension Table

# COMMAND ----------

Trip_Dim = nyc_taxi_trip.withColumn("Trip_ID", nyc_taxi_trip.Index)\
    .select("Trip_ID", "Trip_distance", "Store_and_fwd_flag")

display(Trip_Dim.limit(30))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Creating "RateCode_Dim" Dimension Table

# COMMAND ----------

Ratecode_name = { 1:"Standard rate" ,
            2:"JFK" ,
            3:"Newark" ,
            4:"Nassau or Westchester" ,
            5:"Negotiated fare" ,
            6:"Group ride" }

from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

# Define a UDF to map the dictionary
def rate_code(rate_code_id):
    return Ratecode_name.get(rate_code_id, "Unknown")  # Returns "Unknown" if key not found

# Register the UDF
rate_code_udf = udf(rate_code, StringType())

# Apply the UDF to create the new column
Ratecode_Dim = nyc_taxi_trip.withColumn("RateCode_name", rate_code_udf(nyc_taxi_trip["RatecodeID"]))


Ratecode_Dim = Ratecode_Dim.withColumn("RateCode_ID", Ratecode_Dim.Index)\
    .withColumn("RateCode_type", Ratecode_Dim.RatecodeID)\
    .select("RateCode_ID", "RateCode_type", "RateCode_name")

display(Ratecode_Dim.limit(30))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Creating "Payment_Dim" Dimension Table

# COMMAND ----------

Payment_mode = { 
            1:"Credit card" ,
            2:"Cash" ,
            3:"No charge" ,
            4:"Dispute" ,
            5:"Unknown" ,
            6:"Voided trip" }


# Define a UDF to map the dictionary
def Payment_mode_name(Payment_id):
    return Payment_mode.get(Payment_id, "Unknown")  # Returns "Unknown" if key not found

# Register the UDF
Payment_mode_name_udf = udf(Payment_mode_name, StringType())

# Apply the UDF to create the new column
Payment_Dim = nyc_taxi_trip.withColumn("Payment_mode", Payment_mode_name_udf(nyc_taxi_trip["Payment_type"]))


Payment_Dim = Payment_Dim.withColumn("Payment_ID", Payment_Dim.Index)\
    .select("Payment_ID", "Payment_type", "Payment_mode")

display(Payment_Dim.limit(30))

# COMMAND ----------

nyc_taxi_trip.columns

# COMMAND ----------


Pickup_Location_Dim.cache()
Drop_Location_Dim.cache()

from pyspark import StorageLevel

Trip_Dim.persist(StorageLevel.MEMORY_AND_DISK)


# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Creating "Fact_table"

# COMMAND ----------



Fact_table = nyc_taxi_trip.join(Pickup_datetime_Dim, nyc_taxi_trip.Index == Pickup_datetime_Dim.Pickup_ID, how="inner")\
            .join(Drop_datetime_Dim, nyc_taxi_trip.Index == Drop_datetime_Dim.Drop_ID, how="inner")\
            .join(Pickup_Location_Dim, nyc_taxi_trip.PULocationID == Pickup_Location_Dim.Pickup_Location_ID, how="inner")\
            .join(Drop_Location_Dim, nyc_taxi_trip.DOLocationID == Drop_Location_Dim.Drop_Location_ID, how="inner")\
            .join(Trip_Dim, nyc_taxi_trip.Index == Trip_Dim.Trip_ID, how="inner")\
            .join(Passenger_Dim, nyc_taxi_trip.Index == Passenger_Dim.Passenger_ID, how="inner")\
            .join(Payment_Dim, nyc_taxi_trip.Index == Payment_Dim.Payment_ID, how="inner")\
            .join(Ratecode_Dim, nyc_taxi_trip.RatecodeID == Ratecode_Dim.RateCode_ID, how="inner")\
            .select("VendorID","Pickup_ID", "Drop_ID", "Passenger_ID", "Trip_ID", "Pickup_Location_ID", "Drop_Location_ID", "Ratecode_ID", 
                    "Payment_ID", "Fare_amount", "Extra", "Mta_tax", "Tip_amount", "Tolls_amount", "Improvement_surcharge", "Congestion_surcharge", "Airport_fee", "Total_amount")
                            
                            

display(Fact_table)


# COMMAND ----------



target_location = "abfss://nyc-taxi-tripdata@storageaccproj1azure.dfs.core.windows.net/transformed-data-gold"


Pickup_datetime_Dim.repartition(1).write.mode("overwrite").option("header", "True").parquet(f"{target_location}/Pickup_datetime_Dim")

Drop_datetime_Dim.repartition(1).write.mode("overwrite").option("header", "True").parquet(f"{target_location}/Drop_datetime_Dim")

Pickup_Location_Dim.repartition(1).write.mode("overwrite").option("header", "True").parquet(f"{target_location}/Pickup_Location_Dim")

Drop_Location_Dim.repartition(1).write.mode("overwrite").option("header", "True").parquet(f"{target_location}/Drop_Location_Dim")

Passenger_Dim.repartition(1).write.mode("overwrite").option("header", "True").parquet(f"{target_location}/Passenger_Dim")

Trip_Dim.repartition(1).write.mode("overwrite").option("header", "True").parquet(f"{target_location}/Trip_Dim")

Ratecode_Dim.repartition(1).write.mode("overwrite").option("header", "True").parquet(f"{target_location}/Ratecode_Dim")

Payment_Dim.repartition(1).write.mode("overwrite").option("header", "True").parquet(f"{target_location}/Payment_Dim")



# COMMAND ----------




Fact_table.repartition(1)\
    .write.mode("overwrite")\
    .option("compression", "snappy")\
    .parquet(f"{target_location}/Yellow_Taxi_Tripdata_Fact")


print("Success")



# COMMAND ----------

