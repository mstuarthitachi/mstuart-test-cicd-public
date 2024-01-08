# Databricks notebook source
# MAGIC %md
# MAGIC # Overview
# MAGIC This notebook loads and saves compressing hours values. It reads from the data lake named <b>iotctsistorage</b> and saves to these two delta tables:  
# MAGIC * data_analytics.compressor_maintenance_training
# MAGIC * data_analytics.compressor_maintenance_scoring  
# MAGIC
# MAGIC ### Notes
# MAGIC Only these columns are loaded and saved because they are the only columns required for generating compressor maintenance forecasts:  
# MAGIC * timestamp
# MAGIC * COMPRESSINGHR_long  
# MAGIC   
# MAGIC Invalid rows are filtered out where the compressing hours is one of these:
# MAGIC * a null value
# MAGIC * a negative value
# MAGIC * an extra large value greater than 100,000 hours (the highest actual values have been ~50,000 as of Sep 2023)
# MAGIC
# MAGIC This notebook saves or updates the highest compressing hours for each day, for each compressor. If this notebook is run as a Databricks job and is scheduled multiple times per day, then only the newest (largest) number of compressing hours will be saved for each compressor on a given day.  
# MAGIC
# MAGIC This notebook reads and processes all of the data in <b>iotctsistorage</b>. It does not perform incremental load. It may be possible for some older missing IoT data to be recovered and land in the data lake. If this happens, we want this missing data to be processed for possible inclusion in the training and scoring data sets.
# MAGIC
# MAGIC This notebook proceses raw IoT messages (a lot of data):  
# MAGIC * During development, it might be helpful to use a cluster with more workers, for example a Standard_DS4_v2 with 8 workers
# MAGIC * Given this size of cluster this notebook will take approximately 20-30 minutes to run

# COMMAND ----------

# DBTITLE 1,Imports
from pyspark.sql.functions import col, current_timestamp, date_trunc, lit, row_number
from pyspark.sql.types import StructType, StructField, LongType, StringType, TimestampType

from pyspark.sql.window import Window

# COMMAND ----------

# DBTITLE 1,Development Variables
# These variables should kept commented out
# They are only used during development
# dbutils.widgets.removeAll()
# dbutils.widgets.text("catalog", "hive_metastore")
# dbutils.widgets.text("delta_lake_schema", "data_analytics")
# dbutils.widgets.text("customer_asset_metadata_table", "maintenance.dynamics365ce_msdyn_customerasset")
# dbutils.widgets.text("raw_storage_path", "/mnt/iotc/*/*/*/*/*.parquet")

# COMMAND ----------

# Get the schema and table where the values will be stored for training and scoring
catalog = dbutils.widgets.get("catalog")
delta_lake_schema = dbutils.widgets.get("delta_lake_schema")
delta_table_train = f"""{catalog}.{delta_lake_schema}.compressor_maintenance_training"""
delta_table_score = f"""{catalog}.{delta_lake_schema}.compressor_maintenance_scoring"""

# Get the name of the "customer asset" metadata table
# We use this to filter for only IoT enabled compressors
customer_asset_table = dbutils.widgets.get("customer_asset_metadata_table")

# Get the delta lake storage location where the compressing hours values are available
# in raw parquet format
raw_storage_path = dbutils.widgets.get("raw_storage_path")


print("catalog:", catalog)
print("schema/database:", delta_lake_schema)
print("training_table:", delta_table_train)
print("scoring_table:", delta_table_score)
print("customer asset metadata table:", customer_asset_table)
print("raw storage path:", raw_storage_path)

# COMMAND ----------

# Specify only the values needed from the raw data
# We only need number of compressing hours and its timestamp
# Most of the 270+ values are not used so we ignore them
raw_schema = (
     StructType([
        StructField("timestamp", TimestampType(), True),
        StructField("deviceId_string", StringType(), True),
        StructField("telemetry.COMPRESSINGHR_long", LongType(), True)
    ]
))

# COMMAND ----------

# DBTITLE 1,Load the compressing hours values
# Load all of the raw data currently in the data lake
# It may be possible for some older missing IoT data to be recovered
# If this happens, we want this missing data to be processed
raw_df = (
    spark.read
    .schema(raw_schema)
    .parquet(raw_storage_path)
)

# display(raw_df)

# COMMAND ----------

# DBTITLE 1,Select only IoT enabled compressors
# Get a list of device ids for only IoT enabled compressors
iot_devices_df = (
    spark.read
    .table(customer_asset_table)
    .filter(col("msdyn_deviceid").isNotNull())
    .filter(col("msdyn_customerassetcategoryname") == "Compressors")
    .select("msdyn_deviceid")
)

# Filter the values for only IoT enabled compressors
iot_devices_raw_df = (
    raw_df
    .join(iot_devices_df, raw_df.deviceId_string == iot_devices_df.msdyn_deviceid , 'left')
    .filter(col("msdyn_deviceid").isNotNull())
)

# display(iot_devices_raw_df)

# COMMAND ----------

# DBTITLE 1,Filter out any unexpected values (nulls, negatives, extra large)
# Filter out any extra large compressing hours as these are invalid
# Use 100,000 as the maximum. Real values have been as high as 40,000-50,000.
max_hours = 100000

# Clean the compressing hours values by filtering out nulls, negative values, and extra large values
# Drop duplicate rows where multiple rows have the same timestamp, device, and compressing hours
readings_df = (
    iot_devices_raw_df
    .withColumnRenamed("timestamp", "read_timestamp")
    .withColumn("read_date", date_trunc("Day", col("read_timestamp")).cast("date"))
    .withColumn("read_month", date_trunc("Month", col("read_timestamp")).cast("date"))    
    .withColumnRenamed("deviceId_string", "device_id")    
    .withColumn("compressing_hours", col("`telemetry.COMPRESSINGHR_long`"))

    .filter(col("compressing_hours").isNotNull())
    .filter(col("compressing_hours") > 0)
    .filter(col("compressing_hours") <= lit(max_hours))
    
    .select("read_timestamp", "read_date", "read_month", "device_id", "compressing_hours")
    .dropDuplicates()
)

# display(readings_df)

# COMMAND ----------

# DBTITLE 1,Define a helper function for saving the values as training and scoring datasets
def updateTrainAndScoreTables(update_df, epochId):

    # Filter for only the largest compressing hours for each day from each compressor
    windowReadDate = Window.partitionBy("device_id", "read_date").orderBy(col("compressing_hours").desc())
    latest_readings_df = (
        update_df
        .withColumn("row", row_number().over(windowReadDate))
        .filter(col("row") == 1)
        .drop("row")
    )

    # Add a timestamp when these rows were written to the table
    final_df = (
        latest_readings_df
        .withColumn("processed_datetime", current_timestamp())
        .select("processed_datetime"
                , "read_timestamp"
                , "read_date"
                , "read_month"
                , "device_id"
                , "compressing_hours")
    )

    # Update the training data set
    query1 = (
        final_df
        .write
        .mode('append')
        .option('mergeSchema', 'false')
        .saveAsTable(delta_table_train)
    )

    # Update the scoring data set
    query2 = (
        final_df
        .write
        .mode('append')
        .option('mergeSchema', 'false')
        .saveAsTable(delta_table_score)
    )

# COMMAND ----------

# DBTITLE 1,Save the values
# Call the helper function
updateTrainAndScoreTables(readings_df, 0)
