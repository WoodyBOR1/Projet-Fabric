# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "warehouse": {
# META       "default_warehouse": "98c0f399-d9ed-49f2-bf2f-563a71a38451",
# META       "known_warehouses": [
# META         {
# META           "id": "98c0f399-d9ed-49f2-bf2f-563a71a38451",
# META           "type": "Lakewarehouse"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Import the functions library python
import requests
import pandas as pd
from datetime import timedelta

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Base URL for GitHub raw CSV files
base_url = "https://raw.githubusercontent.com/mikailaltundas/datasets-for-training/main/wind-power-dataset/"

# Path to the wind table in the Bronze Lakehouse
bronze_table_path = "abfss://WindPowerAnalytics@onelake.dfs.fabric.microsoft.com/LH_Wind_Power_Bronze.Lakehouse/Tables/dbo/wind_power"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Load existing wind_power data and convert to Pandas
df_spark = spark.read.format("delta").load(bronze_table_path)
df_pandas = df_spark.toPandas()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Find the most recent date and calculate next day's date
most_recent_date = pd.to_datetime(df_pandas['date'], format = '%Y%m%d').max()
next_date = (most_recent_date + timedelta(days = 1 )).strftime('%Y%m%d')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Download and load new data in a Pandas Dataframe
file_url = f"{base_url}{next_date}_wind_power_data.csv"
df_pandas_new = pd.read_csv(file_url)
df_pandas_new['date'] = pd.to_datetime(df_pandas_new['date'])


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Convert to Spark DataFrame append in wind_power_table
df_spark_new = spark.createDataFrame (df_pandas_new, schema = df_spark.schema)
df_spark_new.write.format("delta").mode("append").save(bronze_table_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
