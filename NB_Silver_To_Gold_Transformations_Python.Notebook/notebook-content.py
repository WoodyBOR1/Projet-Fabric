# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "2c72e86b-4ccb-4acc-a983-63f0e9bd49e7",
# META       "default_lakehouse_name": "LH_Wind_Power_Silver",
# META       "default_lakehouse_workspace_id": "cd28528f-6aba-4d1e-b9ed-f6fd6c34a914",
# META       "known_lakehouses": [
# META         {
# META           "id": "2c72e86b-4ccb-4acc-a983-63f0e9bd49e7"
# META         },
# META         {
# META           "id": "0e232c3b-a70d-484e-996c-e62260504c87"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

#Import the functions library python
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

#Path to wind_power table in Silver Lakehouse 
Silver_table_path = "abfss://WindPowerAnalytics@onelake.dfs.fabric.microsoft.com/LH_Wind_Power_Silver.Lakehouse/Tables/dbo/wind_power"

#Load the wind_power table into a DataFrame
df = spark.read.format("delta").load(Silver_table_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# create the date dimension table
date_dim = df.select("date","day", "month","quarter", "year" ).distinct() \
                .withColumnRenamed("date", "date_id")

# create the time dimension table
time_dim = df.select("time","hour_of_day", "minute_of_hour", "second_of_minute", "time_period").distinct() \
                .withColumnRenamed("time", "time_id")      

# create the turbine dimension table with the primary key numeric
turbine_dim = df.select("turbine_name", "capacity", "location_name", "latitude","longitude", "region").distinct() \
                .withColumn("turbine_id",row_number().over(Window.orderBy("turbine_name", "capacity", "location_name", "latitude","longitude", "region")))   

# create the Operational Status dimension table
operational_status_dim = df.select("status", "responsible_department").distinct() \
                 .withColumn("status_id", row_number().over(Window.orderBy("status","responsible_department")))                

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Join with turbine and operational status dimensions tables to the original Dataframe
df = df.join(turbine_dim, ["turbine_name", "capacity", "location_name", "latitude","longitude", "region"], "left") \
    .join(operational_status_dim, ["status", "responsible_department"],"left")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# create the fact table
fact_table = df.select("production_id","date", "time", "turbine_id", "status_id", "wind_direction", "energy_produced") \
                .withColumnRenamed("date", "date_id") \
                .withColumnRenamed("time", "time_id")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Paths to the Gold tables
gold_date_dim_path = "abfss://WindPowerAnalytics@onelake.dfs.fabric.microsoft.com/LH_Wind_Power_Gold.Lakehouse/Tables/dbo/dim_date"
gold_time_dim_path = "abfss://WindPowerAnalytics@onelake.dfs.fabric.microsoft.com/LH_Wind_Power_Gold.Lakehouse/Tables/dbo/dim_time"
gold_turbine_dim_path = "abfss://WindPowerAnalytics@onelake.dfs.fabric.microsoft.com/LH_Wind_Power_Gold.Lakehouse/Tables/dbo/dim_turbine"
gold_operational_status_dim_path = "abfss://WindPowerAnalytics@onelake.dfs.fabric.microsoft.com/LH_Wind_Power_Gold.Lakehouse/Tables/dbo/dim_operational_status"
gold_fact_table_path = "abfss://WindPowerAnalytics@onelake.dfs.fabric.microsoft.com/LH_Wind_Power_Gold.Lakehouse/Tables/dbo/fact_wind_power"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Save the table in the Gold Lakehouse
date_dim.write.format("delta").option("overwriteSchema","true").mode("overwrite").save(gold_date_dim_path)
time_dim.write.format("delta").option("overwriteSchema", "true").mode("overwrite").save(gold_time_dim_path)   # Force schema overwrite to avoid a mismatch error when writing the Gold table
turbine_dim.write.format("delta").option("overwriteSchema","true").mode("overwrite").save(gold_turbine_dim_path)
operational_status_dim.write.format("delta").option("overwriteSchema","true").mode("overwrite").save(gold_operational_status_dim_path)
fact_table.write.format("delta").option("overwriteSchema","true").mode("overwrite").save(gold_fact_table_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Visualize the table fact 
#display(time_dim)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": false,
# META   "editable": true
# META }
