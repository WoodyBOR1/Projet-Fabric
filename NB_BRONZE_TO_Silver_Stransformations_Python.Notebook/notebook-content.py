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
# META         }
# META       ]
# META     },
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

#Import the functions library python
from pyspark.sql.functions import round, col, dayofmonth, month, year, to_date, quarter, substring, when, regexp_replace

#Path to wind_power table in Bronze Lakehouse
bronze_table_path = "abfss://WindPowerAnalytics@onelake.dfs.fabric.microsoft.com/LH_Wind_Power_Bronze.Lakehouse/Tables/dbo/wind_power"

# Load the wind_power table into a DataFrame
df = spark.read.format("delta").load(bronze_table_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Clean and enrich data
df_transformed = (df 
     .withColumn("wind_speed", round (col("wind_speed"), 2 ))                         # Round to two digits after the decimal
     .withColumn("energy_produced", round (col("energy_produced"), 2 ))               # idem

# Transforming the date column and creating a new column
     .withColumn("day", dayofmonth(col("date")))                                      # Extract day & Replace '-' with ':'
     .withColumn("month", month(col("date")))                                          # Extract moth
     .withColumn("quarter", quarter(col("date")))                                     # Extract quarter
     .withColumn("year", year(col("date")))                                           # Extract year

#Transforming the time column and creating a new column   
     .withColumn("time", regexp_replace(col("time"), "-", ":"))                        # Extract hours & Replace '-' with ':'
     .withColumn("hour_of_day", substring(col("time"), 1, 2).cast("int"))              # Extract hour in numeric format
     .withColumn("minute_of_hour", substring(col("time"), 4, 2).cast("int"))           # Extract minutes in numeric format
     .withColumn("second_of_minute", substring(col("time"), 7, 2).cast("int"))         # Extract second in numeric format

#Period of the day
     .withColumn("time_period", when((col("hour_of_day") >=5) & (col("hour_of_day") < 12), "Morning")
                                .when((col("hour_of_day") >=12) & (col("hour_of_day") < 17), "Afternoon")
                                .when((col("hour_of_day") >=17) & (col("hour_of_day") < 21), "Evening")
                                .otherwise("Night")     #This code adds a column that assigns each hour to a period of the day (Morning, Afternoon, Evening, or Night) based on the value of hour_of_day
      )
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Path to wind_power table in Silver Lakehouse
silver_table_path = "abfss://WindPowerAnalytics@onelake.dfs.fabric.microsoft.com/LH_Wind_Power_Silver.Lakehouse/Tables/dbo/wind_power"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Save the transformed table in Silver Lakehouse
#df_transformed.write.format("delta").mode("overwrite").save(silver_table_path)
df_transformed.write.format("delta").option("overwriteSchema","true").mode("overwrite").save(silver_table_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Load the wind_power table into a DataFrame
df = spark.read.format("delta").load(silver_table_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Visualizing the DataFrame
#display(df_transformed)                                  #Command to be used during the table transformation

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
