from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, desc, row_number, asc, max, month, dayofmonth, hour
import sys,time,os




spark = SparkSession.builder.master("spark://192.168.0.2:7077").getOrCreate()
print("spark session created")

# Path to the data
hdfs_path = "hdfs://192.168.0.2:9000/user/user/data/"

# Read the Parquet files from HDFS and create a dataframe
df_taxi_trips = spark.read.parquet(hdfs_path + "yellow_tripdata_2022-01.parquet", hdfs_path + "yellow_tripdata_2022-02.parquet", hdfs_path + "yellow_tripdata_2022-03.parquet", hdfs_path + "yellow_tripdata_2022-04.parquet", hdfs_path + "yellow_tripdata_2022-05.parquet", hdfs_path + "yellow_tripdata_2022-06.parquet")

# create the rdd from the dataframe
rdd_taxi_trips = df_taxi_trips.rdd

# read csv file
df_taxi_zone_lookup = spark.read.csv(hdfs_path + "taxi_zone_lookup.csv")

# Create an RDD from dataframe 
rdd_taxi_zone_lookup = df_taxi_zone_lookup.rdd


# Query 1

start_Q1 = time.time()

# Να βρεθεί η διαδρομή με το μεγαλύτερο φιλοδώρημα (tip)τον Μάρτιο και σημείο άφιξης το "BatteryPark"
df_taxi_trips.filter(month(col("tpep_pickup_datetime")) == 3)\
    .join(df_taxi_zone_lookup, [df_taxi_trips.DOLocationID == df_taxi_zone_lookup._c0, df_taxi_zone_lookup._c2 == "Battery Park"])\
    .sort(desc("tip_amount"))\
    .show(1)


end_Q1 = time.time()
print(f'Q1 time taken: {end_Q1-start_Q1} seconds.')



# Query 2

start_Q2 = time.time()

# Να βρεθεί,για κάθε μήνα,η διαδρομή με το υψηλότερο ποσό στα διόδια. Αγνοήστε μηδενικά ποσά
df_taxi_trips.filter(col("total_amount") > 0)\
.groupBy(month(col("tpep_pickup_datetime")))\
.agg(max("Tolls_amount")\
.alias("max_Tolls_amount"))\
.sort(asc("month(tpep_pickup_datetime)"))\
.show()

end_Q2 = time.time()

print(f'Q2 time taken: {end_Q2-start_Q2} seconds.')
