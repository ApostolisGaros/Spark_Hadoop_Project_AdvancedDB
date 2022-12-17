from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, desc, row_number, asc, max, month, dayofmonth, hour,round,floor
import sys,time,os
from pyspark.sql.window import Window
from pyspark.sql.functions import col, row_number

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

# start_Q2 = time.time()

# # Να βρεθεί,για κάθε μήνα,η διαδρομή με το υψηλότερο ποσό στα διόδια. Αγνοήστε μηδενικά ποσά
# df_taxi_trips.filter(col("total_amount") > 0)\
# .groupBy(month(col("tpep_pickup_datetime")))\
# .agg(max("Tolls_amount")\
# .alias("max_Tolls_amount"))\
# .sort(asc("month(tpep_pickup_datetime)"))\
# .show()

# end_Q2 = time.time()

# print(f'Q2 time taken: {end_Q2-start_Q2} seconds.')

# Query 3

#Να βρεθεί, ανά15 ημέρες,ο μέσος όρος της απόστασης και του κόστους για όλες τις διαδρομές με σημείο αναχώρησης διαφορετικό από το σημείο άφιξης.
# start_Q3_DF = time.time()

# df_taxi_trips.filter(col("PULocationID") != col("DOLocationID"))\
# .groupBy([dayofmonth(col("tpep_pickup_datetime")),month(col("tpep_pickup_datetime"))])\
# .agg(sum("trip_distance").alias("sum_trip_distance"),sum("total_amount").alias("sum_total_amount"))\
# .sort(asc("month(tpep_pickup_datetime)"),asc("dayofmonth(tpep_pickup_datetime)"))\
# .withColumn("index", row_number().over(Window.orderBy("month(tpep_pickup_datetime)","dayofmonth(tpep_pickup_datetime)")))\
# .withColumn("group", floor((col("index")-1)/15))\
# .groupBy("group")\
# .agg(round(sum("sum_trip_distance")/15,2).alias("avg_trip_distance"),round(sum("sum_total_amount")/15,2).alias("avg_total_amount"))\
# .show()

# end_Q3_DF = time.time()

# print(f'Q3_DF time taken: {end_Q3_DF-start_Q3_DF} seconds.')

#Να βρεθεί, ανά15 ημέρες,ο μέσος όρος της απόστασης και του κόστους για όλες τις διαδρομές με σημείο αναχώρησης διαφορετικό από το σημείο άφιξης.
# using RDD

# start_Q3_RDD = time.time()

# print(rdd_taxi_trips.filter(lambda x: x.PULocationID != x.DOLocationID)\
# .map(lambda x: (str((x.tpep_pickup_datetime.day,x.tpep_pickup_datetime.month)),(float(x.trip_distance),float(x.total_amount))))\

# .take(20))
    









# .groupByKey()\
# .mapValues(list)\
# .map(lambda x: list(x))\
# .take(20))

# .mapValues(lambda x: (sum([y.trip_distance for y in x]),sum([y.total_amount for y in x])))\
# .sortByKey()\
# .map(lambda x: (x[0][1],x[0][0],x[1][0],x[1][1]))\
# .map(lambda x: (x[0],x[1],x[2],x[3],(x[0],x[1])))\
# .map(lambda x: (x[4],(x[2],x[3])))\
# .groupByKey()\
# .mapValues(lambda x: (sum([y[0] for y in x])/15,sum([y[1] for y in x])/15))\
# .sortByKey()\
# .map(lambda x: (x[0][0],x[0][1],x[1][0],x[1][1]))\

# end_Q3_RDD = time.time()

# print(f'Q3_RDD time taken: {end_Q3_RDD-start_Q3_RDD} seconds.')
