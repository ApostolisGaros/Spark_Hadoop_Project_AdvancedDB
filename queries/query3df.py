from pyspark.sql.functions import col,avg, row_number, asc, month, dayofmonth,round,floor
from pyspark.sql.window import Window
from pyspark.sql.functions import col

# Να βρεθεί, ανά15 ημέρες,ο μέσος όρος της απόστασης και του κόστους για όλες τις διαδρομές με σημείο αναχώρησης διαφορετικό από το σημείο άφιξης.
def query3df(df_taxi_trips):
    return  df_taxi_trips.filter(col("PULocationID") != col("DOLocationID"))\
        .groupBy([dayofmonth(col("tpep_pickup_datetime")),month(col("tpep_pickup_datetime"))])\
        .agg(avg("trip_distance").alias("avg_trip_distance"),avg("total_amount").alias("avg_total_amount"))\
        .sort(asc("month(tpep_pickup_datetime)"),asc("dayofmonth(tpep_pickup_datetime)"))\
        .withColumn("index", row_number().over(Window.orderBy("month(tpep_pickup_datetime)","dayofmonth(tpep_pickup_datetime)")))\
        .withColumn("group", floor((col("index")-1)/15))\
        .groupBy("group")\
        .agg(round(avg("avg_trip_distance"),2).alias("15_day_avg_trip_distance"),round(avg("avg_total_amount"),2).alias("15_day_avg_total_amount"))\
        .collect()
