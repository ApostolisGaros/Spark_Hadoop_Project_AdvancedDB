from pyspark.sql.functions import col,sum,desc, row_number, asc, max, month, dayofmonth
from pyspark.sql.window import Window
from pyspark.sql.functions import col

# Να βρεθούν οι κορυφαίες πέντε (top 5) ημέρες ανά μήνα στις οποίες οι κούρσες είχαν το μεγαλύτερο ποσοστό σε tip
def query5(df_taxi_trips):
    return  df_taxi_trips.groupBy([month(col("tpep_pickup_datetime")), dayofmonth(col("tpep_pickup_datetime"))])\
    .agg(sum("Fare_amount").alias("sum_fare_amount"), sum("Tip_amount").alias("sum_tip_amount"))\
    .withColumn("tip_percentage", col("sum_tip_amount")/col("sum_fare_amount"))\
    .withColumn("index", row_number().over(Window.partitionBy("month(tpep_pickup_datetime)").orderBy(desc("tip_percentage"))))\
    .filter(col("index") <= 5)\
    .sort(asc("month(tpep_pickup_datetime)"),asc("index"))\
    .drop("sum_fare_amount", "sum_tip_amount")\
    .show(100)

