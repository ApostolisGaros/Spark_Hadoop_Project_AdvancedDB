from pyspark.sql import SparkSession
from pyspark.sql.functions import col, asc, max, month
from pyspark.sql.functions import col

# Να βρεθεί,για κάθε μήνα,η διαδρομή με το υψηλότερο ποσό στα διόδια. Αγνοήστε μηδενικά ποσά
def query2(df_taxi_trips):
    return  df_taxi_trips.filter(col("Tolls_amount") > 0)\
    .groupBy(month(col("tpep_pickup_datetime")))\
    .agg(max("Tolls_amount")\
    .alias("max_Tolls_amount"))\
    .sort(asc("month(tpep_pickup_datetime)"))\
    .join(df_taxi_trips, [month(col("tpep_pickup_datetime")) == col("month(tpep_pickup_datetime)"), col("Tolls_amount") == col("max_Tolls_amount")])\
    .drop("month(tpep_pickup_datetime)","max_Tolls_amount")\
    .show()
