from pyspark.sql import SparkSession
from pyspark.sql.functions import col, desc, row_number, asc, max, hour,dayofweek
from pyspark.sql.window import Window
from pyspark.sql.functions import col

# Να βρεθούν οι τρεις μεγαλύτερες (top3)ώρες αιχμής ανάημέρα της εβδομάδος, εννοώντας τις ώρες (π.χ., 7-8πμ, 3-4μμ, κλπ) της ημέρας με τον μεγαλύτερο αριθμό επιβατών σε μια κούρσα ταξί.Ο υπολογισμός αφορά όλους τους μήνες
def query4(df_taxi_trips):
    return  df_taxi_trips.groupBy([dayofweek(col("tpep_pickup_datetime")), hour(col("tpep_pickup_datetime"))])\
    .agg(max("Passenger_count").alias("max_passenger_count"))\
    .withColumn("index", row_number().over(Window.partitionBy("dayofweek(tpep_pickup_datetime)").orderBy(desc("max_passenger_count"))))\
    .filter(col("index") <= 3)\
    .sort(asc("dayofweek(tpep_pickup_datetime)"),asc("index"))\
    .show()