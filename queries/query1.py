from pyspark.sql.functions import col, desc, month
from pyspark.sql.functions import col

# Να βρεθεί η διαδρομή με το μεγαλύτερο φιλοδώρημα (tip)τον Μάρτιο και σημείο άφιξης το "BatteryPark"
def query1(df_taxi_trips, df_taxi_zone_lookup):
    return  df_taxi_trips.filter(month(col("tpep_pickup_datetime")) == 3)\
        .join(df_taxi_zone_lookup, [df_taxi_trips.DOLocationID == df_taxi_zone_lookup._c0, df_taxi_zone_lookup._c2 == "Battery Park"])\
        .sort(desc("tip_amount"))\
        .drop("_c0","_c1","_c2","_c3")\
        .show(1)
