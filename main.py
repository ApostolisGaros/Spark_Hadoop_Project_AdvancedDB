from pyspark.sql.functions import col, lit
import sys,time
from pyspark.sql.functions import col
from spark_session import create_spark_session
from queries import query1, query2, query3df,query3rdd, query4, query5

# take user input
queryNumber = input("Enter query number: ") 
if queryNumber == "3":
    # specify if is df or rdd
    queryType = input("Enter query type (df/rdd): ")
    if queryType not in ["df","rdd"]:
        print("Invalid query type")
        sys.exit()

# check if query number and queryType is valid
if queryNumber not in ["1","2","3","4","5"]:
    print("Invalid query number")
    sys.exit()


spark = create_spark_session()
print("spark session created")

# Path to the data
hdfs_path = "hdfs://192.168.0.2:9000/user/user/data/"

# Read the Parquet files from HDFS and create a dataframe
df_taxi_trips = spark.read.parquet(hdfs_path + "yellow_tripdata_2022-01.parquet", hdfs_path + "yellow_tripdata_2022-02.parquet", hdfs_path + "yellow_tripdata_2022-03.parquet", hdfs_path + "yellow_tripdata_2022-04.parquet", hdfs_path + "yellow_tripdata_2022-05.parquet", hdfs_path + "yellow_tripdata_2022-06.parquet")

# keep the first 6 months of 2022
df_taxi_trips = df_taxi_trips.filter(
  (col("tpep_pickup_datetime") >= lit("2022-01-01")) &
  (col("tpep_pickup_datetime") < lit("2022-07-01"))
)

# create the rdd from the dataframe
rdd_taxi_trips = df_taxi_trips.rdd

# read csv file
df_taxi_zone_lookup = spark.read.csv(hdfs_path + "taxi_zone_lookup.csv")

# Create an RDD from dataframe 
rdd_taxi_zone_lookup = df_taxi_zone_lookup.rdd


# Query 1

if queryNumber == "1":
    
    start_Q1 = time.time()
    q1results = query1.query1(df_taxi_trips, df_taxi_zone_lookup)
    end_Q1 = time.time()

    print(q1results)

    print(f'Q1 time taken: {end_Q1-start_Q1} seconds.')

# Query 2

if queryNumber == "2":

    start_Q2 = time.time()
    q2results = query2.query2(df_taxi_trips)
    end_Q2 = time.time()

    for result in q2results:
        print(result)

    print(f'Q2 time taken: {end_Q2-start_Q2} seconds.')

# Query 3

if queryNumber == "3":
    
    if queryType == "df": 

        start_Q3_DF = time.time()
        q3dfresults = query3df.query3df(df_taxi_trips)
        end_Q3_DF = time.time()

        for result in q3dfresults:
            print(result)

        print(f'Q3_DF time taken: {end_Q3_DF-start_Q3_DF} seconds.')
       
    if queryType == "rdd":
        
        start_Q3_RDD = time.time()
        print(query3rdd.query3rdd(rdd_taxi_trips))
        end_Q3_RDD = time.time()

        print(f'Q3_RDD time taken: {end_Q3_RDD-start_Q3_RDD} seconds.')


# Query 4

if queryNumber == "4":

    start_Q4 = time.time()
    q4results =  query4.query4(df_taxi_trips)
    end_Q4 = time.time()

    for result in q4results:
        print(result)

    print(f'Q4 time taken: {end_Q4-start_Q4} seconds.')

# Query 5

if queryNumber == "5":

    start_Q5 = time.time()
    q5results = query5.query5(df_taxi_trips)
    end_Q5 = time.time()

    for result in q5results:
        print(result)

    print(f'Q5 time taken: {end_Q5-start_Q5} seconds.')






