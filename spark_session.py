from pyspark.sql import SparkSession

def create_spark_session():
    """Create a spark session"""
    spark = SparkSession.builder.master("spark://192.168.0.2:7077").config("spark.executor.memory", "2850m")\
        .config("spark.executor.cores", "2")\
        .config("spark.driver.memory", "4g")\
        .config("spark.cleaner.periodicGC.interval", "1min")\
        .config("spark.memory.offHeap.enabled","true")\
        .config("spark.memory.offHeap.size","5g")\
        .config("spark.default.parallelism", "4")\
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")\
        .appName("ADVDB").getOrCreate()
    return spark