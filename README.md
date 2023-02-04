# Advanced Topics in Database Systems: Semester Project

Welcome to the semester project for the class "Advanced Topics in Database Systems"! This project was developed by [Apostolis Garos](https://github.com/ApostolisGaros) and [Nikos Vlachakis](https://github.com/NikosVlachakis), and is hosted on GitHub at [https://github.com/ApostolisGaros/Spark_Hadoop_Project_AdvancedDB](https://github.com/ApostolisGaros/Spark_Hadoop_Project_AdvancedDB).

In this project, we will be processing volume data using Apache Spark and HDFS. The data consists of records of taxi rides in New York City, and includes information such as pickup and drop-off times and locations, trip distances, fares, and payment types. The data is provided by the New York Taxi and Limousine Commission and is open to the public.

## Prerequisites

In order to run this code, you will need to have set up a multinode cluster and an HDFS. You will also need to have the Yellow Taxi Trip Records data for the months January to June 2022, as well as the file "taxi+\_zone_lookup.csv". These files should be in compressed parquet format.

## Running the code

To run the code, follow these steps:

1. Clone the repository:

    > git clone https://github.com/ApostolisGaros/Spark_Hadoop_Project_AdvancedDB.git

2. Navigate to the root directory of the project:
    > cd Spark_Hadoop_Project_AdvancedDB
3. Run the following command:
    > python main.py

This will execute the code in the `main.py` file, which will process the volume data using Apache Spark and HDFS. The python script will ask for the number of the query you want to run. The queries are numbered from 1 to 5.

## Additional notes

We will be using the DataFrame/SQL and RDD APIs in Apache Spark for this project. The exact field descriptions for the data can be found [here](https://www.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_yellow.pdf). If you have any questions or comments, please don't hesitate to reach out to us via our GitHub profiles.
