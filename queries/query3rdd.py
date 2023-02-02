
# Να βρεθεί, ανά15 ημέρες,ο μέσος όρος της απόστασης και του κόστους για όλες τις διαδρομές με σημείο αναχώρησης διαφορετικό από το σημείο άφιξης.
def query3rdd(rdd_taxi_trips):
    return  print(rdd_taxi_trips.filter(lambda x: x.PULocationID != x.DOLocationID)\
        .map(lambda x: ((x.tpep_pickup_datetime.day,x.tpep_pickup_datetime.month),(float(x.trip_distance),float(x.total_amount),1)))\
        .reduceByKey(lambda x,y: (x[0]+y[0],x[1]+y[1],x[2]+y[2]))\
        .map(lambda x: (x[0],(x[1][0]/x[1][2],x[1][1]/x[1][2])))\
        .sortBy(lambda x: (x[0][1],x[0][0]))\
        .zipWithIndex()\
        .map(lambda x: (int(str(x[1]/15)[0]),(x[0][1][0],x[0][1][1],1)))\
        .reduceByKey(lambda x,y: (x[0]+y[0],x[1]+y[1],x[2]+y[2]))\
        .map(lambda x: (x[0],(x[1][0]/x[1][2],x[1][1]/x[1][2])))\
        .sortByKey()\
        .take(20))







