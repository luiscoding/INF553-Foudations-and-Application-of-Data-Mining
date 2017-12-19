
import sys
from pyspark import SparkContext

ratingsFile = "/Users/mac/Desktop/INF553/HW1/ml-1m/ratings.dat"
usersFile = "/Users/mac/Desktop/INF553/HW1/ml-1m/users.dat"

sc = SparkContext("local[1]", "Simple App")

ratingsData = sc.textFile(ratingsFile)
usersData = sc.textFile(usersFile)


def g(x):
    print x


def text(x):
    tmpt = x.split('::')
    return [int(str(tmpt[0])), (int(str(tmpt[1])), float(tmpt[2].strip()))]


def user(x):
    tmpt = x.split('::')
    return [int(str(tmpt[0])), str(tmpt[1])]


def movie_tuple(x):
    return [(x[1][0][0], x[1][1]), x[1][0][1]]


def clean_tuple(x):
    return x[0][0], x[0][1], x[1]


def toCSVLine(data):
    return ','.join(str(d) for d in data)

ratingsData = ratingsData.map(lambda row: text(row))
usersData = usersData.map(lambda row: user(row))

finalData = ratingsData.join(usersData).map(lambda row: movie_tuple(row)).groupByKey()\
    .mapValues(list).map(lambda line: [line[0], sum(line[1])/len(line[1])]).sortByKey()\
    .map(lambda row: clean_tuple(row)).map(toCSVLine).repartition(1)
# finalData.saveAsTextFile("/Users/mac/Desktop/INF553_TA/HW1/result_Task1")
finalData.foreach(g)
