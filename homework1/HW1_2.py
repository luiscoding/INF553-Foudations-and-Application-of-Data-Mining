
import sys
from pyspark import SparkContext

ratingsFile = "/Users/mac/Desktop/INF553/HW1/ml-1m/ratings.dat"
usersFile = "/Users/mac/Desktop/INF553/HW1/ml-1m/users.dat"
moviesFile = "/Users/mac/Desktop/INF553/HW1/ml-1m/movies.dat"

sc = SparkContext("local[1]", "Simple App")

ratingsData = sc.textFile(ratingsFile)
usersData = sc.textFile(usersFile)
movieData = sc.textFile(moviesFile)


def g(x):
    print x


# (userID, movieID. rating)
def text(x):
    tmpt = x.split('::')
    return [int(str(tmpt[0])), (int(str(tmpt[1])), float(tmpt[2].strip()))]


# (userID, gender)
def user(x):
    tmpt = x.split('::')
    return [int(str(tmpt[0])), str(tmpt[1])]


# (movieID, genres)
def movie(x):
    tmpt = x.split('::')
    return [int(str(tmpt[0])), str(tmpt[2])]


# (movieID, (gender, rating))
def movie_tuple(x):
    return [x[1][0][0], (x[1][1], x[1][0][1])]


# (movieID, ((gender, rating), genres)) -> ((genres, gender), rating)
def genres_tuple(x):
    return [(x[1][1], x[1][0][0]), x[1][0][1]]


def clean_tuple(x):
    return x[0][0], x[0][1], x[1]


def toCSVLine(data):
    return ','.join(str(d) for d in data)

ratingsData = ratingsData.map(lambda row: text(row))
usersData = usersData.map(lambda row: user(row))
movieData = movieData.map(lambda row: movie(row))

finalData = ratingsData.join(usersData).map(lambda row: movie_tuple(row)).join(movieData)\
    .map(lambda row: genres_tuple(row)).groupByKey().mapValues(list)\
    .map(lambda line: [line[0], sum(line[1])/len(line[1])]).sortByKey()\
    .map(lambda row: clean_tuple(row)).map(toCSVLine).repartition(1)
finalData.saveAsTextFile("/Users/mac/Desktop/INF553_TA/HW1/result_Task2")
# finalData.write.format('com.databricks.spark.csv').options(delimiter="\t", codec="org.apache.hadoop.io.compress.GzipCodec")\
#     .save("/Users/mac/Desktop/INF553_TA/HW1/result_task2.csv")
# finalData.foreach(g)
