## This program finds the most popular movie in the input dataset.
## The input dataset contains 4 fields on each row: User Id, Movie Id, Rating, and Timestamp.
## The fields are separated by whitespace.
## The most popular movie is the one that appears most frequently in the dataset.
from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("PopularMovies")
sc = SparkContext(conf = conf)

lines = sc.textFile("file:///SparkCourse/ml-100k/u.data")
movies = lines.map(lambda x: (int(x.split()[1]), 1))

## Calculate the number of ratings for each movie id
movieCounts = movies.reduceByKey(lambda x,y : x+y)

flipped = movieCounts.map( lambda (x, y) : (y, x) )
sortedMovies = flipped.sortByKey()

results = sortedMovies.collect()

for result in results:
    print(result)