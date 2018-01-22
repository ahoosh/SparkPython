import findspark
findspark.init()
from pyspark import SparkConf, SparkContext


def loadMovieNames():
    movieNames = {}
    # count = 0
    with open("data/ml-100k/u.item", encoding="latin-1") as f:
        for line in f:
            # count += 1
            # print(count)
            fields = line.split('|')
            # print(fields[1])
            movieNames[int(fields[0])] = fields[1]
    return movieNames


conf = SparkConf().setMaster("local").setAppName("PopularMovies")
sc = SparkContext(conf=conf)

nameDict = sc.broadcast(loadMovieNames())

lines = sc.textFile("file:///Users/abbas/Documents/Projects_Repositories/SparkPython/data/ml-100k/u.data")
movies = lines.map(lambda x: (int(x.split()[1]), 1))
movieCounts = movies.reduceByKey(lambda x, y: x + y)

flipped = movieCounts.map( lambda x : (x[1], x[0]))
sortedMovies = flipped.sortByKey()

sortedMoviesWithNames = sortedMovies.map(lambda countMovie : (nameDict.value[countMovie[1]], countMovie[0]))

results = sortedMoviesWithNames.collect()

for result in results:
    print(result)
