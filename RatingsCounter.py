import findspark
findspark.init()
from pyspark import SparkConf, SparkContext
import collections

conf = SparkConf().setMaster("local").setAppName("RatingsHistogram")
sc = SparkContext(conf=conf)

lines = sc.textFile("file:///Users/abbas/Documents/Projects_Repositories/SparkPython/data/ml-100k/u.data")
ratings = lines.map(lambda x: x.split()[2])
result = ratings.countByValue()
# print(type(result))
# print(result)

sortedResults = collections.OrderedDict(sorted(result.items()))
# print(type(sortedResults))
for key, value in sortedResults.items():
    print("%s %i" % (key, value))
