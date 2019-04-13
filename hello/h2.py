from pyspark import SparkContext
from operator import add
from pyspark import SparkContext

from graphframes import *

sc = SparkContext("local", "Hello World App")


data = ["one","Sixteen","two","nine","five",
  "nine","Sixteen","four","nine","Sixteen","four"]

data_RDD = sc.parallelize(data)


def mapper(x):
	return (x,1)

def reducer(a, b):
	return a + b

counts = data_RDD.map(mapper).reduceByKey(reducer).collect() #.sortBy(lambda x: x[1], ascending=False).collect()

for (word, count) in counts:
    print("{}: {}".format(word, count))

