from pyspark import SparkContext
from operator import add
from pyspark import SparkContext

from graphframes import *

sc = SparkContext("local", "Hello World App")


data = [3,7,99]

data_RDD = sc.parallelize(data)


def mapper(x):
	return [(x,1), (x,2)]

def reducer(a, b):
	return a + b

counts = data_RDD.map(mapper).reduceByKey(reducer).collect() #.sortBy(lambda x: x[1], ascending=False).collect()

for (word, count) in counts:
    print("{}: {}".format(word, count))

