# Md Lutfar Rahman

from pyspark import SparkContext
import bisect

sc = SparkContext("local", "hw3 solution")

def dataReader(f):
	f = open(f)
	arr =  f.readlines()[0].split(" ")
	f.close()
	return [int(x) for x in arr]

rdata = dataReader("P3Test/data")
rindex = dataReader("P3Test/rindx")
cindex = dataReader("P3Test/cindx")
xvector = dataReader("P3Test/xvector")

def formulate():
	data = []
	for i in range(len(rdata)):

		v1 = rdata[i]
		v2 = xvector[cindex[i]]

		ind = bisect.bisect_left(rindex, i+1)
		data.append((ind-1, v1*v2))

	return data


def mapper(x):
	return x


def reducer(a, b):
	return a+b


data = formulate()
result = sc.parallelize(data).map(mapper).reduceByKey(reducer).collect()
ans = [0]*len(xvector)
for r in result:
	ans[r[0]] = r[1]

ans = " ".join(str(x) for x in ans)
ansFile = open("yvector","w")
ansFile.write(ans+'\n')
ansFile.close()

#print(ans)