# Md Lutfar Rahman

from pyspark import SparkContext
sc = SparkContext("local", "hw2 solution")

ugraph = "undirect.graph"
import math

inf = math.inf

#graph reading
############################################

def readGraph():
	g = {}
	with open(ugraph) as f:
		for line in f.readlines():
			a,b = line.split()
			a,b = int(a), int(b)
			if a in g:
				g[a].append(b)
			else:
				g[a] = [b]

			if b in g:
				g[b].append(a)
			else:
				g[b] = [a]

			# if len(g.keys()) > 60000:
			# 	break
	return g

def formulate(g):
	data = []
	for node, to_nodes in g.items():
		data.append((node, [inf] + to_nodes))
	return data

def fmapper(x):
	global visited
	data = []
	node, to_nodes = x
	D = to_nodes[0]
	if D == 0:
		data.append((node, [D]))

	if D >= 0 and D < inf:
		for p in to_nodes[1:]:
			if p not in visited:
				data.append((p, [D+1]))


	if D == inf:
		data.append((-1, [-1,1]))

	if D == -1:
		data.append((-1, [-1,0]))
	else:
		data.append((node, to_nodes))

	#print(data)

	return data


def reducer(v1,v2):

	#print(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
	#print(v1,v2)

	if v1[0] == -1:
		return [-1, v1[1]+ v2[1]]

	D = min(v1[0], v2[0])

	if len(v1) == 1 and len(v2) == 1:
		return [D]
	elif len(v1) == 1:
		v2[0] = D
		return v2
	else:
		v1[0] = D
		return v1


###############################################
g = readGraph()
print(len(g.keys()))


data = formulate(g)
worst_count = len(data)
	
data = sc.parallelize(data)
ansFile = open("CCRes","w")

comp = 0
visited = {}
while True:
	pos_source = data.filter(lambda x: x[1][0] == inf).collect()
	if len(pos_source) > 0:
		source = sc.parallelize([(pos_source[0][0], [0])])
		data = data.union(source)
		comp+=1
	else:
		break
	
	inf_prev = 0
	for i in range(worst_count):
		data = data.flatMap(fmapper).reduceByKey(reducer)

		inf_count = data.filter(lambda x: x[0]==-1).collect()[0][1][1]
		if inf_count == inf_prev:
			break
		else:
			inf_prev = inf_count

		visited = data.filter(lambda x: x[1][0] < inf and x[1][0] > -1).collectAsMap()
		#print(visited)

	print("components", comp)

print(comp)
ansFile.write(str(comp)+'\n')
		

ansFile.close()


