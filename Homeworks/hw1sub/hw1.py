# Md Lutfar Rahman

from pyspark import SparkContext
import math

inf = math.inf


sc = SparkContext("local", "hw1 solution")


tgrap = "direct.graph"
DTest = "DTest"


#graph reading
############################################

def readGraph():
	g = {}
	with open(tgrap) as f:
		for line in f.readlines():
			a,b = line.split()
			a,b = int(a),int(b)
			if a in g:
				g[a].append(b)
			else:
				g[a] = [b]

	return g

def formulate(g, source):
	data = []
	for node, to_nodes in g.items():
		if node == source:
			data.append((node, [0] + to_nodes))
		else:
			data.append((node, [inf] + to_nodes))

	return data

def fmapper(x):
	#print("AAA",x)
	global visited
	data = []
	node, to_nodes = x
	D = to_nodes[0]
	if D == 0:
		data.append((node, [D]))

	if D >= 0 and D < inf:
		for p in to_nodes[1:]:
			data.append((p, [D+1]))


	if D == inf:
		data.append((-1, [-1,1]))

	if D == -1:
		data.append((-1, [-1,0]))
	else:
		data.append((node, to_nodes))

	#print(data)

	return data

def mapper(x):
	return x


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


def BFS(g, source, dest):

	q = [source,]
	visited = {}
	steps = {source:0}

	while len(q)>0:
		
		node = q.pop(0)
		visited[node] = True

		if node == dest:
			break

		if node in g:
			for a_node in g[node]:
				if a_node not in visited and a_node not in steps:
					q.append(a_node)
					steps[a_node] = steps[node]+1

					if a_node == dest:
						break

	if dest in steps:
		return steps[dest]

	return -1


g = readGraph()
print(len(g.keys()))
print("*****")
#print(naiveBFS(g, 288644, 211687))

#############################################
ansFile = open("DRes","w")
with open(DTest) as f:
	
	for line in f.readlines():
		ans = -1
		source, dest  = line.split()
		source, dest = int(source), int(dest)
		
		data = formulate(g, source)
		worst_count = len(data)
		
		data = sc.parallelize(data)

		inf_prev = 0
		for i in range(worst_count):
			data = data.flatMap(fmapper).reduceByKey(reducer)
			ansq = data.filter(lambda x: x[0]==dest).collect()

			if len(ansq) > 0:
				ansq = ansq[0][1][0]
				if ansq < inf:
					ans = ansq
					break

			inf_count = data.filter(lambda x: x[0]==-1).collect()[0][1][1]
			if inf_count == inf_prev:
				break
			else:
				inf_prev = inf_count


			#visited = data.filter(lambda x: x[1][0] < inf and x[1][0] > -1).collectAsMap()
			#print(visited)

			print("iteration >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>", i)

		print(ans)
		ansFile.write(str(ans)+'\n')
		

ansFile.close()



