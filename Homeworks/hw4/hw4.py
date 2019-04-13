# Md Lutfar Rahman

from pyspark import SparkContext
import math
import bisect

sc = SparkContext("local", "hw4 solution")

total_products = 0
total_queries = 0
pdict = None
product = 'product'
query = 'query'

def queryReader(f):
	global total_queries
	queries = []
	f = open(f)
	q_id = 0
	for line in f.readlines():
		a_query = line.split()

		if len(a_query) < 2:
			break
			
		a_query = [q_id] + [int(x) for x in a_query]
		queries.append(a_query)
		q_id += 1
	#total_queries = q_id
	f.close()
	return queries

def fmapper(query):
	global total_products

	print(query)
	q_id, x, k, m, r = query  # top k product, category x, reviews >= m, rating >= r
	
	data = []
	for i in range(total_products):
		data.append(((q_id, i), [i, 0, x, k, m, r]))
		data.append(((q_id, i), [i, 0, x, k, m, r]))

	#print(data)
	return data

def mapper(query):
	return query


def reducer(p1, p2):
	i, match, x, k, m, r = p1
	match, m, r = search_query(i,x,m,r)
	return [i, match, x, k, m, r]

def search_query(pid,cat,min_reviews,min_rating):

	global pdict

	# if pid == 174839:
	# 	print('>>>>>>>>>>>>>>>>>',pdict[pid])

	

	if pdict is None:
		pdict = createProductDataDict()

	if pdict[pid]['reviews'] >= min_reviews and pdict[pid]['rating'] >= min_rating and cat in pdict[pid]['categories']: 
		match = 1
		reviews = pdict[pid]['reviews']
		rating = pdict[pid]['rating']

	else:
		match = 0
		reviews = -1
		rating = -1


	return match, reviews, rating


def createProductDataDict():
	pdict = {}
	pfile = open(product)
	import re
	regex = r"^\nId((.+)\n)+$"

	matches = re.finditer(regex, pfile.read(), re.MULTILINE)
	for match in matches:
		a_product =  createProductDict(match.group())
		pdict[a_product['Id']] = a_product
	
	pfile.close()
	return pdict


def readCategories(c_str):
	cats = set()
	words = c_str.replace('[',']').split(']')
	for word in words:
		if word.strip().isdigit():
			cats.add(int(word.strip()))

	return cats


def createProductDict(p_str):
	lines = p_str.split('\n')
	#print(lines)
	Id = 0
	categories = set()
	reviews = 0
	rating = 0
	for i in range(len(lines)):
		if "Id:   " in lines[i]:
			Id = int(lines[i].strip().split(":")[1].strip())
		elif "reviews: " in lines[i]:
			tdata = lines[i].strip().split(":")
			reviews = int(tdata[2].strip().split()[0].strip())
			rating =  float(tdata[4].strip())
		
		elif "categories" in lines[i]:
			cat_no = int(lines[i].strip().split(":")[1])
			for j in range(cat_no):
				#print(j)
				cats = readCategories(lines[i+j+1])
				categories = categories.union(cats)



	return {'Id':Id, 'reviews': reviews, 'rating': rating, 'categories': list(categories)}
	


if __name__ == "__main__":

	pdict = createProductDataDict()
	total_products = len(pdict.keys())
	print(total_products)

	print(pdict[174839])

	 # for Id, a_product in pdict.items():
	 # 	if 2159 in a_product['categories']:
	 # 		print(a_product)



	data = queryReader(query)
	total_queries = len(data)
	result = sc.parallelize(data).flatMap(fmapper).map(mapper).reduceByKey(reducer).filter(lambda x: x[1][1]==1)

	ansFile = open("Recommend","w")
	for q_id in range(total_queries):
		k = data[q_id][2]
		qresult = result.filter(lambda x: x[0][0] == q_id).top(k, key=lambda x: (x[1][5], x[1][4], - x[1][0]))
		for qr in qresult:
			ans = str(qr[0][1])
			ansFile.write(ans+'\n')
		#ansFile.write('******\n')
		#print(qresult)
		#break

	ansFile.close()
