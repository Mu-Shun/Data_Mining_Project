from pyspark import SparkContext
from pyspark import SparkConf
from operator import add
import sys
import json

review = sys.argv[1]
business= sys.argv[2]
output_file= sys.argv[3]
if_spark = sys.argv[4]
top_n = int(sys.argv[5])
# review = "review.json"
# business = "business.json"
# output_file = "output_file_spark.json"
# if_spark = "spark"
# top_n = int("50")

dic = {}
if if_spark == "spark":
	sc = SparkContext().getOrCreate()
	RDD_review = sc.textFile(review)
	RDD_review = RDD_review.map(lambda line:line.split('\n')).map(lambda line:json.loads(line[0])).map(lambda x: (x["business_id"], x["stars"]))

	RDD_business = sc.textFile(business)
	RDD_business = RDD_business.map(lambda line:line.split('\n')).map(lambda line:json.loads(line[0])).map(lambda x: (x["business_id"], x["categories"]))

	RDD = RDD_business.join(RDD_review).map(lambda x:(x[1][1], x[1][0])).flatMapValues(lambda x: x.split(',') if x else "").map(lambda x: [x[1].strip(" "), x[0]])\
	.aggregateByKey((0,0), lambda U,v: (U[0]+v, U[1]+1), lambda U1,U2: (U1[0] + U2[0], U1[1] + U2[1]))\
	.map(lambda x: (x[0], float(x[1][0])/x[1][1])).\
	sortBy(lambda x: (-x[1],x[0])).take(top_n)
	dic["result"] = RDD
else:
	dic_b = {}
	dic_join = {}
	b = open(business,"r",encoding="utf-8")
	for i in b:
		line = json.loads(i)
		if line["business_id"] not in dic_b:
			dic_b[line["business_id"]] = [line["categories"],[]]

	r = open(review,"r",encoding="utf-8")
	for i in r:
		line = json.loads(i)
		if line["business_id"] in dic_b:
			dic_b[line["business_id"]][1].append(line["stars"])

	for i in dic_b:
		if dic_b[i][1] and dic_b[i][0] not in dic_join:
			dic_join[dic_b[i][0]] = dic_b[i][1]
		elif dic_b[i][1] and dic_b[i][0] in dic_join:
			dic_join[dic_b[i][0]] = dic_join[dic_b[i][0]] + dic_b[i][1]
	
	dic_count = {}
	count = 1
	for i in dic_join:
		if i:
			a_list = i.split(",")
			for j in range(len(a_list)):
				a_list[j] = a_list[j].strip(" ")
				if a_list[j] not in dic_count:
					dic_count[a_list[j]] = []
				for k in dic_join[i]:
					dic_count[a_list[j]].append(k)
	rank = []
	for i in dic_count:
		rank.append([i, sum(dic_count[i])/len(dic_count[i])])
	rank.sort(key=lambda k: (-k[1],k[0]))
	dic["result"] = rank[:top_n]

with open(output_file, "w") as output_file1:
    json.dump(dic, output_file1)




