from pyspark import SparkContext
from pyspark import SparkConf
import random
import sys
import json
import re
import time

hash_fcn_nums = 40
start_time = time.time()

input_file = sys.argv[1]
output_file= sys.argv[2]
# input_file = "train_review.json"
# output_file= "output_task1_version2.json"

sc = SparkContext().getOrCreate()
RDD = sc.textFile(input_file).map(lambda line:line.split('\n')).map(lambda line:json.loads(line[0]))

# create a dictionary of index for all_user_id
ALL_User_ID = RDD.map(lambda x: x["user_id"]).distinct().collect()
ALL_User_ID.sort()
dict_user_index = {}
count = 0
for i in ALL_User_ID:
	dict_user_index[i] = count
	count = count + 1
user_length = len(ALL_User_ID)

a = random.sample(range(1, 1000), hash_fcn_nums)
b = random.sample(range(1, 1000), hash_fcn_nums)

def minhash_signature_matrix(business):
	res = []
	for i in range(hash_fcn_nums):
		hash_values = []
		for x in business[1]:
			value = (a[i]*x + b[i]) % user_length
			hash_values.append(value)
		res.append(min(hash_values))
	return res

def LSH(x):
	res = []
	for i in range(len(x[1])):
		res.append(((i, x[1][i]), [x[0]]))
	return res

def run_all_pairs(x):
	A = set()
	for i in range(len(x)):
		for j in range(i+1, len(x)):
			if (x[i], x[j]) not in A and (x[j], x[i]) not in A:
				A.add((x[i], x[j]))
	return list(A)

def Jaccard(x):
	Jaccard = len(set(dic[x[0]]) & set(dic[x[1]]))/len(set(dic[x[0]]) | set(dic[x[1]]))
	return (x[0], x[1], Jaccard)

table_RDD = RDD.map(lambda x: (x["business_id"],[dict_user_index[x["user_id"]]])).reduceByKey(lambda x,y: x+y).sortBy(lambda x: x[0])
dic = table_RDD.collectAsMap()


candidates = table_RDD.map(lambda x: (x[0], minhash_signature_matrix(x))).flatMap(lambda x: LSH(x)).reduceByKey(lambda x,y: x+y).filter(lambda x: len(x[1]) > 1).map(lambda x: tuple(x[1])).distinct()
candidates = candidates.flatMap(lambda x: run_all_pairs(x)).distinct().map(lambda x: Jaccard(x)).filter(lambda x: x[2] >= 0.05)

pair_candidates = candidates.collect()

with open(output_file, "w") as output_file1:
	for x,y,Jaccard in pair_candidates:
		output_file1.write(json.dumps({"b1": x, "b2": y, "sim": Jaccard})+"\n")
output_file1.close()

print('Number of true positives', len(pair_candidates))
print('Accuracy:' , len(pair_candidates)/59435)
print('Duration: ', time.time() - start_time)
