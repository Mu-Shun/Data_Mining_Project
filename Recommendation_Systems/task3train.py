from pyspark import SparkContext
from pyspark import SparkConf
import random
import sys
import json
import re
import time
import math 

start_time = time.time()

train_file = sys.argv[1]
model_file= sys.argv[2]
cf_type = sys.argv[3]
# train_file = "train_review.json"
# model_file= "task3_user_model111.json"
# cf_type= "user_based"

# Use for item_based
def average_rate_from_same_user(array):
	count_and_stars = {}
	res = {}
	for i in array:
		if i[0] not in count_and_stars:
			count_and_stars[i[0]] = [i[1], 1]
		else:
			count_and_stars[i[0]][0] = count_and_stars[i[0]][0] + i[1]
			count_and_stars[i[0]][1] = count_and_stars[i[0]][1] + 1
	for i in count_and_stars:
		res[i] = count_and_stars[i][0]/count_and_stars[i][1]
	return res

# Use for item_based
def generate_pairs(array):
	array = list(array)
	res = []
	for i in range(len(array)):
		for j in range(i+1, len(array)):
			Set = Business_Based_User_Dictionary[array[i]].keys() & Business_Based_User_Dictionary[array[j]].keys()
			if len(Set) >= 3:
				res.append(((array[i], array[j]), list(Set)))
	return res

# Use for item_based and user_based
def pearson(x):
	sum_item1, sum_item2= 0, 0
	sum_item2 = 0
	for item in x[1]:
		sum_item1 = sum_item1 + Business_Based_User_Dictionary[x[0][0]][item]
		sum_item2 = sum_item2 + Business_Based_User_Dictionary[x[0][1]][item]
	avg_item1 = sum_item1/len(x[1])
	avg_item2 = sum_item2/len(x[1])
	denominator_item1, denominator_item2, fraction = 0, 0, 0
	for item in x[1]:
		fraction = fraction + (Business_Based_User_Dictionary[x[0][0]][item]-avg_item1)*(Business_Based_User_Dictionary[x[0][1]][item]-avg_item2)
		denominator_item1 = denominator_item1 + (Business_Based_User_Dictionary[x[0][0]][item]-avg_item1)**2
		denominator_item2 = denominator_item2 + (Business_Based_User_Dictionary[x[0][1]][item]-avg_item2)**2
	if denominator_item1*denominator_item2 != 0:
		return (x[0][0], x[0][1],  fraction/(math.sqrt(denominator_item1*denominator_item2)))
	else:
		return (x[0][0], x[0][1], 0)

# Use for user_based
def minhash_signature_matrix(business):
	res = []
	for i in range(hash_fcn_nums):
		hash_values = []
		for x in business[1]:
			value = (a[i]*x + b[i]) % Business_length
			hash_values.append(value)
		res.append(min(hash_values))
	return res

# Use for user_based
def LSH(x):
	res = []
	for i in range(len(x[1])):
		res.append(((i, x[1][i]), x[0]))
	return res

# Use for user_based
def run_all_pairs(x):
	A = set()
	for i in range(len(x)):
		for j in range(i+1, len(x)):
			if (x[i], x[j]) not in A and (x[j], x[i]) not in A:
				A.add((x[i], x[j]))
	return list(A)

# Use for user_based
def Jaccard(x):
	Intersaction = Business_Based_User_Dictionary[x[0]].keys() & Business_Based_User_Dictionary[x[1]].keys()
	UNION = Business_Based_User_Dictionary[x[0]].keys() | Business_Based_User_Dictionary[x[1]].keys()
	Jaccard = len(Intersaction)/len(UNION)
	if Jaccard >= 0.01 and len(Intersaction) >= 3:
		return ((x[0], x[1]), list(Intersaction))
	else:
		return ((x[0], x[1]), [])

sc = SparkContext().getOrCreate()

if cf_type == "item_based":
	Business = sc.textFile(train_file).map(json.loads).map(lambda x: (x["business_id"], (x["user_id"], float(x["stars"])))).groupByKey().mapValues(average_rate_from_same_user)
	Business_Based_User_Dictionary = Business.collectAsMap()
	Business_List = Business.map(lambda x: (1, x[0])).groupByKey().flatMap(lambda x: generate_pairs(x[1])).map(pearson).filter(lambda x: x[2] > 0).collect()
	print("Length of Candidates :", len(Business_List))

	with open(model_file, "w") as output_file1:
		for b1, b2, pearson in Business_List:
			output_file1.write(json.dumps({"b1": b1, "b2": b2, "sim": pearson})+"\n")
else:
	hash_fcn_nums = 40
	a = random.sample(range(1, 1000), hash_fcn_nums)
	b = random.sample(range(1, 1000), hash_fcn_nums)
	User = sc.textFile(train_file).map(json.loads)
	# Actually it is User_Based_Business_Dictionary but using same variable for convenient
	Business_Based_User_Dictionary = User.map(lambda x: (x["user_id"], (x["business_id"], float(x["stars"])))).groupByKey().mapValues(average_rate_from_same_user).collectAsMap()
	ALL_Business_ID = User.map(lambda x: x["business_id"]).distinct().sortBy(lambda x: x).collect()
	dict_business_index = {}
	count = 0
	for i in ALL_Business_ID:
		dict_business_index[i] = count
		count = count + 1
	Business_length = len(ALL_Business_ID)
	candidates = User.map(lambda x: (x["user_id"],dict_business_index[x["business_id"]])).groupByKey().mapValues(list).sortBy(lambda x: x[0])\
	.map(lambda x: (x[0], minhash_signature_matrix(x))).flatMap(lambda x: LSH(x)).groupByKey().map(lambda x: tuple(x[1])).filter(lambda x: len(x) > 1)\
	.distinct().flatMap(lambda x: run_all_pairs(x)).distinct().map(lambda x: Jaccard(x)).filter(lambda x: len(x[1]) > 0).map(pearson).filter(lambda x: x[2] > 0).collect()
	print("Length of Candidates :", len(candidates))

	with open(model_file, "w") as output_file1:
		for u1, u2, pearson in candidates:
			output_file1.write(json.dumps({"u1": u1, "u2": u2, "sim": pearson})+"\n")
output_file1.close()
print('Duration: ', time.time() - start_time)

