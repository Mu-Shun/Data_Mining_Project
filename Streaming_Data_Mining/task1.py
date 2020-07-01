from pyspark import SparkContext
from pyspark import SparkConf
import random
import sys
import json
import re
import time
import binascii

hash_fcn_nums = 10
start_time = time.time()

train_file = sys.argv[1]
predict_file = sys.argv[2]
output_file = sys.argv[3]
# train_file = "business_first.json"
# predict_file = "business_second.json"
# output_file = "output11.txt"

sc = SparkContext().getOrCreate()
RDD_train = sc.textFile(train_file).map(json.loads).filter(lambda x: "city" in x and x["city"]).map(lambda x: x["city"]).distinct().map(lambda x: int(binascii.hexlify(x.encode('utf8')),16))
city_length = RDD_train.count()*1000
a = random.sample(range(1, city_length*1000), hash_fcn_nums)
b = random.sample(range(1, city_length*1000), hash_fcn_nums)

def hash_fcn(x):
	res = []
	for i in range(hash_fcn_nums):
		res.append(((a[i]*x + b[i]) % city_length))
	return res

def prediction(x):
	if not x: return "0"
	hash_res = hash_fcn(x)
	for num in hash_res:
		if Filter[num] == 0:
			return "0"
	return "1"

RDD_Bloom_Filtering = RDD_train.flatMap(lambda x: hash_fcn(x)).distinct().collect()
Filter = [0] * city_length
for num in RDD_Bloom_Filtering: Filter[num] = 1
RDD_predict = sc.textFile(predict_file).map(json.loads).map(lambda x: x["city"] if "city" in x and x["city"] else 0)\
.map(lambda x: int(binascii.hexlify(x.encode('utf8')),16) if x else 0).map(lambda x: prediction(x)).collect()

with open(output_file, "w") as output_file1:
	output_file1.write(" ".join(RDD_predict))
output_file1.close()

print('Duration: ', time.time() - start_time)