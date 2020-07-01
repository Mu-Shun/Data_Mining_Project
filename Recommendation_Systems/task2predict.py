from pyspark import SparkContext
from pyspark import SparkConf
import sys
import json
import re
import time
import math 

start_time = time.time()

test_file = sys.argv[1]
model_file= sys.argv[2]
output_file = sys.argv[3]
# test_file = "test_review.json"
# model_file= "model_file_output1.json"
# output_file= "output_file"

dic = {}
with open(model_file) as file:
	dic = json.load(file)

def run_result(x):
	if x[0] not in dic["User_profile"] or x[1] not in dic["Business_profile"]:
		return (0, 0, 0)
	else:
		cosine = len(set(dic["User_profile"][x[0]]) & set(dic["Business_profile"][x[1]]))/math.sqrt(len(dic["User_profile"][x[0]])*len(dic["Business_profile"][x[1]]))
		return (x[0], x[1], cosine)

sc = SparkContext().getOrCreate()
RDD = sc.textFile(test_file).map(json.loads).map(lambda x: (x['user_id'], x['business_id'])).map(run_result).filter(lambda x: x[2] >= 0.05).collect()
# print(RDD)

with open(output_file, "w") as output_file1:
	for x,y,sim in RDD:
		output_file1.write(json.dumps({"user_id": x, "business_id": y, "sim": sim})+"\n")
output_file1.close()

print('Duration: ', time.time() - start_time)
