from pyspark import SparkContext
from pyspark import SparkConf
import random
import sys
import json
import re
import time
import math 

start_time = time.time()
N_neighbors = 5

train_file = sys.argv[1]
test_file = sys.argv[2]
model_file= sys.argv[3]
output_file = sys.argv[4]
cf_type = sys.argv[5]
# train_file = "train_review.json"
# test_file = "test_review.json"
# model_file = "task3_item_model.json"
# output_file = "task3_output_file11111.json"
# cf_type = "item_based"

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

def prediction_item(x):
	Use_for_caculated = []
	if x[0] not in Dictionary_find_stars:
		return (x[0], x[1], 0)
	for item in Dictionary_find_stars[x[0]]:
		pair = tuple(sorted((x[1], item)))
		if pair in model_with_sim:
			Use_for_caculated.append((model_with_sim[pair], Dictionary_find_stars[x[0]][item]))
	Use_for_caculated.sort(reverse = True)
	fraction, denominator = 0, 0
	for i in Use_for_caculated[:N_neighbors]:
		fraction = fraction + i[0]*i[1]
		denominator = denominator + i[0]
	return (x[0], x[1], fraction/denominator if denominator else 0)

def split_to_two(x):
	item = x
	res = []
	res.append((item[0], (item[1], item[2])))
	res.append((item[1], (item[0], item[2])))
	return res

def to_dic(x):
	array = list(x[1])
	dic = {}
	for i in array:
		dic[i[0]] = i[1]
	return (x[0], dic)

def prediction_user(x):
	if x[0] not in Dictionary_find_stars or x[0] not in model_with_sim:
		return (x[0], x[1], 0)
	Avg_rate = 0
	for i in Dictionary_find_stars[x[0]]:
		Avg_rate = Avg_rate + Dictionary_find_stars[x[0]][i]
	Avg_rate = Avg_rate/len(Dictionary_find_stars[x[0]])
	fraction, denominator = 0, 0
	for i in model_with_sim[x[0]]:
		if i not in Dictionary_find_stars:
			continue
		elif x[1] in Dictionary_find_stars[i].keys():
			r_point = Dictionary_find_stars[i][x[1]]
			r_avg = 0
			for j in Dictionary_find_stars[i]:
				r_avg = r_avg + Dictionary_find_stars[i][j]
			r_avg = (r_avg-r_point)/(len(Dictionary_find_stars[i])-1)
			denominator = denominator + model_with_sim[x[0]][i]
			fraction = fraction + (r_point-r_avg)*model_with_sim[x[0]][i]
	return (x[0], x[1], Avg_rate + fraction/denominator if denominator else 0)


sc = SparkContext().getOrCreate()
RDD = sc.textFile(test_file).map(json.loads).map(lambda x : (x['user_id'], x['business_id'])).persist()
All_id = set(RDD.map(lambda x: x[0]).collect())
Dictionary_find_stars = sc.textFile(train_file).map(json.loads).map(lambda x: (x['user_id'], (x['business_id'], float(x["stars"])))).filter(lambda x: x[0] in All_id).groupByKey().mapValues(average_rate_from_same_user).collectAsMap()

if cf_type == "item_based":
	model_with_sim = sc.textFile(model_file).map(json.loads).map(lambda x: (tuple(sorted((x['b1'], x['b2']))), x['sim'])).collectAsMap()
	Result = RDD.map(prediction_item).filter(lambda x: x[2] > 0).collect()
else:
	model_with_sim = sc.textFile(model_file).map(json.loads).map(lambda x: (x['u1'], x['u2'], x['sim'])).flatMap(lambda x: split_to_two(x)).groupByKey().map(to_dic).collectAsMap()
	Result = RDD.map(prediction_user).filter(lambda x: x[2] > 0).collect()

with open(output_file, "w") as output_file1:
	for user_id, business_id, predict_stars in Result:
		output_file1.write(json.dumps({"user_id": user_id, "business_id": business_id, "stars": predict_stars})+"\n")
output_file1.close()

print("Length of Candidates :", len(Result))
print('Duration: ', time.time() - start_time)


