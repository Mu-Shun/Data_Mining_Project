from pyspark import SparkContext
from pyspark import SparkConf
import sys
import json
import re
import time
import random
import math 

start_time = time.time()

train_file = sys.argv[1]
model_file= sys.argv[2]
stopwords = sys.argv[3]
# train_file = "train_review.json"
# model_file= "model_file_output1.json"
# stopwords= "stopwords"

stopwords_set = set()
file = open(stopwords)
for i in file:
	stopwords_set.add(i.strip('\n').lower())

sc = SparkContext().getOrCreate()
RDD = sc.textFile(train_file).map(json.loads)

def preprocessing_words(x):
	res = []
	text_split = re.sub('[^A-Za-z]+', ' ', x['text']).split()
	for i in text_split:
		k = i.lower()
		if k not in stopwords_set and k:
			res.append(((x["business_id"], k) ,1))
	return res

def TFIDF(x):
	words = []
	max_occurreny = 0 
	for i in x[1]:
		max_occurreny = max(max_occurreny, i[1])
	for i in x[1]:
		TFIDF_score = i[1]/max_occurreny*IDF[i[0]]
		words.append((i[0], TFIDF_score))
	words.sort(key=lambda tup:tup[1], reverse = True)
	top_200 = []
	for i in words[:200]:
		top_200.append(i[0])
	return (x[0], top_200)

def deal_with_user_profile_TFIDF(x):
	freq_dic = {}
	words = []
	user_profile = []
	for k in x[1]:
		for i in Business_profile[k]:
			if i not in freq_dic:
				freq_dic[i] = 1
			else:
				freq_dic[i] = 1 + freq_dic[i]
	for i in freq_dic:
		words.append((i, freq_dic[i]))
	words.sort(key=lambda tup:tup[1], reverse = True)
	for i in words[:400]:
		user_profile.append(i[0])
	return (x[0], user_profile)

User_review_business = RDD.map(lambda x: (x["user_id"],[x["business_id"]]))
ALL_documents_num = User_review_business.map(lambda x: x[1][0]).distinct().count()
User_review_business = User_review_business.reduceByKey(lambda x,y: x+y).persist()
RDD = RDD.flatMap(preprocessing_words).persist()

Total_words_count = RDD.count()
IDF = RDD.distinct().map(lambda x: (x[0][1], 1)).reduceByKey(lambda x,y: x+y).map(lambda x: (x[0], math.log2(ALL_documents_num/x[1]))).collectAsMap()
# rare_words = set(RDD.map(lambda x: (x[0][1], 1)).reduceByKey(lambda x,y: x+y).filter(lambda x: x[1] < ALL_documents_num * 0.000001).collect())

RDD = RDD.reduceByKey(lambda x,y: x+y).map(lambda x: (x[0][0], [(x[0][1], x[1])])).reduceByKey(lambda x,y: x+y).persist()
Business_profile = RDD.map(TFIDF).collectAsMap()
User_profile = User_review_business.map(deal_with_user_profile_TFIDF).collectAsMap()
dic = {}
dic["Business_profile"] = Business_profile
dic["User_profile"] = User_profile

with open(model_file, "w") as output_file1:
    json.dump(dic, output_file1)
output_file1.close()

print('Duration: ', time.time() - start_time)
