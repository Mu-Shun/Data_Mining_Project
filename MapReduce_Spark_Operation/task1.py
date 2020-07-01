from pyspark import SparkContext
from pyspark import SparkConf
from operator import add
import sys
import json
import re

input_file = sys.argv[1]
output_file= sys.argv[2]
stopwords= sys.argv[3]
given_year = sys.argv[4]
top_m = int(sys.argv[5])
top_n = int(sys.argv[6])
# input_file = "review_test.json"
# output_file = "output1.json"
# stopwords = "stopwords"
# given_year = "2017"x`
# top_m = int("10")
# top_n = int("15")

dic = {}
sc = SparkContext().getOrCreate()
RDD = sc.textFile(input_file)
RDD = RDD.map(lambda line:line.split('\n')).map(lambda line:json.loads(line[0]))

# example = {"review_id": "-I5umRTkhw15RqpKMl_o1Q", "user_id": "-mA3-1mN4JIEkqOtdbNXCQ", "business_id": "mRUVMJkUGxrByzMQ2MuOpA", 
# "stars": 1.0, "text": "Walked in around 4 on a Friday afternoon, we sat at a table just off the bar and walked out after 5 min or so. \
# Don't even think they realized we walked in. However everyone at the bar noticed we walked in!!! Service was non existent at best. \
# Not a good way for a new business to start out. Oh well, the location they are at has been about 5 different things over the past several years, \
# so they will just be added to the list. SMDH!!!", "date": "2017-12-15 23:27:08"}
# print(RDD.take(1))

# A. The total number of reviews (0.5pts)
total_reviews = RDD.count()
dic["A"] = total_reviews

# B. The number of reviews in a given year, y (1pts)
reviews_of_given_year = RDD.filter(lambda x: x['date'][0:4] == given_year ).count()
dic["B"] = reviews_of_given_year

# C. The number of distinct users who have written the reviews (1pts)
distinct_users_written_reviews = RDD.map(lambda x: x['user_id']).distinct().count()
dic["C"] = distinct_users_written_reviews

# # D. Top m users who have the largest number of reviews and its count (1pts)
Top_m_users_and_count = RDD.map(lambda x: (x['user_id'],1)).reduceByKey(add).sortBy(lambda x: x[1], ascending=False).take(top_m)
dic["D"] = Top_m_users_and_count

# E. Top n frequent words in the review text. The words should be in lower cases. The following punctuations
# “(”, “[”, “,”, “.”, “!”, “?”, “:”, “;”, “]”, “)” and the given stopwords are excluded (1pts)
stopwords_set = set()
file = open(stopwords)
for i in file:
	stopwords_set.add(i.strip('\n').lower())

Top_n_frequent_words = RDD.flatMap(lambda x: re.split("[ \\(\\[,.!?:;\\]\\)]", x['text'])).map(lambda x: (x.strip().lower(),1)).\
filter(lambda x: x[0] not in stopwords_set and x[0]).\
reduceByKey(add).map(lambda x: (x[1],x[0])).sortByKey(False).map(lambda x: x[1]).take(top_n)
dic["E"] = Top_n_frequent_words

# print(dic)

with open(output_file, "w") as output_file1:
    json.dump(dic, output_file1)