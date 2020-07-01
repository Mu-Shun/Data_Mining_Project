from pyspark import SparkContext, SQLContext
from pyspark import SparkConf
from graphframes import GraphFrame
import os
import sys
import json
import re
import time

start_time = time.time()

# os.environ["PYSPARK_SUBMIT_ARGS"] = "--packages graphframes:graphframes:0.6.0-spark2.3-s_2.11 pyspark-shell"

filter_threshold = int(sys.argv[1])
input_file = sys.argv[2]
community_output_file_path = sys.argv[3]
# filter_threshold = int("7")
# input_file = "ub_sample_data.csv"
# community_output_file_path = "community_output_file_path.txt"
def run_filter_threshold(x):
	res = []
	array = x[1]
	for i in range(len(array)):
		S1 = array[i][1]
		for j in range(i+1, len(array)):
			S2 = array[j][1]
			intersection = len(S1 & S2)
			if intersection >= filter_threshold:
				res.append((array[i][0], array[j][0]))
				res.append((array[j][0], array[i][0]))
	return res

sc = SparkContext().getOrCreate()
sc.setLogLevel("ERROR")

RDD = sc.textFile(input_file).map(lambda line:tuple(line.split(','))).filter(lambda x: x[0] != "user_id").groupByKey().mapValues(set)\
.map(lambda x: (1, (x[0], x[1]))).groupByKey().mapValues(list).flatMap(run_filter_threshold)
User_id = RDD.map(lambda x: x[0]).distinct().map(lambda x: [x]).collect()

sqlContext = SQLContext(sc)
vertices = sqlContext.createDataFrame(User_id, ['id'])
edges = sqlContext.createDataFrame(RDD.collect(), ["src", "dst"])
graph = GraphFrame(vertices, edges)

result = graph.labelPropagation(maxIter=5).select('id', 'label').rdd.map(lambda x: (x[1],x[0])).groupByKey().map(lambda x: sorted(list(x[1]))).sortBy(lambda x : (len(x),x[0])).collect()

with open(community_output_file_path, "w") as output_file1:
	for item in result:
		output_file1.write("'"+"', '".join(item)+"'"+"\n")
output_file1.close()

print('Duration: ', time.time() - start_time)
