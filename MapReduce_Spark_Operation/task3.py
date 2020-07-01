from pyspark import SparkContext
from pyspark import SparkConf
import sys
import json

input_file = sys.argv[1]
output_file= sys.argv[2]
partition_type= sys.argv[3]
n_partitions = int(sys.argv[4])
n = int(sys.argv[5])
# input_file = "review.json"
# output_file = "output_task3.json"
# partition_type = "customized"
# n_partitions = int("100")
# n = int("200")

dic = {}
sc = SparkContext().getOrCreate()
RDD = sc.textFile(input_file)
RDD = RDD.map(lambda line:line.split('\n')).map(lambda line: (json.loads(line[0])["business_id"],1))
if partition_type != "default":
	RDD = RDD.partitionBy(n_partitions, lambda x: hash(x[0][0:4]))
dic["n_items"] = RDD.glom().map(len).collect()
RDD = RDD.reduceByKey(lambda a, b: a + b).filter(lambda x: x[1]>n)
dic["n_partitions"] = RDD.getNumPartitions()
dic["result"] = RDD.collect()

with open(output_file, "w") as output_file1:
    json.dump(dic, output_file1)