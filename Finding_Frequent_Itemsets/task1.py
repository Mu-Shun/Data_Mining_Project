from pyspark import SparkContext
from pyspark import SparkConf
from operator import add
import sys
import json
import re
import time

start_time = time.time()

case= sys.argv[1]
support = int(sys.argv[2])
input_file = sys.argv[3]
output_file= sys.argv[4]
# case = "1"
# support = int("4")
# input_file = "small_make.csv"
# output_file = "output1"

def A_priori(basket, support):
	basket_temp = list(basket)
	single_item = {}
	candidates = []
	for item in basket_temp:
		for i in item[1]:
			if i not in single_item:
				single_item[i] = 1
			else:
				single_item[i] = single_item[i] + 1
	for i in single_item:
		if single_item[i] >= support:
			candidates.append(i)
	# candidates.sort()
	find_next_level_candidates = []
	for i in candidates:
		find_next_level_candidates.append(tuple([i]))
	count = 2
	while find_next_level_candidates:
		next_level_candidates = loop_the_candidates_to_next_level(find_next_level_candidates, count)
		frequent_items_basket = find_frequent_items(basket_temp, next_level_candidates, support)
		count = count + 1
		candidates = candidates + frequent_items_basket
		find_next_level_candidates = frequent_items_basket
	yield candidates

def loop_the_candidates_to_next_level(x, count):
	result = set()
	for i in range(len(x)):
		for j in range(i+1, len(x)):
			candidates_item = list(set(x[i]).union(set(x[j])))
			candidates_item.sort()
			if len(candidates_item) == count and tuple(candidates_item) not in result:
				result.add(tuple(candidates_item))
	result = list(result)
	# result.sort()
	return result

def find_frequent_items(basket, candidates, support):
	dic_of_count = {}
	frequent_items = []
	for item in basket:
		for i in candidates:
			if set(i).issubset(item[1]):
				if i not in dic_of_count:
					dic_of_count[i] = 1
				else:
					dic_of_count[i] = dic_of_count[i] + 1
	for i in dic_of_count:
		if dic_of_count[i] >= support:
			frequent_items.append(i)
	return frequent_items

def count_of_candidate(basket, candidates):
	dic_of_count = {}
	frequent_items = []
	for item in basket:
		for i in candidates:
			if type(i) == str:
				if i not in dic_of_count and i in item[1]:
					dic_of_count[i] = 1
				elif i in dic_of_count and i in item[1]:
					dic_of_count[i] = dic_of_count[i] + 1
			elif set(i).issubset(item[1]):
				if i not in dic_of_count:
					dic_of_count[i] = 1
				else:
					dic_of_count[i] = dic_of_count[i] + 1
	for i in dic_of_count: 
		# if dic_of_count[i] >= support:
		frequent_items.append((i, dic_of_count[i]))
	return frequent_items


sc = SparkContext().getOrCreate()
RDD = sc.textFile(input_file).map(lambda line:tuple(line.split(','))).distinct()
if case == "1":
	RDD = RDD.map(lambda x: (x[0],[x[1]])).filter(lambda x: x[0] != "user_id")
else:
	RDD = RDD.map(lambda x: (x[1],[x[0]])).filter(lambda x: x[0] != "business_id") 

RDD = RDD.reduceByKey(lambda a, b: a+b).map(lambda x: (x[0], set(x[1])))
# print(RDD.collect() , end='\n')
num_of_partition = RDD.getNumPartitions()

count = 1
RDD_phase_1 = RDD.mapPartitions(lambda x: (A_priori(x, support/num_of_partition))).map(lambda x: (1,x)).reduceByKey(lambda x,y: list(set(x+y)))
Candidates = RDD_phase_1.collect()[0][1]

RDD_phase_2 = RDD.mapPartitions(lambda basket: count_of_candidate(basket, Candidates)).reduceByKey(lambda x,y: x+y).filter(lambda x: x[1] >= support).map(lambda x: x[0])
Frequent_Itemsets = RDD_phase_2.collect()

# print(Candidates)
# print()
# print(Frequent_Itemsets)
# print()

candidates_dic = {1:[]}
frequent_items_dic = {1:[]}

for i in Candidates:
	if type(i) == str:
		candidates_dic[1].append(i)
		continue
	elif len(i) not in candidates_dic:
		candidates_dic[len(i)] = []
	candidates_dic[len(i)].append(i)

candidates_combination_number = 0
for i in candidates_dic:
	candidates_combination_number = max(i, candidates_combination_number)
	candidates_dic[i].sort()

for i in Frequent_Itemsets:
	if type(i) == str:
		frequent_items_dic[1].append(i)
		continue
	elif len(i) not in frequent_items_dic:
		frequent_items_dic[len(i)] = []
	frequent_items_dic[len(i)].append(i)

frequent_combination_number = 0
for i in frequent_items_dic:
	frequent_combination_number = max(i, frequent_combination_number)
	frequent_items_dic[i].sort()

with open(output_file, "w") as output_file1:
	output_file1.write("Candidates:\n")
	for i in range(1, candidates_combination_number+1):
		if i != 1:
			string = ""
			for j in candidates_dic[i]:
				if not string:
					string = str(j)
				else:
					string = string + "," + str(j)
			output_file1.write(string+'\n\n')
		else:
			string = ""
			for j in range(len(candidates_dic[i])):
				if not string:
					string = "('" + str(candidates_dic[i][j]) + "')"
				else:
					string = string + ",('" + str(candidates_dic[i][j]) + "')"
			output_file1.write(string+'\n\n')

	output_file1.write("Frequent Itemsets:\n")
	for i in range(1, frequent_combination_number+1):
		if i != 1:
			string = ""
			for j in frequent_items_dic[i]:
				if not string:
					string = str(j)
				else:
					string = string + "," + str(j)
			output_file1.write(string+'\n\n')
		else:
			string = ""
			for j in range(len(frequent_items_dic[i])):
				if not string:
					string = "('" + str(frequent_items_dic[i][j]) + "')"
				else:
					string = string + ",('" + str(frequent_items_dic[i][j]) + "')"
			output_file1.write(string+'\n\n')

output_file1.close()
end_time = time.time()
print('Duration: ', end_time - start_time)
