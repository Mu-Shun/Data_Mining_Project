from pyspark import SparkContext, SQLContext
from pyspark import SparkConf
from collections import deque
from copy import deepcopy
import sys
import time

start_time = time.time()

filter_threshold = int(sys.argv[1])
input_file = sys.argv[2]
betweenness_output_file_path = sys.argv[3]
community_output_file_path = sys.argv[4]
# filter_threshold = int("7")
# input_file = "ub_sample_data.csv"
# betweenness_output_file_path = "b1.txt"
# community_output_file_path = "c1.txt"

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

def BFS(vertices):
    Betweenness = {}
    for vertice in vertices:
        Used = set([vertice])
        stack_vertice = []
        queue = deque([vertice])
        count_visit_of_each_vertice = {vertice:1}
        temp_dic_vertice_to_edge = {vertice:["No_Edge"]}

        while queue:
            len_queue = len(queue)
            temp_vertice = set()
            while len_queue:
                parent = queue.popleft()
                for child in dic[parent]:
                    if child not in Used:
                        if child not in count_visit_of_each_vertice:
                            count_visit_of_each_vertice[child] = count_visit_of_each_vertice[parent]
                        else:
                            count_visit_of_each_vertice[child] = count_visit_of_each_vertice[child] + count_visit_of_each_vertice[parent]
                        if child not in temp_dic_vertice_to_edge:
                            temp_dic_vertice_to_edge[child] = set()
                        temp_dic_vertice_to_edge[child].add(tuple(sorted((parent, child))))
                        temp_vertice.add(child)
                len_queue = len_queue -1

            if temp_vertice:
                queue = queue + deque(temp_vertice - Used)
                stack_vertice = stack_vertice + [list(temp_vertice - Used)]
                Used = temp_vertice | Used

        dic_for_temp_credit = {}
        while stack_vertice:
            leaf = stack_vertice.pop()
            for child in leaf:
                short_path_count = 0
                for edge in temp_dic_vertice_to_edge[child]:
                    parent = edge[0] if edge[0] != child else edge[1]
                    if child in dic_for_temp_credit:
                        credit = dic_for_temp_credit[child] + 1
                    else:
                        credit = 1
                    each_credit = credit/count_visit_of_each_vertice[child]*count_visit_of_each_vertice[parent]
                    if parent in dic_for_temp_credit:
                        dic_for_temp_credit[parent] = dic_for_temp_credit[parent] + each_credit
                    else:
                        dic_for_temp_credit[parent] = each_credit
                    if edge in Betweenness:
                        Betweenness[edge] = Betweenness[edge] + each_credit
                    else:
                        Betweenness[edge] = each_credit

    res = []
    for i in Betweenness:
        res.append((i, Betweenness[i]))
    res.sort(key = lambda x: (-x[1], x[0]))
    return res

sc = SparkContext().getOrCreate()
sc.setLogLevel("ERROR")

RDD = sc.textFile(input_file).map(lambda line:tuple(line.split(','))).filter(lambda x: x[0] != "user_id").groupByKey().mapValues(set)\
.map(lambda x: (1, (x[0], x[1]))).groupByKey().mapValues(list).flatMap(run_filter_threshold)
dic = RDD.groupByKey().mapValues(set).collectAsMap()
all_vertices = RDD.map(lambda x: x[0]).distinct().map(lambda x: (1,x)).groupByKey().mapValues(list).persist()
betweenness_res = all_vertices.flatMap(lambda x: BFS(x[1])).persist()
output_of_betweenness = betweenness_res.collect()

with open(betweenness_output_file_path, "w") as output_file1:
	for item in output_of_betweenness:
		output_file1.write("('"+"', '".join(item[0])+"'), " + str(item[1]/2) + "\n")
output_file1.close()

# PART2
def build_community(vertices):
    community = []
    used = set()
    for vert in vertices:
        if vert not in used:
            temp_comm = [vert]
            used.add(vert)
            stack = list(dic[vert])
            while stack:
                k = stack.pop()
                if k not in used:
                    temp_comm.append(k)
                    used.add(k)
                    stack = list(set(stack) | set(dic[k]))
            community.append(sorted(temp_comm))
    community.sort(key = lambda x: (len(x),x[0]))
    return community

def modularity(community, dictionary):
    mod_total = 0
    for comm in community:
        if len(comm) == 1:
            continue
        for i in range(len(comm)):
            for j in range(i, len(comm)):
                if comm[i] in dictionary[comm[j]]:
                     Aij = 1
                else:
                     Aij = 0
                ki, kj = len(dictionary[comm[i]]), len(dictionary[comm[j]])
                mod_total = mod_total + (Aij - (ki*kj)/(2*m))
    return mod_total/(2*m)

dictionary = deepcopy(dic)
m = len(output_of_betweenness)
max_modularity = -float("inf")
count = m

while count > 0:
	betweenness_res = all_vertices.flatMap(lambda x: BFS(x[1])).persist()
	To_remove = betweenness_res.take(1)
	dic[To_remove[0][0][0]].remove(To_remove[0][0][1])
	dic[To_remove[0][0][1]].remove(To_remove[0][0][0])
	comm = all_vertices.flatMap(lambda x: build_community(x[1])).persist()
	cur_mod = comm.map(lambda x: (1,x)).groupByKey().mapValues(list).map(lambda x: modularity(x[1], dictionary)).collect()[0]
	if cur_mod > max_modularity:
		max_modularity = cur_mod
		best_comm = comm
		# print(count, cur_mod)
	count = count - 1

res = best_comm.collect()
with open(community_output_file_path, "w") as output_file2:
	for item in res:
		output_file2.write("'"+"', '".join(item)+"'"+"\n")
output_file2.close()

print('Duration: ', time.time() - start_time)