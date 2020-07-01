import random
import sys
import json
import re
import time
import os
import math
from copy import deepcopy

start_time = time.time()

input_path = sys.argv[1]
n_cluster = int(sys.argv[2])
output_file_cluster = sys.argv[3]
output_file_intermediate = sys.argv[4]
# input_path = "C:/Users/user/Desktop/INF553/HW/HW6/test1/"
# n_cluster = 10
# output_file_cluster = "cluster_result_test1.json"
# output_file_intermediate = "output_file_intermediate_test1.csv"

all_file = sorted(os.listdir(input_path))
DS_summarized, CS_summarized, CS_cluster, RS_dic = {}, {}, {}, {}
RS_set = set()
cluster_dic_DS, cluster_dic_CS, result_dic, store_of_CS = {}, {}, {}, {}
DS_len, CS_len = 0, 0

def calculated_std(vector):
    std = []
    for i in range(len(vector[0])):
        temp = []
        for j in range(len(vector)):
            temp.append(vector[j][i])
        std.append(stdev(temp))
    return std

def calculated_Euclidean_distance(data1, data2):
    distance = 0
    for i in range(len(data1)):
        distance += (data1[i] - data2[i])**2
    return math.sqrt(distance)

def calculated_Mahalanob_distance(point, Info, thresold, cluster_dic):
    min_dis = float("inf")
    nearest_centroid = ""
    for centroid in Info:
        N = Info[centroid][0]
        SUM_vector = Info[centroid][1]
        SUMSQ_vector = Info[centroid][2]
        center = []
        std = []
        M_distance = 0
        for i in range(len(SUM_vector)):
            if N:
                center.append(SUM_vector[i]/N)
                std.append(((SUMSQ_vector[i]/N) - (SUM_vector[i]/N)**2)**(1/2))
            else:
                center.append(0)
                std.append(0)
            if std[i]:
                M_distance = M_distance + ((point[i] - center[i])/std[i])**2
        if M_distance**(1/2) < thresold and M_distance**(1/2) < min_dis:
            min_dis = M_distance**(1/2)
            nearest_centroid = centroid
    if nearest_centroid:
        return cluster_dic[nearest_centroid], nearest_centroid
    return -1, nearest_centroid

def K_Means(centroid_set, data, num_of_each_cluster):
    cur_dic_for_cluster = {}
    for i in centroid_set:
        cur_dic_for_cluster[tuple(i)] = []
    for index in data:
        min_distance = float("inf")
        which_cluster = 0
        for centroid in centroid_set:
            dis = calculated_Euclidean_distance(data[index], centroid)
            if dis < min_distance:
                min_distance = dis
                which_cluster = tuple(centroid)
        cur_dic_for_cluster[which_cluster].append(index)
    new_cluster_centroid = []
    new_cluster_centroid_length = []
    for i in cur_dic_for_cluster:
        new_cluster_centroid_length.append(len(cur_dic_for_cluster[i]))
    for cluster_centroid in cur_dic_for_cluster:
        sum_of_every_dimension = [0] * d
        for index in cur_dic_for_cluster[cluster_centroid]:
            for dimension in range(len(data[index])):
                sum_of_every_dimension[dimension] += data[index][dimension]
        new_cluster_centroid.append(sum_of_every_dimension)
    ratio = []
    for item_index in range(len(new_cluster_centroid)):
        if num_of_each_cluster[item_index]:
            ratio.append(new_cluster_centroid_length[item_index]/num_of_each_cluster[item_index])
        else:
            ratio.append(1)
        for index in range(d):
            if new_cluster_centroid_length[item_index]:
                new_cluster_centroid[item_index][index] = new_cluster_centroid[item_index][index]/new_cluster_centroid_length[item_index]
    return new_cluster_centroid, new_cluster_centroid_length, ratio, cur_dic_for_cluster

def RUN_K_MEANS(times, centroid, sample_data_dic, num_of_each_cluster):
    count = 0
    new_cluster_centroid = centroid[:]
    while count < times:
        centroid = new_cluster_centroid[:]
        new_cluster_centroid, num_of_each_cluster, ratio, dic_for_cluster = K_Means(centroid, sample_data_dic, num_of_each_cluster)
        for i in ratio:
            if abs(i-1) > 0.03:
                break
        else:
            count = times
        print("Cluster round:", count)
        print("max and min of the ratio increase: ", max(ratio), min(ratio))
        count = count + 1
    return centroid, num_of_each_cluster, ratio, dic_for_cluster

def DS_statistics(cluster, data):
    SUM = [0]* d
    SUMSQ = [0] * d
    N = len(cluster)
    for index in cluster:
        for i in range(len(data[index])):
            SUM[i] += data[index][i]
            SUMSQ[i] += data[index][i]**2
    return N, SUM, SUMSQ

def update_statistics(data, DS_summarized_with_centroid):
    N = DS_summarized_with_centroid[0] + 1
    SUM = DS_summarized_with_centroid[1]
    SUMSQ = DS_summarized_with_centroid[2]
    for i in range(len(data)):
        SUM[i] += data[i]
        SUMSQ[i] += (data[i]**2)
    return [N, SUM, SUMSQ]

def calculated_Mahalanob_distance_by_point(point, point2, thresold):
    nearest_centroid = ""
    N = point2[0]
    SUM_vector = point2[1]
    SUMSQ_vector = point2[2]
    center = []
    std = []
    M_distance = 0
    for i in range(len(SUM_vector)):
        if N:
            center.append(SUM_vector[i]/N)
            std.append(((SUMSQ_vector[i]/N) - (SUM_vector[i]/N)**2)**(1/2))
        else:
            center.append(0)
            std.append(0)
        if std[i]:
            M_distance = M_distance + ((point[i] - center[i])/std[i])**2
    if M_distance**(1/2) < thresold:
        return True
    return False


for file_num in range(len(all_file)):
    open_file = open(input_path + all_file[file_num], 'r')
    data = {}
    for line in open_file:
        point = line.strip().split(',')
        for each in range(len(point)):
            point[each] = float(point[each])
        data[int(point[0])] = tuple(point[1:])
    print("file num:", file_num, ", data length:", len(data))
    if file_num == 0:
        d = len(data[0])
        # RUN K MEANS to find outliers
        more_centroid = []
        pick_n_cluster_TIMES_5 = random.sample(range(0, len(data)), n_cluster*5)
        for pick in pick_n_cluster_TIMES_5:
            more_centroid.append(data[pick])
        more_centroid, num_of_each_cluster, ratio, dic_for_cluster = RUN_K_MEANS(5, more_centroid, data, [1]*len(more_centroid))
        print("nums of each cluster:", num_of_each_cluster)

        centroid = set()
        for i in dic_for_cluster:
            if len(dic_for_cluster[i]) < len(data)/500:
                for j in dic_for_cluster[i]:
                    RS_dic[j] = data[j]
                    data.pop(j)
            elif len(centroid) < 1:
                centroid.add(i)

        print("How many outliers removing: ", len(RS_dic))

        sample_data = random.sample(list(data.keys()), len(data)//n_cluster)
        sample_data_dic = {}
        for i in sample_data:
            sample_data_dic[i] = data[i]

        while len(centroid) < n_cluster:
            max_distance = 0
            farest_point = 0
            for index in sample_data_dic:
                sum_distance = []
                for every_centroid in centroid:
                    temp = 0
                    for dimension in range(len(data[index])):
                        temp = temp + (data[index][dimension] - every_centroid[dimension])**2
                        sum_distance.append(temp**(1/2))
                distance = min(sum_distance)
                if data[index] not in centroid and distance > max_distance:
                    max_distance = distance
                    farest_point = data[index]
            centroid.add(farest_point)
        centroid = list(centroid)
        centroid, num_of_each_cluster, ratio, dic_for_cluster = RUN_K_MEANS(15, centroid, sample_data_dic, [1]*len(centroid))
        print("nums of each cluster:", num_of_each_cluster)


        for cluster in dic_for_cluster:
            N, SUM, SUMSQ = DS_statistics(dic_for_cluster[cluster], sample_data_dic)
            DS_summarized[cluster] = [N, SUM, SUMSQ]
            for index in dic_for_cluster[cluster]:
                data.pop(index)

        gp_count = 0
        for i in DS_summarized:
            cluster_dic_DS[i] = gp_count
            gp_count = gp_count + 1 

        new_centroid = []
        pick_n_cluster_TIMES_5 = random.sample(list(data.keys()), n_cluster*5)
        for pick in pick_n_cluster_TIMES_5:
            new_centroid.append(data[pick])

        # Run K-Means for the rest data
        new_centroid, num_of_each_cluster, ratio, dic_for_cluster = RUN_K_MEANS(10, new_centroid, data, [1]*len(new_centroid))
        print("nums of each cluster:", num_of_each_cluster)

        for cluster in dic_for_cluster:
            N, SUM, SUMSQ = DS_statistics(dic_for_cluster[cluster], data)
            CS_summarized[cluster] = [N, SUM, SUMSQ]
            for index in dic_for_cluster[cluster]:
                if cluster not in CS_cluster:
                    CS_cluster[cluster] = []
                CS_cluster[cluster].append(index)
                store_of_CS[index] = data[index]
                CS_len = CS_len + 1
                data.pop(index)

        gp_count = 0
        for i in CS_summarized:
            cluster_dic_CS[i] = gp_count
            gp_count = gp_count + 1 

        for i in sample_data_dic:
            group, nearest_centroid = calculated_Mahalanob_distance(sample_data_dic[i], DS_summarized, 2*(d**(1/2)), cluster_dic_DS)
            if group == -1:
                group, nearest_centroid = calculated_Mahalanob_distance(sample_data_dic[i], CS_summarized, 2*(d**(1/2)), cluster_dic_CS)
                if group == -1:
                    RS_dic[i] = sample_data_dic[i]
                else:
                    # print("Happened!")
                    CS_cluster[nearest_centroid].append(i)
                    store_of_CS[i] = sample_data_dic[i]
                    CS_summarized[nearest_centroid] = update_statistics(sample_data_dic[i], CS_summarized[nearest_centroid])
            else:
                result_dic[i] = group
                DS_len = DS_len + 1
        
        print(str(file_num+1), str(n_cluster), str(DS_len), str(n_cluster*5), str(CS_len), str(len(RS_dic)))

        with open(output_file_intermediate, "w", encoding="utf-8") as output_file2:
            output_file2.write("round_id,nof_cluster_discard,nof_point_discard,nof_cluster_compression,nof_point_compression,nof_point_retained"+"\n")
            output_file2.write(','.join([str(file_num+1), str(n_cluster), str(DS_len), str(n_cluster*5), str(CS_len), str(len(RS_dic))])+"\n")
        output_file2.close()

    else:
        for i in data:
            group, nearest_centroid = calculated_Mahalanob_distance(data[i], DS_summarized, 2*(d**(1/2)), cluster_dic_DS)
            if group == -1:
                group, nearest_centroid = calculated_Mahalanob_distance(data[i], CS_summarized, 2*(d**(1/2)), cluster_dic_CS)
                if group == -1:
                    RS_dic[i] = data[i]
                else:
                    # print("Happened!")
                    CS_len = CS_len + 1
                    CS_cluster[nearest_centroid].append(i)
                    store_of_CS[i] = data[i]
                    CS_summarized[nearest_centroid] = update_statistics(data[i], CS_summarized[nearest_centroid])
            else:
                result_dic[i] = group
                DS_len = DS_len + 1
                DS_summarized[nearest_centroid] = update_statistics(data[i], DS_summarized[nearest_centroid])
        #MERGE
        set_used = set()
        cluster_items = list(CS_summarized.keys())
        CS_summarized_copy = deepcopy(CS_summarized)
        for centroid_index1 in range(len(cluster_items)):
            if cluster_items[centroid_index1] in set_used:
                continue
            N = CS_summarized[cluster_items[centroid_index1]][0]
            SUM_vector = CS_summarized[cluster_items[centroid_index1]][1]
            SUMSQ_vector = CS_summarized[cluster_items[centroid_index1]][2]
            center = []
            if N:
                for i in range(len(SUM_vector)):
                    center.append(SUM_vector[i]/N)
                for centroid_index2 in range(centroid_index1+1, len(cluster_items)):
                    if cluster_items[centroid_index2] not in set_used and calculated_Mahalanob_distance_by_point(center, CS_summarized[cluster_items[centroid_index2]], (1/4)*(d**(1/2))):
                        N = N + CS_summarized[cluster_items[centroid_index2]][0]
                        for i in range(len(SUM_vector)):
                            SUM_vector[i] = SUM_vector[i] + CS_summarized[cluster_items[centroid_index2]][1][i]
                            SUMSQ_vector[i] = SUMSQ_vector[i] + CS_summarized[cluster_items[centroid_index2]][2][i]
                        CS_summarized_copy[cluster_items[centroid_index1]] = [N, SUM_vector, SUMSQ_vector]
                        CS_summarized_copy.pop(cluster_items[centroid_index2])
                        CS_cluster[cluster_items[centroid_index1]] = CS_cluster[cluster_items[centroid_index1]] + CS_cluster[cluster_items[centroid_index2]]
                        CS_cluster.pop(cluster_items[centroid_index2])
                        set_used.add(cluster_items[centroid_index2])
            set_used.add(cluster_items[centroid_index1])

        CS_summarized = deepcopy(CS_summarized_copy)

        print(str(file_num+1), str(n_cluster), str(DS_len), len(CS_summarized), str(CS_len), str(len(RS_dic)))
        with open(output_file_intermediate, "a", encoding="utf-8") as output_file2:
            output_file2.write(','.join([str(file_num+1), str(n_cluster), str(DS_len), str(len(CS_summarized)), str(CS_len), str(len(RS_dic))])+"\n")
        output_file2.close()

for cluster in CS_summarized:
    CS_center = CS_summarized[cluster][1]
    for i in range(len(CS_center)):
        if CS_summarized[cluster][0]:
            CS_center[i] = CS_center[i]/CS_summarized[cluster][0]
    group, nearest_centroid = calculated_Mahalanob_distance(CS_center, DS_summarized, 2*(d**(1/2)), cluster_dic_DS)
    if group != -1:
        for index in CS_cluster[cluster]:
            result_dic[index] = group
            DS_summarized[nearest_centroid] = update_statistics(store_of_CS[index], DS_summarized[nearest_centroid])
    else:
        for index in CS_cluster[cluster]:
            RS_dic[index] = store_of_CS[index]

for index in RS_dic:
    group, nearest_centroid = calculated_Mahalanob_distance(RS_dic[index], DS_summarized, 2*(d**(1/2)), cluster_dic_DS)
    if group != -1:
        result_dic[index] = group
        DS_summarized[nearest_centroid] = update_statistics(RS_dic[index], DS_summarized[nearest_centroid])
    else:
        result_dic[index] = -1

print("Result: ", len(result_dic))

with open(output_file_cluster, "w", encoding="utf-8") as output_file1:
    json.dump(result_dic, output_file1)
output_file1.close()

print('Duration: ', time.time() - start_time)