from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from datetime import datetime
import binascii
import sys
import time
import json
import random

start_time = time.time()
hash_fcn_nums = 50

port = int(sys.argv[1])
output_file = sys.argv[2]

file = open(output_file, 'w')
with open(output_file, "w") as output_file1:
    output_file1.write("Time,Ground Truth,Estimation"+"\n")
output_file1.close()

a = [55264, 56563, 59522, 90085, 18769, 11424, 52331, 43181, 42771, 78756, 7033, 71054, 61307, 8966, 695, 91193, 3603, 10562, 8070, 30954, 86994, 1220, 29203, 62101, 92025, 44318, 66708, 83300, 95110, 84308, 94646, 79963, 68716, 39260, 80591, 62975, 98802, 11847, 45450, 11489, 66921, 74590, 15274, 34236, 177, 41707, 52318, 32960, 79616, 49610]
b = [31243, 67619, 33832, 57440, 30456, 26173, 20531, 15756, 60510, 78568, 64208, 55976, 98757, 78867, 5255, 26314, 73167, 12862, 44831, 52275, 35861, 13949, 98783, 22017, 51604, 9242, 91563, 94039, 35347, 27705, 96924, 74741, 35657, 26498, 98896, 57063, 32150, 22854, 7698, 94939, 33100, 3191, 35441, 25565, 56856, 34650, 49800, 30516, 17425, 48055]
m = [53619, 43147, 30413, 59972, 7804, 87256, 89477, 11439, 9519, 53015, 37158, 25503, 52772, 42856, 81416, 4404, 24678, 53402, 30570, 78557, 9252, 63788, 26328, 52028, 77243, 59908, 63217, 74143, 20886, 22242, 25178, 30500, 2170, 67786, 97703, 41177, 88978, 9240, 22423, 31263, 63863, 20071, 87214, 21135, 84793, 27524, 87559, 70606, 66089, 90918] 

def hash_fcn_and_some_step(array, length):
    res = []
    for j in range(hash_fcn_nums):
        temp = []
        for i in array:
            x = int(binascii.hexlify(i.encode('utf8')),16)
            temp.append("{0:b}".format(((a[j]*x + b[j]) % m[j])))
        res.append(temp[:])
    for i in range(len(res)):
        temp = []
        for item in res[i]:
            count = 0
            index = 0
            while item[len(item) - index - 1] == '0' and index < len(item):
                count = count + 1
                index = index + 1
            temp.append(count)
        res[i] = 2**max(temp)
    return res

def Flajolet_Martin(x):
    time_now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    data = x.collect()
    ground_truth = set()
    to_hash_data = []
    for item in data:
        Json_item = json.loads(item)
        if "city" in Json_item:
            ground_truth.add(Json_item["city"])
            to_hash_data.append(Json_item["city"])

    res = hash_fcn_and_some_step(to_hash_data, len(to_hash_data))
    res.sort()
    estimation = sum(res[len(res)//2-5:len(res)//2+5])/10

    print(time_now, len(ground_truth), estimation, str((estimation-len(ground_truth))/len(ground_truth)))
    with open(output_file, "a") as output_file1:
        output_file1.write(str(time_now) + "," + str(len(ground_truth)) + "," + str(estimation) + "\n")
    output_file1.close()

sc = SparkContext().getOrCreate()
sc.setLogLevel("ERROR")
ssc = StreamingContext(sc, 5)
data_stream = ssc.socketTextStream("localhost", port).window(30,10)
data_stream.foreachRDD(Flajolet_Martin)
ssc.start()
ssc.awaitTermination(timeout= 600)

print('Duration: ', time.time() - start_time)