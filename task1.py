#/opt/spark/spark-3.1.2-bin-hadoop3.2/bin/spark-submit --executor-memory 4G --driver-memory 4G task1.py
#/opt/spark/spark-3.1.2-bin-hadoop3.2/bin/spark-submit --executor-memory 4G --driver-memory 4G --packages graphframes:graphframes:0.8.2-spark3.1-s_2.12 task1.py 
import pyspark, pyspark.sql, graphframes
from graphframes import *
import sys
import time
import csv
import os

os.environ["PYSPARK_SUBMIT_ARGS"] = "--packages graphframes:graphframes:0.8.2-spark3.1-s_2.12 pyspark-shell"

filter_threshold = int(sys.argv[1])
input_file_path = sys.argv[2]
community_output_file_path = sys.argv[3]
#filter_threshold = 2
#input_file_path = '../resource/asnlib/publicdata/ub_sample_data.csv'
#community_output_file_path = 'task1_1_ans.py'


time_start = time.time()


sc = pyspark.SparkContext()
spark = pyspark.sql.SparkSession(sc)
sc.setLogLevel("WARN")


data = sc.textFile(input_file_path).map(lambda x: x.split(','))
title = data.take(1)
dataRDD = data.filter(lambda x: x[0] != title[0][0]).map(lambda x: (x[0],x[1]))
users = dataRDD.map(lambda x: x[0]).distinct().collect()
user_num = len(users)
business_dict = dataRDD.map(lambda x: x[1]).distinct().zipWithIndex().collectAsMap()
user_business_dict = dataRDD.map(lambda x: (x[0], business_dict[x[1]])).groupByKey().mapValues(set).collectAsMap()

def generate_pair(i):
    pairs = []
    for j in range(i+1,user_num):
        pairs.append((users[i],users[j]))
    return pairs

user_pairs = sc.parallelize([i for i in range(user_num)]).flatMap(lambda x: generate_pair(x))


def edge_exist(user1,user2):
    user1_reviewed_business = user_business_dict[user1]
    user2_reviewed_business = user_business_dict[user2]
    if len(user1_reviewed_business & user2_reviewed_business) >= filter_threshold:
        return True
    return False

nodesRDD = user_pairs.filter(lambda x: edge_exist(x[0],x[1])).flatMap(lambda x: x).distinct()
#Problem reference: https://stackoverflow.com/questions/32742004/create-spark-dataframe-can-not-infer-schema-for-type
nodes = nodesRDD.map(lambda x: (x, )).toDF(["id"])
edgeRDD = user_pairs.filter(lambda x: edge_exist(x[0],x[1])).flatMap(lambda x: [x, (x[1], x[0])])
edges = edgeRDD.toDF(["src", "dst"])

g =  graphframes.GraphFrame(nodes, edges)
label_user_DF = g.labelPropagation(maxIter=5)
#Row(id='PoADjvCdEl-oHyWETaW7Ng', label=979252543495)...
output = label_user_DF.rdd.map(lambda x: (x[1],x[0])).groupByKey().mapValues(list).map(lambda x: sorted(x[1])).sortBy(lambda x: (len(x), x[0])).collect()

with open(community_output_file_path, 'w')as f:
    for i in output:
        f.write(str(i)[1:-1])#.replace("'","").replace(" ",""))
        f.write('\n')

    

time_end = time.time()
print("Duration:{0:.2f}".format(time_end - time_start))

#/opt/spark/spark-3.1.2-bin-hadoop3.2/bin/spark-submit --executor-memory 4G --driver-memory 4G --packages graphframes:graphframes:0.8.2-spark3.1-s_2.12 task1.py 1 '../resource/asnlib/publicdata/ub_sample_data.csv' output1.py                                  