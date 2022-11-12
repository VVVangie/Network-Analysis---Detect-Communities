#/opt/spark/spark-3.1.2-bin-hadoop3.2/bin/spark-submit --executor-memory 4G --driver-memory 4G task2.py

import pyspark, pyspark.sql, graphframes
from graphframes import *
import sys
import time
import csv
import os
import copy
from collections import deque

#os.environ["PYSPARK_SUBMIT_ARGS"] = "--packages graphframes:graphframes:0.8.2-spark3.1-s_2.12 pyspark-shell"

filter_threshold = int(sys.argv[1])
input_file_path = sys.argv[2]
betweenness_output_file_path = sys.argv[3]
community_output_file_path = sys.argv[4]
#filter_threshold = 7
#input_file_path = '../resource/asnlib/publicdata/ub_sample_data.csv'
#betweenness_output_file_path = 'task2_betweeness.py'
#community_output_file_path = 'task2_community.py'


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
        pairs.append((sorted([users[i],users[j]])))
    return pairs

user_pairs = sc.parallelize([i for i in range(user_num)]).flatMap(lambda x: generate_pair(x))


def edge_exist(user1,user2):
    user1_reviewed_business = user_business_dict[user1]
    user2_reviewed_business = user_business_dict[user2]
    if len(user1_reviewed_business & user2_reviewed_business) >= filter_threshold:
        return True
    return False

networkRDD = user_pairs.filter(lambda x: edge_exist(x[0],x[1]))
nodesRDD = networkRDD.flatMap(lambda x: x).distinct()
edgeRDD = networkRDD.flatMap(lambda x: [x, (x[1], x[0])])

neighbor_graphRDD = edgeRDD.groupByKey().mapValues(set)
neighbor_graph = neighbor_graphRDD.collectAsMap()



def get_everytime_edge_credits(root_node, neighbor_graph_map):
    
    #Creat BFS tree for each root node:
    bfs_tree_level_map = {}
    level = 0
    current_level_nodes = set()
    current_level_nodes.add(root_node)
    nodes_already_in_tree = [root_node]
    while len(current_level_nodes) > 0:
        bfs_tree_level_map[level] = current_level_nodes
        level += 1
        next_level_nodes = set()
        for current_node in current_level_nodes:
            next_level_candidates = neighbor_graph_map[current_node]
            for candidate in next_level_candidates:
                if candidate not in nodes_already_in_tree:
                    next_level_nodes.add(candidate)
                    nodes_already_in_tree.append(candidate)
        current_level_nodes = next_level_nodes
    #Until now: get bfs_tree_level_map {0:{a}, 1:{b,c}, 2:{d,e,.}...}    
        
    #Get node weights for each node in this tree:
    node_weight = {}
    level_sum = len(bfs_tree_level_map)
    node_weight[root_node] = 1
    for i in range(1, level_sum):
        current_nodes = bfs_tree_level_map[i]
        pre_level_nodes = bfs_tree_level_map[i-1]
        for node in current_nodes:
            node_neighbors = neighbor_graph_map[node]
            parents_for_this_node = set(node_neighbors) & set(pre_level_nodes)
            node_label = 0
            for p in parents_for_this_node:
                node_label += node_weight[p]
            node_weight[node] = node_label
    #Until now: get node_weight {node:node_weight,...}
    
    #get edge credits for each edge in this tree:
    edge_credit = {}
    node_credit ={}
    last_level_nodes = bfs_tree_level_map[level_sum-1]
    for last_node in last_level_nodes:
        node_credit[last_node] = 1
    for index in range(level_sum-2,-1,-1):
        now_level_nodes = bfs_tree_level_map[index]
        next_level_nodes = bfs_tree_level_map[index+1]
        for n in now_level_nodes:
            node_neigh = neighbor_graph_map[n]
            children_for_this_node = set(node_neigh) & set(next_level_nodes)
            if i > 0:
                node_credits_sum = 1
            else:
                node_credits_sum = 0 
            for c in children_for_this_node:
                credits = ( node_credit[c] / node_weight[c] ) * node_weight[n]
                edge_tuple = tuple(sorted([c,n]))
                edge_credit[edge_tuple] = credits
                node_credits_sum += credits
            node_credit[n] = node_credits_sum            
    #Until now: get edge_credit{edge(a,b):credits...}
    return edge_credit.items()

betweenessRDD = neighbor_graphRDD.map(lambda x: x[0]).flatMap(lambda x: get_everytime_edge_credits(x,neighbor_graph)).reduceByKey(lambda x, y: x+y).mapValues(lambda y: round(y/2, 5)).sortBy(lambda x: (-x[1], x[0]))
betweeness_output = betweenessRDD.collect()

with open(betweenness_output_file_path, 'w')as f:
    for i in betweeness_output:
        f.write(str(i[0])+"," + str(i[1]))
        f.write('\n')                
        

        
ranked_edge_betweeness = deque(betweeness_output)
node_degree_search = neighbor_graphRDD.map(lambda x: (x[0], len(x[1]))).collectAsMap()
community_network = copy.deepcopy(neighbor_graph)
networks = networkRDD.collect()
connection_sum = len(networks)
value_2m = 2 * connection_sum# The “m” in the formula represents the edge number of the original graph.

global_highest_modularity = -1
best_community_set = []

while len(ranked_edge_betweeness) >0:
    #Remove the edge with biggest betweeness:
    biggest_betweeness_pair = ranked_edge_betweeness.popleft()[0]
    community_network[biggest_betweeness_pair[0]].remove(biggest_betweeness_pair[1])
    community_network[biggest_betweeness_pair[1]].remove(biggest_betweeness_pair[0])
    # In each remove step, “m”, “A”, “k_i” and “k_j” should not be changed.
    
    #Get the community condition:
    node_already_recorded = []
    community_cluters = []
    for each_node in community_network.keys():
        nodes_in_queue = deque([each_node])
        cluter_members = []
        while len(nodes_in_queue) > 0:
            one_node = nodes_in_queue.popleft()
            if one_node not in node_already_recorded:
                node_already_recorded.append(one_node)
                cluter_members.append(one_node)
                one_node_neighbors = community_network[one_node]
                for each_neighbor in one_node_neighbors:
                    nodes_in_queue.append(each_neighbor)
        if len(cluter_members) > 0:
            community_cluters.append(sorted(cluter_members))
    
    #calculate the modularity for the community separation:
    Q_score_sum = 0
    for one_cluster in community_cluters:
        if len(one_cluster) > 1:
            for index_i in range(0, len(one_cluster)):
                for index_j in range(index_i+1, len(one_cluster)):
                    node_i = one_cluster[index_i]
                    node_j = one_cluster[index_j]
                    model_ij_pair = (sorted([node_i, node_j]))
                    if model_ij_pair in networks:
                        A_ij = 1
                    else:
                        A_ij = 0
                    part_score = A_ij - ( (node_degree_search[node_i] * node_degree_search[node_j]) / value_2m )
                    Q_score_sum += part_score
    Q_score = Q_score_sum / value_2m
    
    if Q_score > global_highest_modularity:
        global_highest_modularity = Q_score
        best_community_set = community_cluters
    updated_neighbor_graphRDD = sc.parallelize(community_network.items())
    updated_betweeness = updated_neighbor_graphRDD.map(lambda x: x[0]).flatMap(lambda x: get_everytime_edge_credits(x,community_network)).reduceByKey(lambda x, y: x+y).mapValues(lambda y: round(y/2, 5)).sortBy(lambda x: (-x[1], x[0])).collect()
    ranked_edge_betweeness = deque(updated_betweeness)
    
with open(community_output_file_path, 'w')as f:
    for i in sorted(best_community_set, key=lambda x:(len(x),x)):
        f.write(str(i)[1:-1])#.replace("'","").replace(" ",""))
        f.write('\n')    
                                       
                    
                
            

        




time_end = time.time()
print("Duration:{0:.2f}".format(time_end - time_start))