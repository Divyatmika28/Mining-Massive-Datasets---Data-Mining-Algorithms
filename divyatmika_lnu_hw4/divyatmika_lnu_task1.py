import time
import os
from operator import add
from pyspark import SparkContext, SparkConf
import collections
from collections import defaultdict
import math
import pyspark
from pyspark.sql.functions import*
from pyspark.sql import Row
from graphframes import *
import sys
import itertools
import random
from itertools import chain, combinations
import os
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages graphframes:graphframes:0.6.0-spark2.3-s_2.11 pyspark-shell'

#import GraphFrames


input_file_name = sys.argv[1]
output_file_name= sys.argv[2]

sc =  SparkContext(appName = "inf553")
sqlContext = pyspark.SQLContext(sc)
#sc =  SparkContext(appName = "inf553")

def findEdges(x):
    edges=[]
    graphNodes = []
    graphSet = set()
    toNodes = []
    node1 = x[0]
    listOfBusInN1 = userGroupByDict[x[0]]
    val = x[0]
    for i in range(val+1,len(userGroupByDict)):
        node2 = i
        listOfBusInN2 = userGroupByDict[node2]
        threshold = set(listOfBusInN1) & set(listOfBusInN2)
        if len(threshold) >=7:
            edges.append((node1,node2))
            graphNodes.append(node1)
            graphNodes.append(node2)
            graphSet.add(node1)
            graphSet.add(node2)
            toNodes.append(node2)
    return (graphSet,edges)

start = time.time()
data = sc.textFile(input_file_name)
header = data.first()
data = data.filter(lambda x: x!=header)
user_business = data.map(lambda x: x.split(',')).map(lambda x:(x[0],x[1]))
sortedUserBusiness = user_business.sortByKey()

listOfUserBusiness=sortedUserBusiness.collect()

users_set = {}
business_set = {}
u = 0
b = 0
for i in range(len(listOfUserBusiness)):
    if listOfUserBusiness[i][0] not in users_set:
        users_set.update({listOfUserBusiness[i][0]:u+1})
        u = u + 1
    if listOfUserBusiness[i][1] not in business_set:
        business_set.update({listOfUserBusiness[i][1]:b+1})
        b = b + 1
        
intUserBusiness = sortedUserBusiness.map(lambda x : (users_set.get(x[0]), business_set.get(x[1])))

userGroupBy = intUserBusiness.groupByKey().mapValues(list).sortByKey()
userGroupByDict = userGroupBy.collectAsMap()



edgeTuplesList = userGroupBy.map(findEdges)
vertices = edgeTuplesList.flatMap(lambda x : x[0]).distinct()
edges = edgeTuplesList.flatMap(lambda x: x[1])

verticesDF = vertices.map(lambda x: Row(id = x))
edgesDF = edges.map(lambda x: Row(src = x[0],dst = x[1]))

verticesDF = sqlContext.createDataFrame(verticesDF)
edgesDF = sqlContext.createDataFrame(edgesDF)

graph = GraphFrame(verticesDF, edgesDF)

communities = graph.labelPropagation(maxIter=5)

df = communities.groupBy('label').agg(collect_list('id'))

communitiesList = df.select('collect_list(id)').rdd.map(lambda x:(sorted(x[0]),len(x[0]))).sortBy(lambda x: x[1])
y = communitiesList.map(lambda x: x[0]).collect()
z = sorted(y)
z.sort(key=len)

f = open(output_file_name, "w")

key_list = list(users_set.keys())
val_list = list(users_set.values())

for x in z:
    for y in range(len(x)):
        if x[y] in val_list:
            f.write("'" + str(key_list[val_list.index(x[y])]) + "'")
            if y<len(x)-1:
                f.write(",")
    f.write('\n')
    
f.close()

end=time.time()

#print('Duration:', end-start)
