import os
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages graphframes:graphframes:0.6.0-spark2.3-s_2.11 pyspark-shell'
import time
import os
from operator import add
from pyspark import SparkContext, SparkConf
import collections
from collections import defaultdict
import math
import sys
import itertools
import random
from itertools import chain, combinations

filter_threshold = int(sys.argv[1])
input_file_name = sys.argv[2]
output_file_name_bet = sys.argv[3]
output_file_name_comm = sys.argv[4]

    
sc =  SparkContext(appName = "inf553")

def findEdges(x):
    edges=[]
    graphNodes = []
    toNodes = []
    node1 = x[0]
    listOfBusInN1 = userGroupByDict[x[0]]
    val = x[0]
    for i in range(1,len(userGroupByDict)):
        if i != val:
            node2 = i
            listOfBusInN2 = userGroupByDict[node2]
            threshold = set(listOfBusInN1) & set(listOfBusInN2)
            if len(threshold) >=filter_threshold:
                edges.append(node2)
                graphNodes.append(node1)
                graphNodes.append(node2)
                toNodes.append(node2)
    if len(toNodes)>0:
        return (x[0],edges)
    
def GN(root):
    head = root
    bfsTraversal = [head]  # the list of bfs traversal order
    nodes = set(bfsTraversal)
    treeLevel = {head: 0}  # {node: level}
    dicParent = {}  # {childNode: [parent nodes in bfs graph]}
    index = 0  # the current index of the bfsTraversal
    while index < len(bfsTraversal):
        head = bfsTraversal[index]
        children = dicTree[head]
        for child in children:
            if child not in nodes:
                bfsTraversal.append(child)
                nodes.add(child)
                treeLevel[child] = treeLevel[head] + 1
                dicParent[child] = [head]
            else:
                if treeLevel[child] == treeLevel[head] + 1:
                    dicParent[child] += [head]
        index += 1
    # step 2: calculate betweenness for each bfs graph
    dicNodeWeight = {}
    for i in range(len(bfsTraversal) - 1, -1, -1):
        node = bfsTraversal[i]
        if node not in dicNodeWeight:
            dicNodeWeight[node] = 1
        if node in dicParent:
            parents = dicParent[node]
            parentsSize = len(parents)
            for parent in parents:
                addition = float(dicNodeWeight[node]) / parentsSize
                if parent not in dicNodeWeight:
                    dicNodeWeight[parent] = 1
                dicNodeWeight[parent] += addition
                tmpEdge = (min(node, parent), max(node, parent))
                yield (tmpEdge, addition / 2)
                
def divide(element):
    e = element[0]
    for i in range(0,2):
            root = e[i]
            visit = {}
            q = []
            visit[root]=True
            q.append(root)

            index = 0
            while index<len(q):
                u = q[index]
                #print(u)
                index+=1
                for v in ver:
                    #print(v)
                    if Aij[u][v]==1:
                        if v == e[1-i]:
                            return False    # if find the other end node: no new components
                        if v not in visit:
                            q.append(v)
                            belongsTo[v] = belongsTo[root]
                            visit[v] = True
            belongsTo[e[1]]=e[1]
    return True
        

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



edgeTuplesList = userGroupBy.map(findEdges).filter(lambda x: x is not None)
vertices = edgeTuplesList.map(lambda x: x[0])
#edges = edgeTuplesList.map(lambda x: x[1])
betweenessList = edgeTuplesList.collect()
dicTree = {}
for x in betweenessList:
    dicTree[x[0]] = x[1]

betweenness = edgeTuplesList.flatMap(lambda x: GN(x[0])).reduceByKey(lambda x, y: x + y).sortBy(lambda x: x[0][1]).sortBy(lambda x: x[0][0])
sortedBetweenness = betweenness.sortBy(lambda x : x[1],ascending = False)
deno = sortedBetweenness.count()
writeBetween = sortedBetweenness.collect()
f = open(output_file_name_bet, "w")

key_list = list(users_set.keys())
val_list = list(users_set.values())

for x in writeBetween:
    f.write("('" + str(key_list[val_list.index(x[0][0])]) + "', '" + str(key_list[val_list.index(x[0][1])]) + "'), " + str(x[1]))
    f.write('\n')
    
f.close()



Aij = {}
Bij = {}
edgeDict = edgeTuplesList.collectAsMap()
ver=vertices.collect()

m=0
for k,v in edgeDict.items():
    Aij[k] = {}
    m += len(v)
    for u in ver:
        Aij[k][u] = 0
nEdges = m/2
for k,v in edgeDict.items():
    for u in v:
        Aij[k][u] =1
        
for u in ver:
    if u not in Bij:
        Bij[u] = {}
    for v in ver:
        Bij[u][v] = Aij[u][v] - float(len(edgeDict[u])*len(edgeDict[v]))/(2*nEdges)
        
iniM = 0
for u in ver:
    for v in ver:
        iniM += Bij[u][v]
maxM = iniM/deno

maxbelong = {}
M = 0

belongsTo = {} 

for u in ver:
    belongsTo[u] = 1
        
sb = sortedBetweenness.collect()
for e in sb:
    Aij[e[0][0]][e[0][1]] = 0
    Aij[e[0][1]][e[0][0]] = 0
    if divide(e)==True:
        M=0
        for u in ver:
            for v in ver:
                if belongsTo[u]==belongsTo[v]:  # u v in the same group
                    M+=Bij[u][v]
        M = M/deno

        if M>maxM:
            maxM = M
            maxbelong = belongsTo.copy()
        elif M<maxM:
            continue
            
groups = {}
for (node, grp) in maxbelong.items():
    if grp not in groups:
        groups[grp] = list()
    groups[grp].append(node)

community = sorted([v for (k,v) in groups.items()], reverse = True)


commRdd = sc.parallelize(community)
commList = commRdd.map(lambda x:(sorted(x),len(x))).sortBy(lambda x: x[1])
y = commList.map(lambda x: x[0]).collect()
z = sorted(y)
z.sort(key=len)

f = open(output_file_name_comm, "w")

#key_list = list(users_set.keys())
#val_list = list(users_set.values())

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

