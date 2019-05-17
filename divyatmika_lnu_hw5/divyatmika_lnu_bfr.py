import numpy as np
from sklearn.cluster import KMeans
import time
import os
import collections
from collections import defaultdict
import math
import sys
import itertools
import random
from itertools import chain, combinations
import ast


input_file_name = sys.argv[1]
n_cluster = int(sys.argv[2])
output_file_name = sys.argv[3]

            
def getSD(summary):
    std = {}
    for k,v in summary.items():
        N = v[0]
        SUM = v[1]
        SUMQ = v[2]
        temp = []
        for x in range(len(SUM)):
            avg = SUM[x]/N
            b = avg * avg
            a = SUMQ[x]/N
            var = a-b
            sd = math.sqrt(var)
            temp.append(sd)
        std[k] = temp
    return std

def getMDistance(point, centroid, std):
    MD = 0
    val3 = 0
    for x in range(len(centroid)):
        a = point[x] - centroid[x]
        b = std[x]
        val = a/b
        val2 = val*val
        val3 = val3 + val2
    final = math.sqrt(val3)
    return final

def getSummaryStats(listOfCoords):
    try:
        N = len(listOfCoords)
        SUM = [sum(x) for x in zip(*listOfCoords)]
        SUMsq = []
        for x in zip(*listOfCoords):
            SUMSQ = 0
            for i in x: 
                SUMSQ = SUMSQ + i*i
            SUMsq.append(SUMSQ)
    except:
        N =1
        SUM = list(listOfCoords)
        SUMsq = []
        for x in range(len(SUM)):
            SUMSQ = SUM[x] * SUM[x]
            SUMsq.append(SUMSQ)
    return [N,SUM,SUMsq]

def getCentroid(summary):
    try:
        centroids = {}
        for k,v in summary.items():
            size = v[0]
            eachAvg = []
            for i in range(len(v[1])):
                avg = v[1][i]/size
                eachAvg.append(avg)
            centroids[k] = eachAvg
        return centroids
    except:
        N = summary[0]
        centroid = []
        for x in range(len(summary[1])):
            avg = summary[1][x]/N
            centroid.append(avg)
        return centroid

def updateSummaryStats(cluster,newPoint):
    N = cluster[0] + newPoint[0]
    summ = []
    sumq = []
    SUM = 0
    SUMQ = 0
    for x in range(len(cluster[1])):
        SUM = cluster[1][x] + newPoint[1][x]
        SUMQ = cluster[2][x] + newPoint[2][x]
        summ.append(SUM)
        sumq.append(SUMQ)
    return [N,summ,sumq]
        
    
start = time.time()
percentage = 0.2
inputData = open(input_file_name,"r")

lines = [ x.strip() for x in inputData.readlines()]

eachLineStrings = []
for line in lines:
    temp = []
    temp = line.split(",")
    tempList = []
    for string in temp:
        tempList.append(float(string))
    eachLineStrings.append(tempList)

#Step 1: load 20% of data
intermediateResults = {}
npData = np.array(eachLineStrings)
y = npData[:,1:2]
n_sample = len(npData)
percentage = 0.2
init_data = npData[:int(n_sample*percentage)]
chunkDict = {}
for x in init_data:
    chunkDict[repr(list(x[2:]))] = int(x[0])

DSkmeansInput_X = list(chunkDict.keys())

#Step 2: kmeans on all data

DSkmeansInputX = []
for i in DSkmeansInput_X:
    DSkmeansInputX.append(ast.literal_eval(i))

DSkmeansInputX = np.array(DSkmeansInputX)
   
kmeansDS = KMeans(n_clusters=10*n_cluster, random_state=0).fit(DSkmeansInputX)
labelCounterDS = dict(collections.Counter(kmeansDS.labels_))

#step 3: assigning clusters with 1 point to RS
RS = []
DSkmeansInput_XNew = []
for x in labelCounterDS:
    if labelCounterDS[x] == 1 :
        index = np.where(kmeansDS.labels_==x)
        points = DSkmeansInputX[index][0]
        RS.append(points)
    else:
        index = np.where(kmeansDS.labels_==x)
        for i in index[0]:
            #print(i)
            points = DSkmeansInputX[i]
            DSkmeansInput_XNew.append(points)
        
DSkmeansInput_XNew = np.array(DSkmeansInput_XNew)
   
#step 4: kmeans on the remaining data
kmeansDS2 = KMeans(n_clusters=n_cluster, random_state=0).fit(DSkmeansInput_XNew)
labelCounterDS2 = dict(collections.Counter(kmeansDS2.labels_))

DSClusterIndex = {}
DSClusters = {}
for x in labelCounterDS2:
    index = np.where(kmeansDS2.labels_==x)
    tempIndex = []
    tempCoords = []
    for i in index[0]:
        #print(i)
        coords = DSkmeansInput_XNew[i]
        dictIndex = int(chunkDict.get(repr(list(coords))))
        tempIndex.append(dictIndex)
        tempCoords.append(coords)
    DSClusterIndex[x] = tempIndex
    DSClusters[x] = getSummaryStats(tempCoords)

#Step 6: Running kmeans on RS and getting final RS and CS
kmeansRS = KMeans(n_clusters=n_cluster, random_state=0).fit(RS)
labelCounter3 = collections.Counter(kmeansRS.labels_)

RS_new = []
CSClusterIndex = {}
CSClusters = {}
for x in labelCounter3:
    if labelCounter3[x]==1:
        index = np.where(kmeansRS.labels_== x)
        #print('1',index)
        coords = RS[index[0][0]]
        RS_new.append(coords)
    else:
        index = np.where(kmeansRS.labels_==x)
        tempIndex = []
        tempCoords = []
        for i in index[0]:
            #print(i)
            coords = RS[i]
            dictIndex = int(chunkDict.get(repr(list(coords))))
            tempIndex.append(dictIndex)
            tempCoords.append(coords)
        CSClusterIndex[x] = tempIndex
        CSClusters[x] = getSummaryStats(tempCoords)
        
RS = RS_new
RS_new = []
RSDict = {}
for x in RS:
    dictIndex = int(chunkDict.get(repr(list(x))))
    RSDict[repr(list(x))] = int(dictIndex)
    
Round = 1
DScount = 0
CSClusterscount = 0
CScount = 0
RScount = 0
for k,v in DSClusters.items():
     DScount += v[0]
for k,v in CSClusters.items():
     CScount += v[0]
CSClusterscount = len(CSClusters)
RScount = len(RSDict)
intermediateResults[Round] = [DScount,CSClusterscount,CScount,RScount]

#Step 7: Load another 20% of the data

startLoop = int(n_sample * percentage)
endLoop = startLoop + int(n_sample * percentage)
d = 10
while startLoop < n_sample:
    init_data = npData[startLoop:endLoop]
    chunkDict = {}
    for x in init_data:
        chunkDict[repr(list(x[2:]))] = int(x[0])

    InputS = list(chunkDict.keys())
    InputF = []
    for i in InputS:
        InputF.append(ast.literal_eval(i))

    InputF = np.array(InputF)
    DSCentroids = getCentroid(DSClusters)
    CSCentroids = getCentroid(CSClusters)
    DS_SD = getSD(DSClusters)
    CS_SD = getSD(CSClusters)

    for x in InputF:
        #Step 8: for each incoming point compare the Mahalnobis distance between DS centroids and the point
        mini = 2*(math.sqrt(d))+1
        mergeCluster = 0
        DSAppendDict = {}
        CSAppendDict = {}
        for dc,v in DSCentroids.items():
            mahalanobisDistance = getMDistance(list(x),v,DS_SD[dc])
            if mahalanobisDistance < mini:
                mini = mahalanobisDistance
                mergeClusterDS = dc
        if mini < 2*(math.sqrt(d)):
            xSummary = getSummaryStats(x)
            dictIndex = int(chunkDict.get(repr(list(x))))
            DSClusterIndex[mergeClusterDS].append(dictIndex)
            DSClusters[mergeClusterDS] = updateSummaryStats(DSClusters[mergeClusterDS],xSummary)
            #DSAppendDict[mergeClusterDS].append(x)
        else:
            #Step 9: for each incoming point compare the mahalanobis distance between CS centroids and the point
            mini = 2*(math.sqrt(d))+1
            for cc,v in CSCentroids.items():
                mahalanobisDistanceCS = getMDistance(list(x),v,CS_SD[cc])
                if mahalanobisDistanceCS < mini:
                    mini = mahalanobisDistanceCS
                    mergeClusterCS = cc
            if mini < 2*(math.sqrt(d)):
                #CSAppendDict[mergeClusterCS].append(x)
                dictIndex = int(chunkDict.get(repr(list(x))))
                CSClusterIndex[mergeClusterCS].append(dictIndex)
                xSummary = getSummaryStats(x)
                CSClusters[mergeClusterCS] = updateSummaryStats(CSClusters[mergeClusterCS],xSummary)
            else:
                #Step 10: Assign point to RS
                dictIndex = int(chunkDict.get(repr(list(x))))
                RSDict[repr(list(x))] = int(dictIndex)
                RS.append(x)

    #Step 11: Run Kmeans on remaining RS and separate RS and CS Clusters
    if len(RS) > n_cluster:
        kmeansRSLoop = KMeans(n_clusters=n_cluster, random_state=0).fit(RS)
        labelCounterLoop = collections.Counter(kmeansRSLoop.labels_)

        RS_new = []
        for x in labelCounterLoop:
            if labelCounterLoop[x]==1:
                index = np.where(kmeansRSLoop.labels_== x)
                #print('1',index)
                coords = RS[index[0][0]]
                RS_new.append(coords)
            else:
                index = np.where(kmeansRSLoop.labels_==x)
                tempIndex = []
                tempCoords = []
                for i in index[0]:
                    #print(i)
                    coords = RS[i]
                    dictIndex = int(RSDict.get(repr(list(coords))))
                    tempIndex.append(dictIndex)
                    tempCoords.append(coords)
                mini = 2*(math.sqrt(d))+1
                mergeClusterNew = 0
                for cc,v in CSCentroids.items():
                    newCSCluster = getSummaryStats(tempCoords)
                    newCSCentroid = getCentroid(newCSCluster)
                    CSDistance = getMDistance(newCSCentroid, v, CS_SD[cc])
                    if CSDistance < mini:
                        mini = CSDistance
                        mergeClusterNew = cc
                if mini < 2*(math.sqrt(d)):
                    #Step 12: Merging CS Clusters that have a Mahalanobis Distance of < 2*sqrt(d)
                    #CSAppendDict[mergeClusterCS].append(x)
                    xSummary = newCSCluster
                    CSClusters[mergeClusterNew] = updateSummaryStats(CSClusters[mergeClusterNew],xSummary)
                    if len(tempIndex)>1:
                        for x in tempIndex:
                            CSClusterIndex[mergeClusterNew].append(x)
                else:
                    key = max(CSClusters.keys()) + 1
                    CSClusterIndex[key] = tempIndex
                    CSClusters[key] = getSummaryStats(tempCoords)

        RS = RS_new
        RS_new = []
        RSDictN = {}
        for x in RS:
            try:
                dictIndex = int(RSDict.get(repr(list(x))))
                RSDictN[repr(list(x))] = int(dictIndex)
            except:
                dictIndex = int(chunkDict.get(repr(list(x))))
                RSDictN[repr(list(x))] = int(dictIndex)

        RSDict = RSDictN
        RSDictN = {}
    startLoop = endLoop
    endLoop = startLoop + int(n_sample * percentage)
    if endLoop > n_sample:
        #Last Step: Merge CS Clusters with DS Clusters
        DSCentroids = getCentroid(DSClusters)
        CSCentroids = getCentroid(CSClusters)
        DS_SD = getSD(DSClusters)
        CS_SD = getSD(CSClusters)
        CSClustersFinal = {}

        for k,v in CSCentroids.items():
            mini = 2*(math.sqrt(d))+1
            mergeCluster = 0
            DSAppendDict = {}
            CSAppendDict = {} 
            for k1,v1 in DSCentroids.items():
                maDistance = getMDistance(v,v1,DS_SD[k1])
                if maDistance < mini:
                    mini = maDistance
                    mergeCSDS = k1
            if mini < 2*(math.sqrt(d)):
                CSIndexes = CSClusterIndex[k]
                if len(CSIndexes)>1:
                    for x in CSIndexes:
                        DSClusterIndex[mergeClusterDS].append(x)
                DSClusters[mergeClusterDS] = updateSummaryStats(DSClusters[mergeClusterDS],xSummary)
                #DSAppendDict[mergeClusterDS].append(x)
            else:
                CSClustersFinal[k] = CSClusters[k]
        Round += 1
        DScount = 0
        CSClusterscount = 0
        CScount = 0
        RScount = 0
        for k,v in DSClusters.items():
             DScount += v[0]
        for k,v in CSClustersFinal.items():
             CScount += v[0]
        CSClusterscount = len(CSClustersFinal)
        RScount = len(RSDict)
        intermediateResults[Round] = [DScount,CSClusterscount,CScount,RScount]


    else:
        Round += 1
        DScount = 0
        CSClusterscount = 0
        CScount = 0
        RScount = 0
        for k,v in DSClusters.items():
             DScount += v[0]
        for k,v in CSClusters.items():
             CScount += v[0]
        CSClusterscount = len(CSClusters)
        RScount = len(RSDict)
        intermediateResults[Round] = [DScount,CSClusterscount,CScount,RScount]
        
finalClusterDict = {}
for k,v in DSClusterIndex.items():
    for i in v:
        finalClusterDict[i] = k
for k,v in CSClusterIndex.items():
    for i in v:
        finalClusterDict[i] = -1
for k,v in RSDict.items():
    finalClusterDict[v] = -1
    
pred = dict(sorted(finalClusterDict.items()))

f = open(output_file_name, "w")
f.write("The intermediate results:\n")
for k,v in intermediateResults.items():
    f.write("Round " + str(k) + ":" + str(v[0]) + ", " + str(v[1]) + ", " + str(v[2]) + ", " + str(v[3]) + "\n")
f.write("The clustering results:\n")
for k,v in pred.items():
    f.write( str(k)+ ", " + str(v) + "\n" )
f.close()
            
#print("Duration:",time.time()-start)
