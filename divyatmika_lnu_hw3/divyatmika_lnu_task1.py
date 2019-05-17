import itertools
from collections import defaultdict
import sys
from pyspark import SparkContext, SparkConf
from itertools import combinations
import time
import csv
import codecs

def getSignMat(x):
    a = 41
    b = 23
    sig = []

    for value in range(numOfHash):
        minHash = 111111111
        for i in x[1]:
            minHash = min(minHash, ((a + value) * i + (b + value)) % 671)
        sig.append(minHash)
    return (x[0], sig)

#start = time.time()
sc = SparkContext(appName="DM_3")
t1 = time.time()
bands = 40
numRows = 3
bins = 6
numOfHash = bands * numRows


input_file = sys.argv[1]
outFile = sys.argv[2]
rdd = sc.textFile(input_file, minPartitions=None, use_unicode=False)
rdd = rdd.mapPartitions(lambda x : csv.reader(codecs.iterdecode(x, 'utf-8')))
header = rdd.first()
rdd = rdd.filter(lambda x: x != header)
totalData = rdd.collect()
users = {}
business = {}
ku = 0
kb = 0
for i in range(len(totalData)):
    users.update({totalData[i][0]:ku+1})
    business.update({totalData[i][1]:kb+1})
    ku = ku + 1
    kb = kb + 1

rdd = rdd.map(lambda x : (users.get(x[0]), business.get(x[1])))
businessUserRdd = rdd.map(lambda x: (int(x[1]), int(x[0])))
busUserRdd = businessUserRdd.map(lambda x: (x[0], [x[1]])).reduceByKey(lambda a, b: a + b)
finalRddd = busUserRdd.collectAsMap()
busUserRdd = busUserRdd.map(lambda x: (x[0], list(x[1])))

signMat = busUserRdd.map(getSignMat).collectAsMap()


start = 0
end = 3
candidatePairsEachBand = defaultdict()
bandDictForEachBand = defaultdict()


def callTogetCandiPairs(input):
    global bandDictForEachBand
    global candidatePairsEachBand
    combinations = itertools.combinations(input,2)
    for comb in combinations:
                if(comb[0][1] == comb[1][1]):
                    candidatePairsEachBand[tuple((comb[0][0],comb[1][0]))] = 1

buckets = 301
eachBucket = []

for band in range(bands):
        #  b
        for x,y in signMat.items():
            val= signMat[x]
            eachRow= val[start:end]
            valSum =0
            for each in eachRow:
                valSum += each*3
            sumOfVal = valSum % 101
            if(sumOfVal in bandDictForEachBand.keys()):
                val = bandDictForEachBand[sumOfVal]
                val.append(tuple((x,eachRow)))
                bandDictForEachBand[sumOfVal] = val
            else:
                val = []
                val.append(tuple((x,y)))
                bandDictForEachBand[sumOfVal] = val

        for each in bandDictForEachBand.keys():
            callTogetCandiPairs(bandDictForEachBand[each])
            bandDictForEachBand[each] = []
            # p

        start = start + 3
        end = end+3



totalFullData =dict()

for each in candidatePairsEachBand.keys():
    l1 = set(finalRddd[each[0]])
    l2 = set(finalRddd[each[1]])
    tot = len(l1 & l2)/float(len(l1|l2))
    if(tot>= 0.5):
        totalFullData[tuple((each[0], each[1]))] = tot

bus_key_list = list(business.keys())
bus_val_list = list(business.values())
#total_data = totalFullData.items()
outFile = open(outFile, "w")
outFile.write("business_id_1,business_id_2,similarity" + "\n")
pairsNew = []
for k,v in totalFullData.items():
    business1 = str(bus_key_list[bus_val_list.index(k[0])])
    business2 = str(bus_key_list[bus_val_list.index(k[1])])
    pairsNew.append(((business1,business2),v))
    #outFile.write(business1+","+business2+","+str(v)+"\n" )

res = sc.parallelize(pairsNew).sortBy(lambda x: x[0][1]).sortBy(lambda x: x[0][0])
finalVal=res.collect()
for x in finalVal:
    outFile.write(str(x[0][0])+","+str(x[0][1])+","+ str(x[1])+"\n" )
    
  


t1_end = time.time()
totaltime = t1_end-t1




