from pyspark import SparkContext
from collections import Counter
from itertools import combinations
import csv
import sys
import time
import os
import math
import codecs



def total_c_Count(baskets, candidates):
    item_dict = {}
    baskets = list(baskets)

    for candidate in candidates:
        if type(candidate) is int:
            candidate = [candidate]
            key = tuple(sorted(candidate))
        else:
            key = candidate
        candidate = set(candidate)
        for basket in baskets:
            if candidate.issubset(basket):
                if key in  item_dict:
                     item_dict[key] =  item_dict[key] + 1
                else:
                     item_dict[key] = 1
    return item_dict.items()



def apriori(baskets, support, chunkCount):
    localRes = list()
    baskets = list(baskets)
    threshold = support*(float(len(baskets))/float(chunkCount))
    #threshold = math.floor(threshold)  
    singleton = Counter()
    for basket in baskets:
        singleton.update(basket)
    c_singletons = {x : singleton[x] for x in singleton if singleton[x] >= threshold }     
    get_fre_singletons = sorted(c_singletons)
    localRes.extend(get_fre_singletons)
    k=2
    items_fre = set(get_fre_singletons)
    while len(items_fre) != 0:
        if k==2:
            pairs = list()
            for val in combinations(items_fre, 2):
                val = list(val)
                val.sort()
                pairs.append(val)
            candidate_k = pairs
                
                            
        else:
            #get candidates of k size
            perm = list()
            items_fre = list(items_fre)
            for i in range(len(items_fre)-1):
                for j in range(i+1, len(items_fre)):
                    a = items_fre[i]
                    b = items_fre[j]
                    if a[0:(k-2)] == b[0:(k-2)]:
                        perm.append(list(set(a) | set(b)))
                    else:
                        break
            candidate_k = perm
                            
        # k frequent items
        k_item_Dict = {}
        for candidate in candidate_k:
            candidate = set(candidate)
            key = tuple(sorted(candidate))
            for basket in baskets:
                if candidate.issubset(basket):
                    if key in k_item_Dict:
                        k_item_Dict[key] = k_item_Dict[key] + 1
                    else:
                        k_item_Dict[key] = 1
        kItem = Counter(k_item_Dict)
        k_fre_items = {x : kItem[x] for x in kItem if kItem[x] >= threshold }
        k_fre_items = sorted(k_fre_items)
        new_item_fre = k_fre_items
        localRes.extend(new_item_fre)
        items_fre = list(set(new_item_fre))
        items_fre.sort()
        k=k+1
    return localRes

    

start = time.time()

threshold = int(sys.argv[1])
support  = int(sys.argv[2])
inputFile = sys.argv[3]
outputFile = sys.argv[4]

sc = SparkContext("local[*]", "SON_DM")
rdd = sc.textFile(inputFile, minPartitions=None, use_unicode=False)
rdd = rdd.mapPartitions(lambda x : csv.reader(codecs.iterdecode(x, 'utf-8')))
header = rdd.first()
rdd = rdd.filter(lambda line:line!= header)
#x = rdd.collect()
#print("Hellllllo")
#print(x[0])
a = []
chunks = rdd.map(lambda x : (x[0], x[1])).groupByKey().mapValues(len).filter(lambda x : x[1]>threshold)
d = [list(elem) for elem in chunks.collect()]
for i in range(len(d)):
    a.append(d[i][0])
    
chunks = rdd.filter(lambda x : x[0] in a).map(lambda x : (x[0], x[1])).groupByKey().mapValues(set)
chunks = chunks.map(lambda x : x[1])
chunksCount = chunks.count()
#print("HELOOOOOOOOOOOOOOOOOOOO")
#print(chunksCount)

#Phase 1
map1 = chunks.mapPartitions(lambda chunk : apriori(chunk, support, chunksCount)).map(lambda x : (x, 1))
reduce1 = map1.reduceByKey(lambda x,y: (1)).keys().collect()

resFile = open(outputFile, "w")
resFile.write("Candidates:")
#reduce1 = sorted(reduce1, key = lambda item: (len(item), item))
single1= []
x_str = []
'''if len(reduce1) != 0:
    for i in range(len(reduce1)):
        if isinstance(reduce1[i], int):
            x= "'" + str(reduce1[i]) + "'"
            x= x.strip('\'"')
            z=[]
            z.append(x)
            z = tuple(z)
            str_val = str(z).replace(',', '')
            #print(str_val)
            single1.append(str_val)
        else:
            x_str.append(reduce1[i])'''
            
        
resFile.write("\n\n")
singles = []
kvalues=[]
if len(reduce1) != 0:
    for i in range(len(reduce1)):
        if isinstance(reduce1[i], str):
            singles.append(reduce1[i])
        else:
            kvalues.append(reduce1[i])
#print(type(singles))
singles = sorted(singles)
#print(singles)
for i in range(len(singles)):
    x= "'" + str(singles[i]) + "'"
    x= x.strip('\'"')
    z=[]
    z.append(x)
    z = tuple(z)
    str_val = str(z).replace(',', '')
    resFile.write(str_val)
#print("HELOOOOOOOOOOOOOOOOOOOO1")
kvalues = sorted(kvalues, key = lambda item: (len(item), item))   
resFile.write("\n")      
if len(kvalues) != 0:
	size = len(kvalues[0])
	#str_val = str(freItems[0]).replace(',', '') 
	#resFile.write(str_val)
	for i in range(0, len(kvalues)):
		c_size = len(kvalues[i])
		if size == c_size:
			resFile.write(", ")
		else:
			resFile.write("\n\n")

		if isinstance(kvalues[i], int):
                    resFile.write(kvalues[i])
                           
		if isinstance(kvalues[i], tuple):
                    x = []
                    for val in kvalues[i]:
                        val = "'" + str(val) + "'"
                        val = val.strip('\'"')
                        x.append(val)
                    x = tuple(x)
                    str_val = str(x)
                    resFile.write(str_val)
			
		
		size = c_size


#print("HELOOOOOOOOOOOOOOOOOOOO2")
#Phase 2
map2 = chunks.mapPartitions(lambda chunk : total_c_Count(chunk, reduce1))
reduce2 = map2.reduceByKey(lambda x,y: (x+y))

finalRes = reduce2.filter(lambda x: x[1] >= support)
#print("HELLLLLLOOOOOO")
#print(finalRes.collect())

#resFile = open(outputFile, "w")
freItems = finalRes.keys().collect()
freItems = sorted(freItems, key = lambda item: (len(item), item))
#print("HELLLLLLOOOOOO")
#print(len(freItems))
#print("HELOOOOOOOOOOOOOOOOOOOO21")
resFile.write("\n\n")
resFile.write("Frequent Itemsets:")
resFile.write("\n")
if len(freItems) != 0:
	size = len(freItems[0])
	j = 0
	#str_val = str(freItems[0]).replace(',', '') 
	#resFile.write(str_val)
	for i in range(0, len(freItems)):
		c_size = len(freItems[i])
		if size == c_size:
                    if j!= 0:
                        resFile.write(", ")
		else:
			resFile.write("\n\n")

		if c_size == 1:
                    z = []
                    #str_val = str(freItems[i]).replace(',', '')
                    for val in freItems[i]:
                        val = "'" + str(val) + "'"
                        val = val.strip('\'"')
                        z.append(val)
                    z = tuple(z)
                    str_val = str(z).replace(',', '')
                        
                    
		else:
                    x = []
                    for val in freItems[i]:
                        val = "'" + str(val) + "'"
                        val = val.strip('\'"')
                        x.append(val)
                    x = tuple(x)
                    str_val = str(x)                    
			
		resFile.write(str_val)
		size = c_size
		j = j +1

end = time.time()
print("Time taken: ")
print(end - start)



        
