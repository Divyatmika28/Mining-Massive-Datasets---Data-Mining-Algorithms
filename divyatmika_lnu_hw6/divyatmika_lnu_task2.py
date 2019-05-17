import math
#import statistics 
import binascii
#from bitarray import bitarray
import random 
from pyspark.streaming import StreamingContext
from pyspark import SparkContext, SparkConf
import time
import json
import datetime
import sys

def trailing_zeros(n):
    s = str(n)
    return len(s)-len(s.rstrip('0'))

def find_next_prime(n): return find_prime_in_range(n, 2*n)

def find_prime_in_range(a, b):
    for p in range(a, b):
        for i in range(2, p):
            if p % i == 0:
                break
        else:
            return p
    return None

def my_hash():
    maxim = 2**16-1
    a = random.randint(1, maxim-1)
    b = random.randint(2, maxim-1)
    prime = find_next_prime((random.randint(121,maxim)))
    return (a,b,prime)

def flajolet_martin(dStream):
    #output_file_name = open("fma.csv", "a" )
    global seQno
    global finalList
    global filename
    stream = dStream.collect()
    unique = set(stream)
    if seQno == 0:
        outfile = open(filename,"w")
        outfile.write("Time,Ground Truth,Estimation\n")
        seQno = 1
    else:
        outfile = open(filename,"a")
        
        
    truth = len(unique)
    ts = time.time()
    st = datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')
    countOfZeroes = []
    numHashFn = 12
    for h in range(0,len(hashparams),1):
        params = hashparams[i]
        a = params[0]
        b = params[1]
        p = params[2]
        maxZero = -10000
        for s in stream:
            int_val = int(binascii.hexlify(s.encode('utf8')),16)
            #hash_val = my_hash(int_val)
            hash_val = ((a*int_val + b) % p) % m
            binary_val = format(hash_val, '032b')
            if hash_val == 0:
                numOfZeroes = 0
            else:
                numOfZeroes = trailing_zeros(binary_val)
            if numOfZeroes > maxZero:
                maxZero = numOfZeroes
                val = binary_val
                xx = hash_val
            #print(maxZero)
            #print("Next")
        #print("Next hash function")
        #print(val)
        #print(xx)
        #print(maxZero)
        countOfZeroes.append(2**maxZero)
        #print(countOfZeroes)
    
    length = len(countOfZeroes)
    group1,group2,group3 = countOfZeroes[0:4],countOfZeroes[4:8],countOfZeroes[8:12]
    avg_g1 = sum(group1)/len(group1)
    avg_g2 = sum(group2)/len(group2)
    avg_g3 = sum(group3)/len(group3)
    x = [avg_g1,avg_g2,avg_g3]
    x.sort()
    estimate = int(x[1])
    finalList.append((st,truth,estimate))
    #print(st,truth,estimate)
    outfile.write(str(st) + "," + str(truth) + "," + str(estimate) + "\n" )
    outfile.close()
    
    

m = 2**8-1
batch_interval = 5
seQno = 0
hashparams = []
finalList = []
for i in range(0,12,1):
    x = my_hash()
    hashparams.append(x)
sc = SparkContext(appName = "inf5531")
ssc = StreamingContext(sc, batch_interval) # process each second
sc.setLogLevel(logLevel="OFF")
port_number = int(sys.argv[1])
filename = sys.argv[2]
lines = ssc.socketTextStream("localhost", port_number)
lines.window(30, 10).map(lambda x: json.loads(x)).map(lambda x: x["city"]).foreachRDD(flajolet_martin)
ssc.start()
ssc.awaitTermination()





