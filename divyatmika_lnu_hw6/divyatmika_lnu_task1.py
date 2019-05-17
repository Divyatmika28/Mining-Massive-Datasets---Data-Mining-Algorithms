#import numpy as np
#from bitarray import bitarray
import random
import binascii
import math
from pyspark.streaming import StreamingContext
from pyspark import SparkContext
import math
from math import pow
import datetime
import time
import json
import sys

def find_next_prime(n): return find_prime_in_range(n, 2*n)

def find_prime_in_range(a, b):
    for p in range(a, b):
        for i in range(2, p):
            if p % i == 0:
                break
        else:
            return p
    return None

#m : size of bit array n : number of items expected to be stored in filter 
'''def get_hash_count(m, n): 
    k = (m/n) * math.log(2) 
    return int(k)'''
             
'''def my_hash():
    maxim = 2**16-1
    a = random.randint(1, maxim-1)
    b = random.randint(2, maxim-1)
    prime = find_next_prime(120)
    return [a,b,prime]
    #return ((a*val + b) % prime) % m'''

def my_hash():
    maxim = 2**16-1
    a = random.randint(1, maxim-1)
    b = random.randint(2, maxim-1)
    prime = find_next_prime((random.randint(121,maxim)))
    return (a,b,prime)


def bloomFilter(stream):
    global bloom_filter
    global cities_seen
    global total_cities_seen
    global fp
    global y
    global xy
    #y = 0
    global filename
    if total_cities_seen == 0 and y== 0:
        #print("Time,FPR")
        output_file_name = open(filename, "w" )
        output_file_name.write("Time,FPR"+"\n" )
        #output_file_name.write("\n")
        y = 1
    else:
        output_file_name = open(filename, "a" )
        
    #print("collecting stream......")
    ts = time.time()
    st = datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')
    city_list = stream.collect()
    #print(city_list)
    n = len(city_list)
    xy += n
    #print("Length of cities")
    #print(n)
    if n != 0:
        hashCount = 12 #get_hash_count(200,n)
        # calculate unicode 
        city_unicode_list = [int(binascii.hexlify(s.encode('utf8')),16) for s in city_list]
        maxim = max(city_unicode_list)
        j = 0
        flag = 0 
        for city in city_unicode_list:
            flag = 0
            for i in range(0,len(hashparams)):
                params = hashparams[i]
                a = params[0]
                b = params[1]
                p = params[2]
                hval = ((a*city + b) % p) % 199
                if bloom_filter[hval] == 0:
                    bloom_filter[hval]=1
                    cities_seen.add(city)
                    flag = 1
            if flag!=1:
                if city not in cities_seen:
                    fp +=1
            total_cities_seen +=1 
        
        fpr = fp/total_cities_seen
        #print("timestamp,fpr")
        #print(st,fpr)
        #print(total_cities_seen)
        #print(xy)
        output_file_name.write(str(st) + "," + str(fpr) + "\n")
        output_file_name.close()
        #output_file_name.write("\n")
    else:
        output_file_name.write(str(st) + "," + str(0.0) + "\n")
        output_file_name.close()
        #output_file_name.write("\n")
        #print("timestamp,fpr")
        #print(st,0.0)
        
        
   
    #print(fpr)
   
    
    
  
    
sc = SparkContext("local[*]", "NetworkWordCount")
m = 200
xy = 0
y = 0
#bloom_filter = bitarray(m)
bloom_filter = [0] * (200)
#bloom_filter[:] = False
fp = 0
cities_seen = set()
a_val = b_val = p_val = []
total_cities_seen = 0
hashparams = []
for i in range(0,12,1):
    x = my_hash()
    hashparams.append(x)
ssc = StreamingContext(sc, 10)
sc.setLogLevel(logLevel="OFF")
#print("Starting......")
port_number = int(sys.argv[1])
filename = sys.argv[2]
stream_lines = ssc.socketTextStream("localhost", port_number)
cities = stream_lines.map(lambda x: json.loads(x)).map(lambda x: x["city"]).foreachRDD(bloomFilter)
ssc.start()
ssc.awaitTermination()
#sc.stop()
