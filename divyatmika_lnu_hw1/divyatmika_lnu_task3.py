from pyspark import SparkContext
import json
import time
import sys


sc=SparkContext(appName="inf553")

input_file_1=sys.argv[1]
input_file_2=sys.argv[2]
output_file=sys.argv[3]
output_file_2=sys.argv[4]
#review.json
raw_data = sc.textFile(input_file_1)
dataset = raw_data.map(json.loads)
#business.json 
bus_raw_data=sc.textFile(input_file_2)
business = bus_raw_data.map(json.loads)

review_id_stars = dataset.map(lambda x: (x['business_id'], x['stars']))
bus_id_cities=business.map(lambda x: (x['business_id'], x['city']))
res = bus_id_cities.join(review_id_stars)

rdd1 = res.map(lambda x: x[1])
aTuple = (0,0)
rdd1 = rdd1.aggregateByKey(aTuple, lambda a,b: (a[0] + b,    a[1] + 1),lambda a,b: (a[0] + b[0], a[1] + b[1]))
average=rdd1.mapValues(lambda v: v[0]/v[1])
x=average.sortByKey()
y=x.map(lambda x:(x[1],x[0])).sortByKey(ascending=False).map(lambda aTuple: (aTuple[0], aTuple[1]))
start = time.time()
output=y.collect()   
result_a = output[:10]
print("Top 10 cities with average rating with method 1:")
print(result_a)
m1 = time.time() - start  #method 1
with open(output_file, "w+") as f:
    f.write("city,stars \n")
    for res in output:
        f.write(str(res[0])+"\t"+str(res[1])+"\n")
        
#print("Time------")
#part----2

start_m2=time.time()
output_by_take=y.take(10)
result_b = output_by_take[:10]
print("Top 10 cities with average rating with method 2:")
print(result_b)
m2=time.time()-start_m2


output_1 = {}
output_1['m1']=m1
output_1['m2']=m2
output_1['explanation']="The collect function collects data from the entire RDD but the take function just takes first 10 values from the RDD so its much faster"

with open(output_file_2, 'w+') as outfile:
    json.dump(output_1, outfile)


