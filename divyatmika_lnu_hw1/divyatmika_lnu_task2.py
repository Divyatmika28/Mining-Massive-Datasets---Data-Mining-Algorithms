from pyspark import SparkContext
import json
import time
import sys

def count_in_a_partition(iterator):
    yield sum(1 for _ in iterator)


sc=SparkContext(appName="inf553")

input_file_1=sys.argv[1]
output_file=sys.argv[2]
n_partition=int(sys.argv[3])

#review.json
raw_data = sc.textFile(input_file_1)
dataset = raw_data.map(json.loads)

#print("Default partiton;")
business_rdd= dataset.map(lambda x: (x['business_id'], 1))
start = time.time()
y = business_rdd.reduceByKey(lambda accum, n: accum + n)
z=y.collect()
bus=dict(z)
sorted_by_business = sorted(bus.items(), key=lambda kv: kv[1])
sorted_by_business.reverse()
top10bus=sorted_by_business[:10]
exe_time_def = time.time()-start

#print("Time------")
#print(time.time()-start)
#print(top10bus)

default_partition = business_rdd.getNumPartitions()
#print("Default partiton nos;")
#print(default_partition)

item_by_partition = business_rdd.mapPartitions(count_in_a_partition).collect()
#print("item in partition")
#print(item_by_partition)

print("Custom partition")

business_rdd= dataset.map(lambda x: (x['business_id'], 1))
business_rdd_custom = business_rdd.partitionBy(n_partition)
start_cus = time.time()
y = business_rdd_custom.reduceByKey(lambda accum, n: accum + n)
z=y.collect()
bus=dict(z)
sorted_by_business = sorted(bus.items(), key=lambda kv: kv[1])
sorted_by_business.reverse()
top10bus=sorted_by_business[:10]
exe_time_cus = time.time()-start_cus
print("Time Custom ------")
print(time.time()-start_cus)
item_by_partition_cus = business_rdd_custom.mapPartitions(count_in_a_partition).collect()

print("item in partition")
print(item_by_partition_cus)

default={}
default['n_partitions']= default_partition
default['n_items']= item_by_partition
default['exe_time']= exe_time_def

customized={}
customized['n_partitions']= n_partition
customized['n_items']= item_by_partition_cus
customized['exe_time']= exe_time_cus

explanation = "Defualt partitions has partitions equal to nodes in cluster. Reducing partitions reduces time because of less time in scheduling and resource utilization. "
output={}
output['default']= default
output['customized']=customized
output['explanation']=explanation

with open(output_file, 'w+') as outfile:
    json.dump(output, outfile)




