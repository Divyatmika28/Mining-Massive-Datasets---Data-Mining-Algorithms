from pyspark import SparkContext
import json
import time
import sys
import os


start = time.time()
sc=SparkContext(appName="inf553")


input_file=sys.argv[1]
output_file=sys.argv[2]

#with open(input_path) as input_file:
raw_data = sc.textFile(input_file)
    
dataset = raw_data.map(json.loads)


#1.no of reviews
n_review=dataset.count()

#2.no of reviews in 2018
n_review_2018=dataset.filter(lambda x: x['date'][:4]=='2018').count()

#3.no of distinct users
p=dataset.map(lambda x : x['user_id']).collect()
n_user=len(set(p))

#4.top 10 users who wrote largest no fo reviews and count. 
users = dataset.map(lambda x: (x['user_id'], 1))

user = users.reduceByKey(lambda accum, n: accum + n)
res=user.collect()
xyz=dict(res)

sorted_by_users = sorted(xyz.items(), key=lambda kv: kv[1])
sorted_by_users.reverse()
top10user= sorted_by_users[:10]

#5. number of distinct businesses have been reviewed.
z=dataset.map(lambda x :x['business_id']).collect()
n_business=len(set(z))

#6. top 10 businesses
business = dataset.map(lambda x: (x['business_id'], 1))

y = business.reduceByKey(lambda accum, n: accum + n)
z=y.collect()
bus=dict(z)
    
sorted_by_business = sorted(bus.items(), key=lambda kv: kv[1])
sorted_by_business.reverse()
top10bus=sorted_by_business[:10]

output={}
output['n_review']=n_review
output['n_review_2018']=n_review_2018
output['n_user']=n_user
output['top10_user']=top10user
output['n_business']=n_business
output['top10_business']=top10bus

with open(output_file, 'w+') as outfile:
    json.dump(output, outfile)

#print("Time taken:")
#print(time.time()-start)









