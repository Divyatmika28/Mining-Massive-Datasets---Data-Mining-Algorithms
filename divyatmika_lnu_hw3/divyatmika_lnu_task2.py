import itertools
from collections import defaultdict
from itertools import combinations
from pyspark import SparkContext
from pyspark.mllib.recommendation import ALS, Rating
import csv
import sys
import time
import math
import codecs
import random


def parse1(x):
    if x[0] in users:
        a = users.get(x[0])
    else:
        a = max(users.values()) + 1
        users.update({x[0]:a})
    if x[1] in business_id:
        b = business_id.get(x[1])
    else:
        b = max(business_id.values()) + 1
        business_id.update({x[1]:b})
    return ((a,b),float(x[2]))

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



def bus_avg(x):
    avg = sum(x[1])/len(x[1])
    return (x[0],avg)

def predict(predUser, predBusiness, foundSameUsersForPrediction):
    predUserData = user_dict[predUser]
    a_temp = [x[1] for x in predUserData]
    a_mean = sum(a_temp)/len(a_temp)
    if len(foundSameUsersForPrediction) == 0:
        return a_mean
    else:
        num = 0
        den = 0
        for sameUser in foundSameUsersForPrediction:
            otherUser = sameUser[1]
            key = (otherUser, predBusiness)
            if key in userBusinessDict:
                o_rating = userBusinessDict[(otherUser, predBusiness)]
                otherData = user_dict[otherUser]
                o_temp = [x[1] for x in otherData if otherData[0] != predBusiness]
                o_mean = sum(o_temp)/len(o_temp)
                den = den + abs(sameUser[0])
                num = num + sameUser[0]*(o_rating - o_mean)
        if den == 0:
            return a_mean
        else:
            return (a_mean + num/den)

def predictAll(predUser, predBusiness, sim):
    if len(sim)==1:
        if sim[0][0] == -2:
            return 3.5
        elif sim[0][0] == -3:
            b_mean = busAvg[b]
            return b_mean
        elif sim[0][0]==-4:
            u_list = [x[1] for x in user_dict[predUser]]
            u_mean = sum(u_list)/len(u_list)
            return u_mean
        else:
            val = predict(predUser,predBusiness,sim)
            return val
    else:
        val = predict(predUser,predBusiness,sim)
        return val

def callTogetCandiPairs(input):
    global bandDictForEachBand
    global candidatePairsEachBand
    combinations = itertools.combinations(input,2)
    for comb in combinations:
                if(comb[0][1] == comb[1][1]):
                    candidatePairsEachBand[tuple((comb[0][0],comb[1][0]))] = 1

def calPearsonCorrelation(predUserData, otherUserData):
    corratedItems = list()
    i = 0
    j = 0
    predUserData.sort()
    otherUserData.sort()
    while (i<len(predUserData) and j< len(otherUserData)):
        if predUserData[i][0] == otherUserData[j][0]:
            corratedItems.append((predUserData[i][0], (predUserData[i][1], otherUserData[j][1]))) # list of rating of the active user and the other user for each movie:(M1, (A_R1,O_R1))
            i = i+1
            j = j+1
        elif predUserData[i][0] < otherUserData[j][0]:
            i = i+1
        else:
            j = j+1
    if len(corratedItems) < 5:
        return 0
    
    predUserRatings = [x[1][0] for x in corratedItems]
    a_mean = sum(predUserRatings)/len(predUserRatings)
    otherRatings = [x[1][1] for x in corratedItems]
    o_mean = sum(otherRatings)/len(otherRatings)
    a_list = predUserRatings
    o_list = otherRatings
    num = 0.0
    d1 = 0.0
    d2 = 0.0
    for i in range(len(a_list)):
        a = a_list[i] - a_mean
        o = o_list[i] - o_mean
        num = num + a*o
        d1 = d1 + (a*a)
        d2 = d2 + (o*o)
    den = math.sqrt(d1) * math.sqrt(d2)
    if den == 0 or num == 0:
        return -2.0
    else: 
        return num/den     
def findWeightOfItemsWithLSH(predUser, predBusiness):
    listOfSameBusiness  = list()
    if (predBusiness not in similarPairs1) and (predBusiness not in similarPairs2):
        listOfSameBusiness.append((1, predBusiness)) # no business similar to active bus # assign average rating of user
        return listOfSameBusiness
    if predBusiness not in business_dict:
        listOfSameBusiness.append((1, predBusiness)) # item never rated by any user (item cold start)
        return listOfSameBusiness
    allRatingsForActiveBus = business_dict[predBusiness]
    allOtherBusiness = list()
    if predBusiness in similarPairs1:
        allOtherBusiness = allOtherBusiness + similarPairs1[predBusiness]
    if predBusiness in similarPairs2:
        allOtherBusiness = allOtherBusiness + similarPairs2[predBusiness]
    allOtherBusiness = list(set(allOtherBusiness))
    for bus in allOtherBusiness:
        if predBusiness != bus:
            o_bus_data = business_dict[bus]
            pearson_corr = calPearsonCorrelation(allRatingsForActiveBus, o_bus_data)
            if pearson_corr != -2.0:
                listOfSameBusiness.append((pearson_corr, bus))
            else:
                listOfSameBusiness.append((1, predBusiness))
    getSameBusiness = sorted(listOfSameBusiness, reverse=True)
    return getSameBusiness

def findWeightofUsers(predUser, predBusiness):
    listOfSameUsers = list()
    if predBusiness not in business_dict:
        listOfSameUsers.append((0, predUser))
        return listOfSameUsers
    predUserData = user_dict[predUser]    # tuple of all the items and their ratings that the user has rated.
    listOfOtherUsers = business_dict[predBusiness]     #list of other users who have rated item i. 
    for otherUser in listOfOtherUsers:
        if predUser != otherUser:
            otherUserData = user_dict[otherUser]     #tuple of items and ratings by other users.
            if len(otherUserData) > 360:
                weight = calPearsonCorrelation(predUserData, otherUserData)
                if weight != -2.0:
                    listOfSameUsers.append((weight, otherUser))
    listOfSameUsers = sorted(listOfSameUsers, reverse=True)
    return listOfSameUsers
def predictItemsBased(predUser, predBusiness, listOfSameItems):
    predUserData = user_dict[predUser]# average of active user redundant
    a_temp = [x[1] for x in predUserData]
    a_mean = sum(a_temp)/len(a_temp)
    listOfSameItems = list(set(listOfSameItems))
    if len(listOfSameItems) == 0:
        return a_mean
    if(len(listOfSameItems) == 1) and (listOfSameItems[0][1] == predBusiness):
        return a_mean
    else:
        num = 0
        den = 0
        for sameBusiness in listOfSameItems:
            otherBusiness = sameBusiness[1]
            key = (predUser, otherBusiness)
            if key in userBusinessDict:
                o_rating = userBusinessDict[(predUser, otherBusiness)]
                den = den + abs(sameBusiness[0])
                num = num + sameBusiness[0]*o_rating
        if den == 0 or num == 0:
            return a_mean
        else:
            pred = num/den
            if pred < 0:
                pred = -1 * pred
            return pred

def calPearsonCorrelationItems(predItemData, otherItemData):
    corratedBusiness = list()
    i = 0
    j = 0
    predItemData.sort()
    otherItemData.sort()
    while (i<len(predItemData) and j< len(otherItemData)):
        if predItemData[i][0] == otherItemData[j][0]:
            corratedBusiness.append((predItemData[i][0], (predItemData[i][1], otherItemData[j][1])))
            i = i+1
            j = j+1
        elif predItemData[i][0] < otherItemData[j][0]:
            i = i+1
        else:
            j = j+1
    if len(corratedBusiness) == 0 or len(corratedBusiness) == 1:
        return -2.0 # no corrated items or only 1 corrated items
    active = [x[1][0] for x in corratedBusiness]
    a_mean = sum(active)/len(active)
    other = [x[1][1] for x in corratedBusiness]
    o_mean = sum(other)/len(other)
    a_list = active
    o_list = other
    num = 0.0
    d1 = 0.0
    d2 = 0.0
    for i in range(len(a_list)):
        a = a_list[i] - a_mean
        o = o_list[i] - o_mean
        num = num + a*o
        d1 = d1 + (a*a)
        d2 = d2 + (o*o)
    den = math.sqrt(d1) * math.sqrt(d2)
    if den == 0 or num == 0:
        return -2.0
    else:
        return num/den

def findWeightOfItemsWithoutLSH(predUser, predBusiness):
    listOfSameBusiness = list()
    if predBusiness not in business_dict:
        listOfSameBusiness.append((1, predBusiness))# item never rated by any user (item cold start)
        return listOfSameBusiness
    allRatingsForActiveBus = business_dict[predBusiness] #list of users and rating for a bus.
    allRatingsByActiveUser = user_dict[predUser] #list of businesss and ratings by that active user. 
    otherBusRatedByActiveUser = [x[0] for x in allRatingsByActiveUser] # list of other items rated by the active user.
    if len(otherBusRatedByActiveUser) > 480:
        for otherBus in otherBusRatedByActiveUser:    #check one
            if predBusiness != otherBus:
                allRatingsForOtherBusData = business_dict[otherBus]
                weightPearCorr = calPearsonCorrelationItems(allRatingsForActiveBus, allRatingsForOtherBusData)
                if weightPearCorr != -2.0:
                    listOfSameBusiness.append((weightPearCorr, otherBus))
                else:
                    listOfSameBusiness.append((1, predBusiness))
        getNeighbourSameBusiness = sorted(listOfSameBusiness, reverse=True)
        return getNeighbourSameBusiness[:10] #Neighbourhood value 10
    else:
        listOfSameBusiness = list()
        return listOfSameBusiness

def computeJaccardSimilarity(dic, bus1, bus2):
    a = dic[bus1]
    b = dic[bus2]
    intersection = len(a & b)
    union = len(a) + len(b) - intersection
    return float(intersection)/float(union)


start = time.time()
inputFile = sys.argv[1]
testFile = sys.argv[2]
caseId = int(sys.argv[3])
outputFile = sys.argv[4]
sc = SparkContext()
trainDataRdd = sc.textFile(inputFile,minPartitions=None, use_unicode=False)
trainDataRdd = trainDataRdd.mapPartitions(lambda x : csv.reader(codecs.iterdecode(x, 'utf-8')))
header1 = trainDataRdd.first()
trainDataRdd = trainDataRdd.filter(lambda x : x != header1)
totalData = trainDataRdd.collect()
users = {}
business_id = {}
k = 0
for i in range(len(totalData)):
    users.update({totalData[i][0]:k+1})
    business_id.update({totalData[i][1]:k+1})
    k = k+1

trainDataRdd = trainDataRdd.map(lambda x : ((users.get(x[0]), business_id.get(x[1])), float(x[2])))
testDataRdd = sc.textFile(testFile, minPartitions=None, use_unicode=False)
testDataRdd = testDataRdd.mapPartitions(lambda x : csv.reader(codecs.iterdecode(x, 'utf-8')))
header2 = testDataRdd.first()
testDataRdd = testDataRdd.filter(lambda x : x != header2)
testDataRdd = testDataRdd.map(parse1)
x_test = testDataRdd.map(lambda p: (p[0], p[1]))



if caseId == 1:
    #print("Hello")
    trainingRdd = trainDataRdd.map(lambda x : Rating(x[0][0], x[0][1], x[1]))
    testDataRdd = testDataRdd.map(lambda x : (x[0][0], x[0][1]))
    random.seed(10)
    lambda_ = 0.2
    model = ALS.train(trainingRdd, rank = 8, iterations = 8, lambda_ = lambda_)
    predictions = model.predictAll(testDataRdd).map(lambda r: ((r[0], r[1]), r[2]))
    outFile = open(outputFile, "w+")
    predictionsList = predictions.collect()
    key_list = list(users.keys())
    val_list = list(users.values())
    bus_key_list = list(business_id.keys())
    bus_val_list = list(business_id.values())
    outFile.write("user_id,business_id,prediction" + "\n")
    for prediction in predictionsList:
        outFile.write(str(key_list[val_list.index(prediction[0][0])]) + ", " + str(bus_key_list[bus_val_list.index(prediction[0][1])]) + ", " + str(prediction[1]) + "\n")
    outFile.close()
    ratesAndPreds = x_test.join(predictions)
    differences = ratesAndPreds.map(lambda x: abs(x[1][0]-x[1][1])).collect()
    diff = sc.parallelize(differences)
    rmse = math.sqrt(diff.map(lambda x: x*x).mean())
    #print("RMSE: ", str(rmse))
    end = time.time()
    #print("Time: ", end - start, " sec")
elif caseId == 2:
    #prepare rdd for test and train
    trainingRdd = trainDataRdd.map(lambda x : (x[0][0], (x[0][1], x[1])))
    testDataRdd = testDataRdd.map(lambda x : (x[0][0], x[0][1])).sortByKey()
    #collect dict of all users, all business and user and business pair
    userBusinessDict = trainingRdd.map(lambda x : ((x[0], x[1][0]), x[1][1])).sortByKey().collectAsMap()       #dict : key : (user,business) : Ratings
    user_dict = trainingRdd.groupByKey().sortByKey().mapValues(list).collectAsMap()                           #dict : key : users ,value [list of business and ratings]
    business_dict = trainingRdd.map(lambda x : (x[1][0], x[0])).groupByKey().sortByKey().mapValues(list).collectAsMap()# dict containing business as key and list of users and ratings
    busAvg = trainingRdd.map(lambda x:(x[1][0],float(x[1][1]))).groupByKey().mapValues(list).map(bus_avg).collectAsMap()
    foundSameUsersForPrediction = testDataRdd.map(lambda x : (x[0], x[1], findWeightofUsers(x[0], x[1])))
    predictions = foundSameUsersForPrediction.map(lambda x: (x[0], x[1], predictAll(x[0], x[1], x[2])))
    predictionsList = predictions.collect()
    outFile = open(outputFile, "w")
    outFile.write("user_id,business_id,prediction" + "\n")

    key_list = list(users.keys())
    val_list = list(users.values())

    bus_key_list = list(business_id.keys())
    bus_val_list = list(business_id.values())
    for p in predictionsList:
        if p[1] in bus_val_list and p[0] in val_list:
            outFile.write(str(key_list[val_list.index(p[0])]) + ", " + str(bus_key_list[bus_val_list.index(p[1])]) + ", " + str(p[2]) + "\n")
        
    outFile.close()
    results = predictions.map(lambda x : ((x[0], x[1]), x[2])).join(x_test)
    differences = results.map(lambda x: abs(x[1][0]-x[1][1]))
    rmse = math.sqrt(differences.map(lambda x: x**2).mean())
    #print("RMSE: ", str(rmse))
    end = time.time()
    #print("Time: ", end - start, " sec")
elif caseId == 3:
    trainingRdd = trainDataRdd.map(lambda x : (x[0][0], (x[0][1], x[1])))   #(455490, (455405, 5.0))
    testDataRdd = testDataRdd.map(lambda x : (x[0][0], x[0][1])).sortByKey()
    user_dict = trainingRdd.groupByKey().sortByKey().mapValues(list).collectAsMap()  # list of all business and ratings per user. (455405, 5.0)
    userBusinessDict = trainingRdd.map(lambda x : ((x[0], x[1][0]), x[1][1])).sortByKey().collectAsMap()  # dict : Keu:  pair of user and business. value : give rating
    business_dict = trainingRdd.map(lambda x : (x[1][0], (x[0], x[1][1]))).groupByKey().sortByKey().mapValues(list).collectAsMap() # dict : list of users and ratings for a business
    # Prediction without LSH
    t2_start = time.time()
    getSameBusItems = testDataRdd.map(lambda x : (x[0], x[1], findWeightOfItemsWithoutLSH(x[0], x[1])))
    predictionsWithoutLSH = getSameBusItems.map(lambda x: (x[0], x[1], predictItemsBased(x[0], x[1], x[2])))
    predictionsList = predictionsWithoutLSH.collect()
    outFile = open(outputFile, "w")
    outFile.write("user_id,business_id,prediction" + "\n")
    key_list = list(users.keys())
    val_list = list(users.values())
    bus_key_list = list(business_id.keys())
    bus_val_list = list(business_id.values())
    for p in predictionsList:
        if p[1] in bus_val_list and p[0] in val_list:
            outFile.write(str(key_list[val_list.index(p[0])]) + ", " + str(bus_key_list[bus_val_list.index(p[1])]) + ", " + str(p[2]) + "\n")
    
    outFile.close()
    results = predictionsWithoutLSH.map(lambda x : ((x[0], x[1]), x[2])).join(x_test)
    differences = results.map(lambda x: abs(x[1][0]-x[1][1]))
    rmseWithoutLSH = math.sqrt(differences.map(lambda x: x**2).mean())
    t2_end = time.time()
    #print("RMSE: ", str(rmseWithoutLSH))
    #print("Time to predict without LSH: ", t2_end - start, " sec")
elif caseId==4:
    start_1 = time.time()
    trainingRdd = trainDataRdd.map(lambda x : (x[0][0], (x[0][1], x[1])))
    testDataRdd = testDataRdd.map(lambda x : (x[0][0], x[0][1])).sortByKey()
    user_dict = trainingRdd.groupByKey().sortByKey().mapValues(list).collectAsMap()  # list of all business and ratings per user. (455405, 5.0)
    userBusinessDict = trainingRdd.map(lambda x : ((x[0], x[1][0]), x[1][1])).sortByKey().collectAsMap()  # dict : Keu:  pair of user and business. value : give rating
    business_dict = trainingRdd.map(lambda x : (x[1][0], (x[0], x[1][1]))).groupByKey().sortByKey().mapValues(list).collectAsMap() # dict : list of users and ratings for a business
    bands = 40
    numRows = 3
    bins = 6
    numOfHash = bands * numRows
    rdd = trainDataRdd.map(lambda x : (x[0][0],x[0][1]))
    businessUserRdd = rdd.map(lambda x: (int(x[1]), int(x[0])))
    busUserRdd = businessUserRdd.map(lambda x: (x[0], [x[1]])).reduceByKey(lambda a, b: a + b)
    finalRddd = busUserRdd.collectAsMap()
    busUserRdd = busUserRdd.map(lambda x: (x[0], list(x[1])))
    signMat = busUserRdd.map(getSignMat).collectAsMap()
    start = 0
    end = 3
    candidatePairsEachBand = defaultdict()
    bandDictForEachBand = defaultdict()
    buckets = 301
    eachBucket = []
    for band in range(bands):
        for x,y in signMat.items():
            temp= signMat[x]
            eachRow= temp[start:end]
            tempSum =0
            for each in eachRow:
                tempSum += each*3
            sumOfTemp = tempSum % 101
            if(sumOfTemp in bandDictForEachBand.keys()):
                temp = bandDictForEachBand[sumOfTemp]
                temp.append(tuple((x,eachRow)))
                bandDictForEachBand[sumOfTemp] = temp
            else:
                temp = []
                temp.append(tuple((x,y)))
                bandDictForEachBand[sumOfTemp] = temp
        for each in bandDictForEachBand.keys():
            callTogetCandiPairs(bandDictForEachBand[each])
            bandDictForEachBand[each] = []
        start = start + 3
        end = end+3
    totalFullData =dict()
    for each in candidatePairsEachBand.keys():
        lol1 = set(finalRddd[each[0]])
        lol2 = set(finalRddd[each[1]])
        tot = len(lol1 & lol2)/float(len(lol1|lol2))
        if(tot>= 0.5):
            totalFullData[tuple((each[0], each[1]))] = tot
    CandidatePairs = totalFullData.keys() #finalPairs 
    #t1_start = time.time()
    similarPairs1 = sc.parallelize(CandidatePairs).map(lambda x : (x[0], x[1])).groupByKey().sortByKey().mapValues(list).collectAsMap()
    similarPairs2 = sc.parallelize(CandidatePairs).map(lambda x : (x[1], x[0])).groupByKey().sortByKey().mapValues(list).collectAsMap()
    sameBusinessWithLSH = testDataRdd.map(lambda x : (x[0], x[1], findWeightOfItemsWithLSH(x[0], x[1])))
    predictions = sameBusinessWithLSH.map(lambda x: (x[0], x[1], predictItemsBased(x[0], x[1], x[2])))
    predictionsList = predictions.collect()
    outFile = open(outputFile, "w")
    outFile.write("user_id,business_id,prediction" + "\n")
    key_list = list(users.keys())
    val_list = list(users.values())
    bus_key_list = list(business_id.keys())
    bus_val_list = list(business_id.values())
    for p in predictionsList:
        if p[1] in bus_val_list and p[0] in val_list:
            outFile.write(str(key_list[val_list.index(p[0])]) + ", " + str(bus_key_list[bus_val_list.index(p[1])]) + ", " + str(p[2]) + "\n")
    outFile.close()
    results = predictions.map(lambda x : ((x[0], x[1]), x[2])).join(x_test)
    differences = results.map(lambda x: abs(x[1][0]-x[1][1]))
    rmseWithLSH = math.sqrt(differences.map(lambda x: x**2).mean())
    t1_end = time.time()
    #print("RMSE: ", str(rmseWithLSH))
    #print("Time to predict with LSH: ", t1_end - start_1, " sec")
    
    
    

    


    



