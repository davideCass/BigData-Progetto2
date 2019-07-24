import os
import requests
from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import StreamMappers_Reducers as mr
from pymongo import MongoClient
import subprocess
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.2.1 pyspark-shell'

currentDate = ''
mapsDict = {}

def printer(rdd):
    for x in rdd.collect():
		print(x)

def sendToDashboardOperators(rdd):
    operators = []
    values = [[],[]]
    for x in rdd.collect():
        operators.append(x[0])
        kills, dead, roundWon, totRounds = x[1][0][0], x[1][0][1], x[1][0][2], x[1][0][3]
        if(dead != 0):
            values[0].append(round((kills / dead), 2))
        else:
            values[0].append(kills)
        if(totRounds != 0):
            values[1].append(round((roundWon / totRounds), 2))
        else:
            values[1].append(roundWon)
    url = 'http://localhost:5001/updateData'
    request_data = {'label': str(operators), 'data': str(values), 'gameMode': str([]), 'divisions': str([]),
                    'label2': str([]), 'data2': str([]), 'maps': str([]), 'platform': str([]),
                    'currentDate': str([]), 'lineLabels': str([]), 'lineData': str([])}
    response = requests.post(url, data=request_data)

def sendToDashboardGameMode(rdd):
    gameMode = []

    for x in rdd.collect():
        # ((gameMode,role), (map, totRoundWon, roundWonOnMap))
        gameMode.append([x[0][0], x[0][1], x[1][1], x[1][0], x[1][2]])
    #print(divisions)
    url = 'http://localhost:5001/updateData'
    request_data = {'label': str([]), 'data': str([]), 'gameMode': str(gameMode), 'divisions': str([]),
                    'label2': str([]), 'data2': str([]), 'maps': str([]), 'platform': str([]),
                    'currentDate': str([]), 'lineLabels': str([]), 'lineData': str([])}
    response = requests.post(url, data=request_data)

def sendToDashboardMaps(rdd):
    global mapsDict, count
    mapsSend = []
    map = []
    # ((map, operator), (role, kills, isdead, haswon, totRounds))
    for x in rdd.collect():
        sumKills = x[1][1]
        sumDead = x[1][2]
        sumRoundWon = x[1][3]
        totRound = x[1][4]
        if(mapsDict.get(x[0][0] + x[0][1]) == None):
            if (sumDead != 0):
                killRatio = round((sumKills / sumDead), 2)
            else:
                killRatio = sumKills
            if (totRound != 0):
                roundRatio = round((sumRoundWon / totRound), 2)
            else:
                roundRatio = x[1][3]
            mapsDict[x[0][0] +  x[0][1]] = [x[1][0], sumKills, sumDead, killRatio, sumRoundWon, totRound, roundRatio]
        else:
            y = mapsDict.get(x[0][0] + x[0][1])
            sumKills = y[1] + x[1][1]
            sumDead = y[2] + x[1][2]
            roundWon = y[4] + x[1][3]
            totRound = y[5] + x[1][4]
            if (sumDead != 0):
                killRatio = round((sumKills / sumDead), 2)
            else:
                killRatio = sumKills
            if (totRound != 0):
                roundRatio = round((sumRoundWon / totRound), 2)
            else:
                roundRatio = x[1][3]
            mapsDict[x[0][0] + x[0][1]] = [x[1][0], sumKills, sumDead, killRatio, roundWon, totRound, roundRatio]
        list = [x[0][0], x[0][1]]
        for z in mapsDict.get(x[0][0] + x[0][1]):
            list.append(z)
        map.append(list)
        map.sort(key = lambda x: x[5] / 2 + x[8], reverse=True)
    attacker = None
    defender = None
    findAtt = 0
    findDef = 0
    for y in map:
        if(y[2] == "Attacker" and findAtt == 0):
            attacker = y
            findAtt = 1
        if(y[2] == "Defender" and findDef == 0):
            defender = y
            findDef = 1
        if(attacker != None and defender != None):
            break

    if(attacker != None and defender != None):
        mapsSend.append(attacker)
        mapsSend.append(defender)
    print(currentDate)
    url = 'http://localhost:5001/updateData'
    request_data = {'label': str([]), 'data': str([]), 'gameMode': str([]), 'divisions': str([]),
                    'label2': str([]), 'data2': str([]), 'maps': str(mapsSend), 'platform': str([]),
                    'currentDate': str([]), 'lineLabels': str([]), 'lineData': str([])}
    response = requests.post(url, data=request_data)

#
# ======================================================================================================================
#

def updateFunction(newValues, lastValues):
    if(lastValues == None):
        lastValues = [(0,0,0,0)]
        return [(newValues[0][0], newValues[0][1], newValues[0][3],
                 newValues[0][4])]
    elif(newValues != []):
        kills = newValues[0][0] + lastValues[0][0]
        dead = newValues[0][1] + lastValues[0][1]
        roundWon = newValues[0][3] + lastValues[0][2]
        totRounds = newValues[0][4] + lastValues[0][3]
        return [(kills, dead, roundWon, totRounds)]
    else:
        return lastValues


def save(rdd):
    global currentDate, count
    client = MongoClient('mongodb://localhost:27017/')
    db = client.mydatabase
    collection = db.lines
    for x in rdd.collect():
        count = count + 1
        if(x != None):
            if(currentDate == ''):
                currentDate = x[0][0]
            if (x[0][0] != currentDate):

                sub = subprocess.Popen('python3 ./batchJobs.py --date {} > result/{}.txt'.format(currentDate, currentDate),                #Popen permette di eseguire il comando senza dover aspettare che finisca
                                    stdout = subprocess.PIPE, stderr=subprocess.STDOUT,
                                    shell = True)
                currentDate = x[0][0]
        # (matchId, (gameMode, map, roundNB, winrole, roundDuration, clearanceLevel, role, haswon, operator, kills, isdead, primaryWeapType))
            dict = {"date": x[0][0], "matchId": x[0][1], "gameMode": x[1][0], "map": x[1][1], "roundNB": x[1][2], "winrole": x[1][3],
            "roundDuration": x[1][4], "clearanceLevel": x[1][5], "role": x[1][6], "hasWon": x[1][7], "operator": x[1][8], "kills": x[1][9],
            "isdead": x[1][10], "primaryWeapType": x[1][11], "platform": x[1][12]}
            collection.insert(dict)
client = MongoClient('mongodb://localhost:27017/')
db = client.mydatabase
collection = db.lines
collection.drop()

conf = SparkConf().setAppName("prova").set("spark.ui.enabled", 'false')
sc = SparkContext.getOrCreate()

ssc = StreamingContext(sc,1)       # (sparkContext, batchDuration)

kafkaParams = {'metadata.broker.list': 'localhost:9092'}
kafkaStream = KafkaUtils.createDirectStream(ssc, ['numtest'], kafkaParams)


linesToMongo = kafkaStream.map(mr.saveToMongo)

linesToMongo.foreachRDD(lambda rdd: save(rdd))

operators = kafkaStream.map(mr.mapStreamOperators).reduceByKey(mr.reduceStreamOperators)

gameMode = kafkaStream.map(mr.map1StreamGameMode).reduceByKey(mr.reduce1StreamGameMode).map(mr.map2StreamGameMode)


maps = kafkaStream.map(mr.mapper1Map).filter(lambda x: x != None).reduceByKey(mr.reducer1Map)
ssc.checkpoint("checkpoint/")
operators2 = operators.updateStateByKey(updateFunction)

maps.foreachRDD((lambda x: printer(x)))

operators2.foreachRDD(sendToDashboardOperators)
gameMode.foreachRDD(sendToDashboardGameMode)
maps.foreachRDD(sendToDashboardMaps)

ssc.start()
ssc.awaitTermination()

