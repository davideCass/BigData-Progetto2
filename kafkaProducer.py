from time import sleep
from json import dumps
from kafka import KafkaProducer
from pyspark import SparkConf
from pyspark import SparkContext
import Constants as C
import os



def parseLines(line):

    if(line != None):
        line = line.split(",")
        if(line[C.MATCHID] != 'matchid'):
            date = line[C.DATEID]
            matchId = line[C.MATCHID]
            gameMode = line[C.GAMEMODE]
            winrole = line[C.WINROLE]
            roundDuration = line[C.ROUNDDURATION]
            clearanceLevel = line[C.CLEARANCELEVEL]
            primaryWeapType = line[C.PRIMARYWEAPONTYPE]
            map = line[C.MAPNAME]
            operator = line[C.OPERATOR]
            role = line[C.ROLE]
            haswon = int(line[C.HASWON])
            kills = int(line[C.NBKILLS])
            isdead = int(line[C.ISDEAD])
            roundNB = int(line[C.ROUNDNB])
            platform = line[C.PLATFORM]

            return ((date, matchId), (gameMode, map, roundNB, winrole, roundDuration, clearanceLevel, role, haswon, operator, kills, isdead, primaryWeapType, platform))


def run():
    producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda x:
                         dumps(x).encode('utf-8'))

    conf = SparkConf().setAppName("job1").set('spark.driver.memory', '8G').set('spark.driver.maxResultSize', '8G')
    sc = SparkContext(conf = conf)

    linesPC = sc.textFile("dataset/raimbow-dataset.csv")

    output = linesPC.map(parseLines).filter(lambda x: x != None).sortByKey()#.reduceByKey(reducerJob1)#.mapValues(lastMap)

    map = output.collectAsMap()
    i = 0
    for x in output.collect():
        matchid = list(map.keys())[i][1]
        date = list(map.keys())[i][0]

        if(x[0][1] == matchid and x[0][0] == date):
            producer.send('numtest', value = x)
            #print(x)

        elif(x[0][0] != date):
            i += 1
            producer.send('numtest', value = x)
            #print(x)
        else:
            sleep(1)
            i += 1
            producer.send('numtest', value=x)
            #print(x)
run()