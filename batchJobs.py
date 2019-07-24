from pymongo import MongoClient
from pyspark import SparkContext
from pyspark import SparkConf
import argparse
import datetime
import time
import requests
import os


parser = argparse.ArgumentParser()
parser.add_argument('--date' , type=str)
args = parser.parse_args()
currentDate = '{}'.format(args.date)


def parseDataset(line):
    line = list(line.values())
    matchId, gameMode, map, roundNB, winrole, roundDuration, clearanceLevel, \
    role, hasWon, operator, kills, isdead, primaryWeapType, platform = line[2:]

    return (matchId, gameMode, map, int(roundNB), winrole, int(roundDuration), int(clearanceLevel), \
            role, int(hasWon), operator, int(kills), int(isdead), primaryWeapType, platform)

#
# Tutti i Job si basano su analisi effettuate sull'intera giornata
#
# ======================================================================================================================
#  JOB 1 : Le prime 5 divisioni della giornata con il punteggio più alto (assegnando 5 punti per ogni uccisione
#           e 15 punti per ogni round vinto)
#
# ======================================================================================================================
#

def mapper1Divisions(line):
    matchId, gameMode, map, roundNB, winrole, roundDuration, clearanceLevel, \
    role, hasWon, operator, kills, isdead, primaryWeapType, platform = line[0:]
    division = operator.split("-")[0]

    return (division, (int(kills) * 5 + int(hasWon) * 15, clearanceLevel, 1, roundDuration))

def reducer1Divisions(lineA, lineB):
    points = lineA[0] + lineB[0]
    sumLevel =lineA[1] + lineB[1]
    countLevel = lineA[2] + lineB[2]
    timeDivision = lineA[3] + lineB[3]

    return points, sumLevel, countLevel, timeDivision

def mapper2Divisions(line):
    division = line[0]
    points, sumLevel, countLevel, timeDivision = line[1][0:]
    avgLevel = round(sumLevel / countLevel, 2)
    #minutes, seconds = timeDivision // 60, timeDivision % 60
    timeInHMS = '{:02}:{:02}:{:02}'.format(timeDivision//3600, timeDivision%3600//60, timeDivision%60)

    return (division, (points, avgLevel, timeInHMS))


#
# ======================================================================================================================
# JOB 2: trovare i primi 3 operatori con il ratio uccisioni e ratio round più alto della giornata e totale round giocati
# ======================================================================================================================
#

def mapper1Operators(line):

    matchId, gameMode, map, roundNB, winrole, roundDuration, level, \
    role, haswon, operator, kills, isdead, primaryWeapType, platform = line[0:]
    killRatio = 0
    totRounds = 1
    roundRatio = 0

    return (operator, (kills, isdead, killRatio, haswon, totRounds, roundRatio, role))

def reducer1Operators(lineA,lineB):
    sumKills = lineA[0] + lineB[0]
    sumDead = lineA[1] + lineB[1]
    nbRoundWon = lineA[3] + lineB[3]
    totRounds = lineA[4] + lineB[4]
    role = lineA[6]
    if(sumDead != 0):
        killRatio = sumKills / sumDead
    else:
        killRatio = sumKills
    if(totRounds != 0):
        roundRatio = nbRoundWon / totRounds
    else:
        roundRatio = nbRoundWon

    # killRatio è calcolato come divisione tra le uccisioni totali (somma delle uccisioni di tutti i round in tutte le partite su quella mappa)
    # e le morti totali; roundRatio è calcolato come divisione tra le vittorie totali
    # dei round su quella mappa e il numero di round complessivi giocati da quell'operatore su quella mappa
    return sumKills, sumDead, round(killRatio,2), nbRoundWon, totRounds, round(roundRatio, 2), role


def mapper2Operators(line):
    operator = line[0]
    role = line[1][6]
    killRatio = line[1][2]
    roundRatio = line[1][5]
    totRounds = line[1][4]

    return (role, (operator, killRatio, roundRatio, totRounds))

#
# ======================================================================================================================
# JOB 3: Per ogni piattaforma calcolare round totali giocati, tempo totale di gioco (per ogni round deve essere lo
#       stesso per tutti gli operatori in quel round) e mappa più giocata (basato sul tempo di gioco)
# ======================================================================================================================
#

def mapper1Platform(line):
    matchId, gameMode, map, roundNB, winrole, roundDuration, level, \
    role, haswon, operator, kills, isdead, primaryWeapType, platform = line[0:]
    totRounds = 1
    totTime = roundDuration

    #print(matchId + " " + map + " " + operator + " " +role + " " +haswon + " " +kills + " " +isdead + " " +roundNB)
    return((platform, map, matchId, roundNB), (totRounds, totTime))

def reducer1Platform(lineA, lineB):
    totRounds = lineA[0]
    totTime = lineA[1]

    return totRounds, totTime

def mapper2Platform(line):
    platform, map = line[0][0:2]
    totRounds, totTime = line[1][0:]

    return ((platform, map), (int(totRounds), int(totTime)))

def reducer2Platform(lineA, lineB):
    totRounds = lineA[0] + lineB[0]
    totTime = lineA[1] + lineB[1]

    return totRounds, totTime

def finalMapperPlatform(line):
    platform, map = line[0][0:]
    totRounds, totTime = line[1][0:]

    return (platform, (map, totRounds, totTime, totTime, totTime))

def finalReducerPlatform(lineA, lineB):
    if(lineA[2] > lineB[2]):
        map = lineA[0]
        timeOnMap = lineA[3]
    else:
        map = lineB[0]
        timeOnMap = lineB[3]
    totTime = lineA[2] + lineB[2]
    totRounds = lineA[1] + lineB[1]
    timeInHMS = '{:02}:{:02}:{:02}'.format(totTime//3600, totTime%3600//60, totTime%60)

    return map, totRounds, totTime, timeOnMap, timeInHMS

#
# ======================================================================================================================
#                                              SEND TO DASHBOARD
# ======================================================================================================================
#

# JOB 1: Le prime 5 divisioni della giornata con il punteggio più alto (assegnando 5 punti per ogni uccisione
#           e 15 punti per ogni round vinto)
# (division, (points, avgLevel, time, minutes, seconds))

# TABLE
def sendToDashboardDivisions(rdd):
    divisions = []
    for x in rdd:
        divisions.append([x[0], x[1][0], x[1][1], x[1][2]])
    url = 'http://localhost:5001/updateData'
    request_data = {'label': str([]), 'data': str([]), 'gameMode': str([]), 'divisions': str(divisions),
                    'label2': str([]), 'data2': str([]), 'maps': str([]), 'platform': str([]),
                    'currentDate': currentDate, 'lineLabels': str([]), 'lineData': str([])}
    response = requests.post(url, data=request_data)

# JOB 2: trovare i primi 3 operatori con il ratio uccisioni e ratio round più alto della giornata e totale round giocati
# (role, (operator, killRatio, roundRatio, totRounds))

# BAR CHART
def sendToDashboardOperators(rdd):
    label = []
    values = [[],[],[]]
    for x in rdd:
        label.append(x[1][0])
        #print(x)
        values[0].append(x[1][1])
        values[1].append(x[1][2])
        values[2].append(x[1][3])

    url = 'http://localhost:5001/updateData'
    request_data = {'label': str([]), 'data': str([]), 'gameMode': str([]), 'divisions': str([]),
                    'label2': str(label), 'data2': str(values), 'maps': str([]), 'platform': str([]),
                    'currentDate': currentDate, 'lineLabels': str([]), 'lineData': str([])}
    response = requests.post(url, data=request_data)


# JOB 3: Per ogni piattaforma calcolare round totali giocati, tempo totale di gioco (per ogni round deve essere lo
#       stesso per tutti gli operatori in quel round) e mappa più giocata (basato sul tempo di gioco)
# (platform, (map, totRounds, totTime, timeOnMap, timeInHMS))

# TABLE E LINE CHART
def sendToDashboardPlatform(rdd):
    platform = []
    lineLabels = []
    pc, ps4, xbox = '','',''
    for x in rdd.collect():
        platform.append([x[0], x[1][0], x[1][1], str(x[1][4])])
        if(x[0] == 'PC'):
            pc = x[1][1]
        elif(x[0] == 'PS4'):
            ps4 = x[1][1]
        else:
            xbox = x[1][1]
    lineData = [pc, ps4, xbox]
    url = 'http://localhost:5001/updateData'
    request_data = {'label': str([]), 'data': str([]), 'gameMode': str([]), 'divisions': str([]),
                    'label2': str([]), 'data2': str([]), 'maps': str([]), 'platform': str(platform),
                    'currentDate': currentDate, 'lineLabels': str(lineLabels), 'lineData': str(lineData)}
    response = requests.post(url, data=request_data)

#
# ======================================================================================================================
#

print(currentDate)

startTime = datetime.datetime.now()

client = MongoClient('mongodb://localhost:27017/')
db = client.mydatabase
collection = db.lines

dataset = list(collection.find({"date": currentDate}))
for x in dataset:
    dict.update(x)

sc = SparkContext.getOrCreate()

dataset = sc.parallelize(dataset).map(parseDataset)

divisions = dataset.map(mapper1Divisions).reduceByKey(reducer1Divisions)\
    .map(mapper2Divisions).takeOrdered(5, lambda x: -x[1][0])


operators = dataset.map(mapper1Operators).filter(lambda x: x != None).reduceByKey(reducer1Operators).map(mapper2Operators)\
    .takeOrdered(3, lambda x: -(x[1][1] / 3 + x[1][2]))#.reduceByKey(reducer2Operators).sortByKey().map(finalMapperOperators).reduceByKey(finalReducerOperators).sortByKey()


platform = dataset.map(mapper1Platform).filter(lambda x: x != None).reduceByKey(reducer1Platform)\
    .map(mapper2Platform).reduceByKey(reducer2Platform).map(finalMapperPlatform).reduceByKey(finalReducerPlatform)


for x in divisions:
    print("Divisione: " + x[0] + ", ", "Punti: " + str(x[1][0]) + ", ", "Livello medio giocatori: " + str(x[1][1]) + ", ",
    "Tempo totale giocato: " + str(x[1][2]))
print("")

for x in operators:
    print("Operatore: " + x[1][0] + ", ", "Ruolo: " + x[0] + ", ", "killRatio: " + str(x[1][1]) + ", ",
          "roundRatio: " + str(x[1][2]) + ", ", "Round totali giocati: " + str(x[1][3]))
print("")


for x in platform.collect():
    print("Piattaforma: " + x[0] + ", ", "Mappa più giocata: " + x[1][0] + ", ", "Totale round giocati: " + str(x[1][1])
          + ", ", "Tempo di gioco totale: " + str(x[1][4]) )

sendToDashboardDivisions(divisions)
sendToDashboardOperators(operators)
sendToDashboardPlatform(platform)


endTime = datetime.datetime.now()
print("\n" + str(endTime - startTime))
exit()