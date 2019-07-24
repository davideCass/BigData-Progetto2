
def saveToMongo(line):
    string = "[],'"
    line = line[1]
    for s in string:
        line = line.replace(s, "")
    line = line.replace('"', "")
    line = line.split(", ")[0].split(" ")
    date, matchid, gameMode, map, roundNB, winrole, roundDuration, clearanceLevel, role, haswon, operator, \
    kills, isdead, primaryWeapType, platform = line[0:]

    # (matchId, (gameMode, map, roundNB, winrole, roundDuration, clearanceLevel, role, haswon, operator, kills, isdead, primaryWeapType))
    return ((date, matchid), (gameMode, map, int(roundNB), winrole, int(roundDuration), int(clearanceLevel), role,
                              int(haswon), operator, int(kills), int(isdead), primaryWeapType, platform))

#
# ======================================================================================================================
#
#
# ======================================================================================================================
# JOB1: per ogni operatore calcolo del killRatio e roundRatio aggiornato in tempo reale
# ======================================================================================================================
#

def mapStreamOperators(line):
    string = "[],'"
    line = line[1]
    for s in string:
        line = line.replace(s, "")
    line = line.replace('"', "")
    line = line.split(", ")[0].split(" ")
    matchid, gameMode, map, roundNB, winrole, roundDuration, clearanceLevel, role, haswon, operator, kills, isdead, primaryWeapType, platform = line[1:]
    operator = operator.split("-")[1]
    killRatio = kills
    roundRatio = haswon
    totRounds = 1
    # (matchId, (gameMode, map, roundNB, winrole, roundDuration, clearanceLevel, role, haswon, operator, kills, isdead, primaryWeapType))
    #kills, isdead, killRatio, haswon, totRounds, roundRatio
    return (operator), (int(kills), int(isdead), float(killRatio), int(haswon), totRounds, float(roundRatio))

def reduceStreamOperators(lineA, lineB):
    sumKills = lineA[0] + lineB[0]
    sumDead = lineA[1] + lineB[1]
    nbRoundWon = lineA[3] + lineB[3]
    if(sumDead != 0):
        killRatio = sumKills / sumDead
    else:
        killRatio = sumKills
    totRounds = lineA[4] + lineB[4]
    if(totRounds != 0):
        roundRatio = nbRoundWon / totRounds
    else:
        roundRatio = nbRoundWon

    return sumKills, sumDead, round(killRatio,2), nbRoundWon, totRounds, round(roundRatio, 2)

#
# ======================================================================================================================
# JOB2: per ogni modalità di gioco calcola vittorie attaccanti e difensori in tempo reale e la mappa in cui i 2 ruoli
#       vincono di più
# ======================================================================================================================
#

def map1StreamGameMode(line):
    string = "[],'"
    line = line[1]
    for s in string:
        line = line.replace(s, "")
    line = line.replace('"', "")
    line = line.split(", ")[0].split(" ")
    matchid, gameMode, map, roundNB, winrole, roundDuration, clearanceLevel, role, haswon, operator, kills, \
    isdead, primaryWeapType, platform = line[1:]


    return ((gameMode, role, map), (int(haswon)))

def reduce1StreamGameMode(lineA, lineB):
    hasWon = lineA + lineB

    return hasWon

def map2StreamGameMode(line):
    gameMode, role, map = line[0][0:]
    roundWon = line[1]

    return((gameMode, role), (map, roundWon, roundWon))

def reduce2StreamGameMode(lineA, lineB):
    print("lineA: " + lineA)
    print("lineB: " + lineB)
    if(lineA[1][2] > lineB[1][2]):
        map = lineA[1][0]
        roundWonOnMap = lineA[1][2]
    else:
        map = lineB[1][0]
        roundWonOnMap = lineB[1][2]
    totRoundWon = lineA[1][1] + lineB[1][1]

    return map, totRoundWon, roundWonOnMap

#
# ======================================================================================================================
# JOB 3: trovare, per ciascuna mappa, l'operatore attaccante e difensore con il ratio uccisioni e ratio round più alti
# ======================================================================================================================
#

def mapper1Map(line):
    string = "[],'"
    line = line[1]
    for s in string:
        line = line.replace(s, "")
    line = line.replace('"', "")
    line = line.split(", ")[0].split(" ")
    matchid, gameMode, map, roundNB, winrole, roundDuration, clearanceLevel, role, haswon, operator, kills, \
    isdead, primaryWeapType, platform = line[1:]
    totRounds = 1

    #print(matchId + " " + map + " " + operator + " " +role + " " +haswon + " " +kills + " " +isdead + " " +roundNB)
    return ((map, operator), (role, int(kills), int(isdead), int(haswon), totRounds))

def reducer1Map(lineA,lineB):
    sumKills = lineA[1] + lineB[1]
    sumDead = lineA[2] + lineB[2]
    nbRoundWon = lineA[3] + lineB[3]
    role = lineA[0]
    totRounds = lineA[4] + lineB[4]

    # killRatio è calcolato come divisione tra le uccisioni totali (somma delle uccisioni di tutti i round in tutte le partite su quella mappa)
    # e le morti totali; roundRatio è calcolato come divisione tra le vittorie totali
    # dei round su quella mappa e il numero di round complessivi giocati da quell'operatore su quella mappa
    return role, sumKills, sumDead, nbRoundWon, totRounds
