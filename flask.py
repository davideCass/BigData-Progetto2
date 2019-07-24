from flask import Flask,jsonify,request
from flask import render_template
import ast

app = Flask(__name__)

labelsChart = []
valuesChart = [[],[]]
finalLabels = []
finalValues = [[],[]]
gameMode = []
divisions = []
labels2 = []
values2 = []
maps = []
platform = []
lineData = [[],[],[]]
lineLabels = []
currentDate = ''


# "/" viene eseguito una sola volta all'inizio

@app.route("/")
def chart():
    global labels,valuesChart, gameMode, labels2, values2, maps, platform, currentDate, lineLabels, lineData
    labelsChart = []
    valuesChart = [[],[]]

    return render_template('index.html', values=valuesChart, labels=labelsChart, gameMode = gameMode, divisions = divisions,
                           labels2 = labels2, values2 = values2, maps = maps,
                           platform = platform, currentDate = currentDate, lineLabels = lineLabels,
                           lineData = lineData) #, table = table)


@app.route('/refreshData')
def refresh_graph_data():
    global labelsChart, valuesChart, gameMode, divisions, labels2, values2, maps, platform, currentDate, lineLabels, lineData
    print("lineLabels now: " + str(lineLabels))
    print("lineData now: " + str(lineData))
    return jsonify(sLabel=finalLabels, sData1=finalValues[0], sData2 = finalValues[1],
                   gameMode = gameMode, divisions = divisions, labels2 = labels2, values2 = values2,
                   maps = maps, platform = platform, currentDate = currentDate,
                   lineLabels = lineLabels, lineData = lineData)


@app.route('/updateData', methods=['POST'])
def update_data_post():
    global labelsChart, valuesChart, finalLabels, finalValues, gameMode, currentDate, divisions, labels2, values2, maps, platform, lineLabels, lineData
    if not request.form or 'data' not in request.form :
        return "error",400
    labelsChart = ast.literal_eval(request.form['label'])
    valuesChart = ast.literal_eval(request.form['data'])
    gameModeReceveid = ast.literal_eval(request.form['gameMode'])
    divisionsReceived = ast.literal_eval(request.form['divisions'])
    labelsChart2 = ast.literal_eval(request.form['label2'])
    valuesChart2 = ast.literal_eval(request.form['data2'])
    mapsReceived = ast.literal_eval(request.form['maps'])
    platformReceived = ast.literal_eval(request.form['platform'])
    dateReceived = ast.literal_eval(request.form['currentDate'])
    lineLabelsRec = ast.literal_eval(request.form['lineLabels'])
    lineDataRec = ast.literal_eval(request.form['lineData'])
    if(labelsChart != []):
        for x in labelsChart:
            if x not in finalLabels:
                finalLabels.append(x)
                finalValues[0].append(valuesChart[0][labelsChart.index(x)])
                finalValues[1].append(valuesChart[1][labelsChart.index(x)])
            else:
                finalValues[0][finalLabels.index(x)] = valuesChart[0][labelsChart.index(x)]
                finalValues[1][finalLabels.index(x)] = valuesChart[1][labelsChart.index(x)]
                
    if(gameModeReceveid != []):
        if(gameMode == []):
            gameMode = gameModeReceveid
        else:
            for y in gameModeReceveid:
                contains = False
                for x in gameMode:
                    totRoundWon, map, roundWonOnMap = x[2], x[3], x[4]
                    if(x[0] == y[0] and x[1] == y[1]):     # gameMode, role
                        contains = True
                        totRoundWon = x[2] + y[2]
                        if(x[4] > y[4]):     # ruolo
                            map, roundWonOnMap = x[3], x[4]
                        else:
                            map, roundWonOnMap = y[3], y[4]
                    x[2], x[3], x[4] = totRoundWon, map, roundWonOnMap
                if(not contains):
                    gameMode.append(y)

    if(divisionsReceived != []):
        divisions = divisionsReceived
    if(labelsChart2 != []):
        labels2 = labelsChart2
        values2 = valuesChart2
    if(mapsReceived != []):
        for x in mapsReceived:
            maps.append([x[0], x[2], x[1], x[5], x[8]])
    if(platformReceived != []):
        for x in platformReceived:
            platform = platformReceived
        platform.sort(key = lambda x: [x[3].split(":")[0], x[3].split(":")[1], x[3].split(":")[2]], reverse = True)
    print("platformReceived: ")
    print(platformReceived)
    print("platform: ")
    print(platform)
    if(dateReceived != []):
        currentDate = dateReceived
    print(currentDate)
    print(lineDataRec)
    if(lineDataRec != [] and dateReceived != []):
        dateReceived = str(dateReceived)
        print(dateReceived)
        year = dateReceived[0:4]
        month = dateReceived[4:6]
        day = dateReceived[6:]
        date = day + "-" + month + "-" + year
        lineLabels.append(date)
        lineData[0].append(lineDataRec[0])
        lineData[1].append(lineDataRec[1])
        lineData[2].append(lineDataRec[2])
    print("maps received: " + str(mapsReceived))
    #print("data2 received: " + str(valuesChart2))
    #print("gameMode received: " + str(gameMode))

    return "success",201


if __name__ == "__main__":
    app.run(host='localhost', port=5001)
