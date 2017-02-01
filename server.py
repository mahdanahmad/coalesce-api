import os, copy, json, math, itertools


from flask import Flask, Response, request
from datetime import datetime, timedelta
from bson import json_util
from flask.ext.pymongo import PyMongo

app = Flask(__name__)

app.config['MONGO_HOST']        = 'localhost'
app.config['MONGO_PORT']        = 27017
app.config['MONGO_DBNAME']      = 'pulselab_coalesce'
app.config['MONGO_USERNAME']    = ''
app.config['MONGO_PASSWORD']    = ''

mongo   = PyMongo(app)

@app.route('/')
def hello_world():
    return 'Hello, World!'

@app.route('/stacked', methods=['GET'])
def create_stacked():
    collection  = mongo.db.data

    stacked     = {}
    freqstart   = None
    freqend     = None
    datatypes   = ['dataset', 'rows', 'filesize']

    try :
        exclude = json.loads(request.args.get('exclude')) if isinstance(json.loads(request.args.get('exclude')), list) else []
    except :
        exclude = []

    try :
        freqs   = json.loads(request.args.get('frequencies')) if isinstance(json.loads(request.args.get('frequencies')), list) else []
    except :
        freqs   = []

    tag         = request.args.get('tag')

    datasets    = list(collection.find({ 'groups' : { '$nin' : exclude }, 'tags' : { '$in' : [tag] }}))
    for tp in datatypes :
        prevData    = []
        stackedType = []
        for freq in sorted(freqs, key=lambda k : int(k), reverse=True) :
            freqData    = []
            freqtered   = filter(lambda o : o['frequency'] == freq, datasets)

            for data in sorted(freqtered, key=lambda k : (datetime.strptime(k['starttime'], '%Y-%m-%d'), datetime.strptime(k['endtime'], '%Y-%m-%d'))) :
                starttime   = datetime.strptime(data['starttime'], '%Y-%m-%d')
                endtime     = datetime.strptime(data['endtime'], '%Y-%m-%d')

                if (freqend is None) or (freqend < endtime) : freqend = endtime
                if (freqstart is None) or (freqstart > starttime) : freqstart = starttime

                delta       = endtime - starttime
                addition    = 1
                if (tp == 'rows') : addition = data['rowscount'] if data['rowscount'] is not None else 0
                if (tp == 'filesize') : addition = math.ceil(data['filesize'] / 1000) if data['filesize'] is not None else 0

                for i in range(delta.days + 1) :
                    currenttime = (starttime + timedelta(days=i)).strftime("%Y-%m-%d")
                    inside      = filter(lambda o : o['date'] == currenttime, freqData)
                    if (len(inside) == 0) :
                        prevDateData    = filter(lambda o : o['date'] == currenttime, prevData)
                        if (len(prevDateData) == 0) :
                            freqData.append({'y0' : 0, 'y1' : addition, 'date' : currenttime})
                        else :
                            freqData.append({'y0' : prevDateData[0]['y1'], 'y1' : prevDateData[0]['y1'] + addition, 'date' : currenttime})
                    else :
                        inside[0]['y1'] += addition

            stackedType.append({'data' : freqData, 'state' : freq})
            prevData = copy.deepcopy(freqData)

        stacked[tp] = stackedType

    return Response(
        json.dumps(stacked),
        mimetype='application/json'
    )

@app.route('/swimlane', methods=['GET'])
def create_swimlane():
    collection  = mongo.db.data

    try :
        exclude = json.loads(request.args.get('exclude')) if isinstance(json.loads(request.args.get('exclude')), list) else []
    except :
        exclude = []

    datasets    = list(collection.find({ 'groups' : { '$nin' : exclude } }))
    alltags     = set(itertools.chain.from_iterable(map(lambda o : o['tags'], datasets)))
    avail_freq  = set(map(lambda o : o['frequency'], datasets))

    swimlane    = {}
    startDate   = None
    endDate     = None

    for tag in alltags :
        dates       = []
        filtered    = filter(lambda o : tag in o['tags'], datasets)

        for data in sorted(filtered, key=lambda k : (datetime.strptime(k['starttime'], '%Y-%m-%d'), datetime.strptime(k['endtime'], '%Y-%m-%d'))) :
            starttime   = datetime.strptime(data['starttime'], '%Y-%m-%d')
            endtime     = datetime.strptime(data['endtime'], '%Y-%m-%d')

            if starttime == endtime : endtime = endtime + timedelta(days=1)

            if (endDate is None) or (endDate < endtime) : endDate = endtime
            if (startDate is None) or (startDate > starttime) : startDate = starttime

            fallunder   = filter(lambda o : o['start'] <= starttime <= o['end'], dates)
            if (len(fallunder) == 0) :
                dates.append({'start' : starttime, 'end' : endtime})
            elif (fallunder[0]['end'] < endtime) :
                fallunder[0]['end'] = endtime

        swimlane[tag.encode('utf-8')] = map(lambda o : {'start' : o['start'].strftime("%Y-%m-%d"), 'end' : o['end'].strftime("%Y-%m-%d")}, dates)

    return Response(
        json.dumps({'startDate' : startDate.strftime("%Y-%m-%d"), 'endDate' : (endDate + timedelta(days=1)).strftime("%Y-%m-%d"), 'data' : swimlane, 'avail_freq' : list(avail_freq)}),
        mimetype='application/json'
    )
