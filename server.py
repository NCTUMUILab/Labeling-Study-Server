from flask import Flask, request, jsonify, json
from bson.objectid import ObjectId
from flask_pymongo import PyMongo
import datetime
from datetime import datetime
import dateutil.parser
from bson import json_util

app = Flask(__name__)

app.config['MONGO_DBNAME'] = 'LabellingStudy'
app.config['MONGO_URI'] = 'mongodb://localhost/LabellingStudy'

mongo = PyMongo(app)
@app.route('/find_latest_and_insert', methods=['POST'])
def find_latest_and_insert():

    if request.method == 'POST':
        collection = str(request.args['collection'])
        action = str(request.args['action'])

        if collection=='dump':
            user = mongo.db.dump
        elif collection=='trip':
            user = mongo.db.trip
        elif collection=='isAlive':
            user = mongo.db.isAlive
        elif collection=='sample':
            user = mongo.db.sample

        if action=='search':
            user_id = request.args['id']
            data = user.find({'device_id':user_id})
            res = data.sort('startTime', -1).limit(1)

            docs = []
            for item in res:
                time = item['startTime']
                docs.append({'startTime':item['startTime']})
            message = json.dumps(docs)

            return message
            
        elif action=='insert':
            json_request = request.get_json(force=True, silent=True)
            print (json_request)  # contain _id field
            if collection=='trip':
                data = dict()
                # need to handle update #
                key = json_request.keys()
                for i in key:
                    if i!='_id':
                        data[i] = json_request[i]
                user.update({'_id':json_request['_id']}, {'$set':data}, upsert=True, multi=True)
                return json.dumps({'createdTime':json_request['createdTime']})
            else:
                user.insert(json_request)
                return 'insert OK!'

@app.route('/time_interval', methods=['POST'])
def time_interval():
    jsonquery = request.get_json(force=True, silent=True)  # type: unico$
    query = json.loads(jsonquery)
    d_id = query['device_id']
    collection = query['collection']
    start = query['query_start_time']
    end = query['query_end_time']

    if collection=='dump':
        col = mongo.db.dump
    elif collection=='trip':
        col = mongo.db.trip
    elif collection=='isAlive':
        col = mongo.db.isAlive
    elif collection=='sample':
        col = mongo.db.sample

    json_docs = []
    number = col.find({'device_id':d_id, 'startTime': {'$gte':str(start["$date"])}, 'endTime': {'$lt': str(end["$date"])}}).count()
    in_time_range = col.find({'device_id':d_id, 'startTime': {'$gte':str(start["$date"])}, 'endTime': {'$lt': str(end["$date"])}})
    for doc in in_time_range:
        json_docs.append(doc)
    print (json_docs)
    return json.dumps({'device_id':d_id, "number":number})


if __name__ == '__main__':
    app.run(host="172.31.34.26")
