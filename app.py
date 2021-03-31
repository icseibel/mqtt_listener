from datetime import timedelta
import os
import threading
import datetime
from flask import Flask, jsonify

from mqtt_listener import MqttListener
from mongo_util import MongoConnection


app = Flask(__name__)


@app.route("/")
def index():
    return "<h1>Hello World</hi>"

@app.route("/lasttemp")
def last_temp():
    client = MongoConnection()    
    result = list(client.get_one(collection_name='temp', sort_field='datetime'))
    final= {"datetime":  result[0]['datetime'].strftime("%m/%d/%Y, %H:%M:%S"), "value": result[0]['value']}
    return jsonify(final)
    
@app.route("/groupedbyhour", methods=['GET'])
def get_grouped_by_hour():
    query = [{
    '$match': {
        "datetime": {
            '$gt': datetime.datetime.utcnow() - timedelta(hours=24)
        }
    }
    },
    {
        '$group': {
            '_id': {
                'ano': {
                    '$year': '$datetime'
                }, 
                'mes': {
                    '$month': '$datetime'
                }, 
                'dia': {
                    '$dayOfMonth': '$datetime'
                }, 
                'hora': {
                    '$hour': '$datetime'
                }
            }, 
            'media': {
                '$avg': '$value'
            }, 
            'max': {
                '$max': '$value'
            }, 
            'min': {
                '$min': '$value'
            }
        }
    }, {
        '$sort': {
            '_id': 1
        }
    }
]
    client = MongoConnection()    
    result = list(client.get_aggregated(collection_name='temp',query=query))
    return jsonify(result)

def call_script():
    mqtt_listener = MqttListener()
    mqtt_listener.start_mqtt_listener()    

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    #th = threading.Thread(target=call_script, name="Thread-mqtt")
    #th.start()
    app.run(host='0.0.0.0', port=port)
