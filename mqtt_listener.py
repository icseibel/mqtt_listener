import paho.mqtt.client as mqtt
import ssl
import datetime

from mongo_util import MongoConnection


class MqttListener():

    def __init__(self):
        self.client = mqtt.Client()
        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message
        self.mongo_conn = MongoConnection()

    # The callback for when the client receives a CONNACK response from the server.
    def on_connect(self, client, userdata, flags, rc):
        if rc == 0:
            print("Connected successfully")
        else:
            print("Connect returned result code: " + str(rc))
    
    # The callback for when a PUBLISH message is received from the server.
    def on_message(self, client, userdata, msg):
        print("Received message: " + msg.topic + " -> " + msg.payload.decode("utf-8"))
        if msg.topic == 'temp':
            post = {"datetime": datetime.datetime.utcnow(),
                    "value": int(msg.payload.decode("utf-8"))}
            post_id = self.mongo_conn.post(collection_name='temp', post_content=post)
            print(post_id)
        if msg.topic == 'humidity':
            post = {"datetime": datetime.datetime.utcnow(),
                    "value": int(msg.payload.decode("utf-8"))}
            post_id = self.mongo_conn.post(collection_name='humidity', post_content=post)
            print(post_id)

    def connect(self, username="gxqjecpa", password="_Ei5OMeV58TL", host="m24.cloudmqtt.com", port=10901):
        self.client.username_pw_set(username=username, password=password)
        self.client.connect(host=host, port=port)

    def subscribe(self, topic):
        self.client.subscribe(topic)

    def start_mqtt_listener(self):
        self.connect()
        self.subscribe(topic='temp')
        self.subscribe(topic='humidity')
        self.client.loop_forever()
        
