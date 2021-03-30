from mqtt_listener import MqttListener
import os
import threading

from flask import Flask


app = Flask(__name__)


@app.route("/")
def index():
    return "<h1>Hello World</hi>"

def call_script():
    mqtt_listener = MqttListener()
    mqtt_listener.start_mqtt_listener()    

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    th = threading.Thread(target=call_script, name="Thread-mqtt")
    th.start()
    app.run(host='0.0.0.0', port=port)

    
