import requests, calendar
from datetime import datetime
from time import sleep
import os
import pandas as pd

userdata = pd.read_json("~/kite_candles/userdata")

api_key = userdata['YOURNAME'].apikey


filehandle = open(os.path.join(os.path.expanduser("~/kite_candles"), "instruments.csv"), "w")

while True:
    
    print("https://api.kite.trade/instruments?api_key={}".format(api_key))
    req = requests.get("https://api.kite.trade/instruments?api_key={}".format(api_key))

    if len(req.content.decode("utf-8")) > 2048:

        break

    sleep(15)



filehandle.write(req.content.decode("utf-8"))

filehandle.close()
