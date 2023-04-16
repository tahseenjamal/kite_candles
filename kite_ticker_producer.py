import redis
from kiteconnect import KiteTicker
import _thread, sys, json, os
from datetime import datetime, date, time, timedelta
import calendar
from time import sleep
from queue import Queue
import pandas as pd


df = pd.read_csv("~/kite_candles/instruments.csv", low_memory = False)

userdata = pd.read_json("~/kite_candles/userdata")

redis_client = redis.Redis('localhost')

messageQueue = Queue()
dataQueue = Queue()
write_queue = Queue()

instrumentMap = {}
allinstrumentMap = {}

user_id = userdata['YOURNAME'].user

api_key = userdata['YOURNAME'].apikey

access_token = redis_client.hget("token.{}".format(user_id), "access_token").decode("utf-8")


def read_queue():

    global allinstrumentMap


    while True:

        bulk_ticks = messageQueue.get()

        redis_client.publish('ticks', f'{bulk_ticks}')

def data_queue():

    
    while True:
            
        dataToWrite = dataQueue.get()

        timestamp = dataToWrite.get('timestamp')

        stockname = allinstrumentMap.get(dataToWrite.get('instrument_token'))

        open_price = dataToWrite['ohlc'].get('open',0)
        high_price = dataToWrite['ohlc'].get('high',0)
        low_price = dataToWrite['ohlc'].get('low',0)
        close_price = dataToWrite['ohlc'].get('close',0)

        buy_quantity = dataToWrite.get('buy_quantity',0)
        last_price = dataToWrite.get('last_price',0)
        sell_quantity = dataToWrite.get('sell_quantity',0)
        volume = dataToWrite.get('volume',0)

        redis_client.hset("tick_last", stockname, last_price)
        redis_client.hset("tick_open", stockname, open_price)
        redis_client.hset("tick_high", stockname, high_price)
        redis_client.hset("tick_low", stockname, low_price)
        redis_client.hset("tick_close", stockname, close_price)

        redis_client.hset("tick_buyer", stockname, buy_quantity)
        redis_client.hset("tick_seller", stockname, sell_quantity)



def on_order_update(ws, data):

    global user_id

    try:

        filehandle = open("{}-order.log".format(user_id), "a")
        filehandle.write("{}\n".format(json.dumps(data)))
        filehandle.close()

    except:
        pass
    

def on_ticks(ws , ticks):

    dt = datetime.now()

    if dt.time() >= time(9,0) and dt.time() < time(15,31):

        messageQueue.put(ticks)

    else:
        
        print("exiting")
        kws.close()
        os._exit(0)





def on_connect(ws, response):
    
    global df, instrumentMap, allinstrumentMap

    weekday = datetime.now().weekday()

    expiry = ''

    if weekday <= 3:
        expiry = (datetime.now().today() + timedelta(days=3 - weekday)).date().isoformat()

    if weekday >= 4:
        expiry = (datetime.now().today() + timedelta(days=6)).date().isoformat()

    fno_nearest_expiry = sorted(df[(df.segment == 'NFO-FUT')].expiry)[0]

    options = df[(df.expiry <= expiry) & (df.segment == 'NFO-OPT') & df.instrument_type.isin(['CE','PE']) & (df.name.isin(['BANKNIFTY','NIFTY']))]

    front_month = date(datetime.now().date().year, datetime.now().date().month,1) + timedelta(days=70)
    front_month = date(front_month.year, front_month.month, calendar.monthrange(front_month.year, front_month.month)[1])
    front_month = front_month.isoformat()

    indices = df[(df.segment == 'INDICES') & (df.name.isin(['NIFTY 50','NIFTY BANK']))]
    vix = df[(df.segment == 'INDICES') & (df.name == 'INDIA VIX')]

    fno = df[(df.exchange == 'NFO') & (df.instrument_type == 'FUT') & (df.expiry <= front_month)]

    stocks = df[(df.segment== 'NSE') & (df.exchange == 'NSE') & ~(df.name.isna()) & (df.tradingsymbol.isin(fno.name.to_list()))]

    final = pd.concat([fno , stocks, indices, vix])

    instrument_tokens = final.instrument_token.to_list()

    instrumentMap = dict(zip(final.instrument_token, final.tradingsymbol))

    allinstrumentMap = dict(zip(df.instrument_token, df.tradingsymbol))

    ws.subscribe(instrument_tokens)
    ws.set_mode(ws.MODE_FULL,instrument_tokens)

    print(final.shape[0])

    stocks = indices = fno = final = None


_thread.start_new_thread(read_queue, ())
_thread.start_new_thread(read_queue, ())
_thread.start_new_thread(read_queue, ())
_thread.start_new_thread(read_queue, ())


kws = KiteTicker(api_key, access_token)
kws.on_ticks = on_ticks
kws.on_order_update = on_order_update
kws.on_connect = on_connect
kws.connect()


