import calendar
import logging
import _thread
import datetime
from redis import Redis
import json
import sys
import pandas as pd
from queue import Queue
from datetime import date, time, timedelta
from time import sleep

def json_output(**kwargs):
    for key in kwargs.keys():
        globals()[key]=kwargs[key]
    return kwargs


logging.basicConfig(filename=f'log-{date.today()}', level = logging.INFO, format='%(asctime)s:%(levelname)s,%(message)s')

df = pd.read_csv("~/kite_candles/instruments.csv", low_memory = False)
stocks = df[(df.segment== 'NSE') & (df.exchange == 'NSE') & ~(df.name.isna())]
indices = df[(df.segment == 'INDICES') & (df.tradingsymbol.str.contains('NIFTY') == True)]

front_month = date(datetime.datetime.now().date().year, datetime.datetime.now().date().month,1) + timedelta(days=70)
front_month = date(front_month.year, front_month.month, calendar.monthrange(front_month.year, front_month.month)[1])
front_month = front_month.isoformat()
fno = df[(df.exchange == 'NFO') & (df.instrument_type == 'FUT') & (df.expiry <= front_month)]

final = pd.concat([fno , stocks , indices])

instrument_tokens = final.instrument_token.to_list()
instrumentMap = dict(zip(final.instrument_token, final.tradingsymbol))

stocks = final.tradingsymbol.to_list()

allinstrumentMap = dict(zip(df.instrument_token, df.tradingsymbol))

df = indices = fno_nearest_expiry = fno = final = instrument_tokens = None

messageQueue = Queue()

today = date.today()

logging.info(f'Started at {datetime.datetime.now().time()}')


def time_increment(t, hours=0, minutes=0):
    hour = t.hour
    minute = t.minute
    total_minutes = hour * 60 + minute

    next_time_total_minutes = total_minutes + 60 * hours + minutes

    next_hour = next_time_total_minutes // 60
    next_minute = next_time_total_minutes % 60

    return time(next_hour, next_minute)




def buildcandle():

    candlesticks = {}
    timeArray = []

    start_time = time(9,0)
    nexttime = time_increment(start_time, 0,1)

    logging.info(f'Start time {start_time} Next time {nexttime}')


    while datetime.datetime.now().time() < start_time:

        sleep(1/1000)
        

    logging.info(f'Starting candle building {datetime.datetime.now().time()}')

    while datetime.datetime.now().time() < time(15,30):

        bulk_ticks = eval(messageQueue.get())

        for tickdata in bulk_ticks:

            # last_trade_time = tickdata.get('last_trade_time',0)

            candleinst = tickdata.get('instrument_token',0)
            candlesymbol = allinstrumentMap[candleinst]

            if candlesymbol not in stocks:
                continue

            currentTime = tickdata.get('exchange_timestamp').time()

            candlelast = tickdata.get('last_price',0)

            # Indexes don't have buy and sell quantity so have to fill their variables with 0
            candlebuyer = tickdata.get('total_buy_quantity',0)
            candleseller = tickdata.get('total_sell_quantity',0)
            candlevolume = tickdata.get('volume_traded',0)


            if nexttime not in timeArray and currentTime >= nexttime:

                logging.info('Writing Candle File')

                start_time = time_increment(nexttime,0,-1)

                tmp_value = start_time
                _thread.start_new_thread(writeToFile, (candlesticks.copy(),tmp_value))
                start_time = nexttime

                timeArray.append(nexttime)
                nexttime = time_increment(nexttime,0, 1)




    
            candlesticks[candlesymbol] = candlesticks.setdefault(candlesymbol,{})
            candlesticks[candlesymbol][start_time] = candlesticks[candlesymbol].setdefault(start_time, {})


            if candlesticks[candlesymbol][start_time].get('open') == None:
            
                candlesticks[candlesymbol][start_time]['open'] = candlelast

            candlesticks[candlesymbol][start_time]['high'] = candlesticks[candlesymbol][start_time].setdefault('high',0)
            candlesticks[candlesymbol][start_time]['high'] = max(candlelast, candlesticks[candlesymbol][start_time]['high'])

            candlesticks[candlesymbol][start_time]['low'] = candlesticks[candlesymbol][start_time].setdefault('low',100000000000)
            candlesticks[candlesymbol][start_time]['low'] =  min(candlelast, candlesticks[candlesymbol][start_time]['low'])

            candlesticks[candlesymbol][start_time]['last'] = candlelast

            candlesticks[candlesymbol][start_time]['buyer'] = candlebuyer
            candlesticks[candlesymbol][start_time]['seller'] = candleseller

            candlesticks[candlesymbol][start_time]['volume'] = candlevolume



def writeToFile(candles,start_time):

    global redis

    today = date.today()

    filename = "candle-{}.csv".format(today)

    filelog = open(filename,"a")


    for stockname in candles.keys():

        if start_time in candles[stockname]:

            candle_open = candles[stockname][start_time]['open']
            candle_high = candles[stockname][start_time]['high']
            candle_low = candles[stockname][start_time]['low']
            candle_last = candles[stockname][start_time]['last']
            candle_close = candle_last

            # Buyer and Seller 
            candle_buyer = candles[stockname][start_time]['buyer']
            candle_seller = candles[stockname][start_time]['seller']
            candle_volume = candles[stockname][start_time]['volume']


            #To Redis Queue


            json_result = json_output(timestamp=start_time.isoformat()[:5],
                            stockname=stockname,
                            candle_open=candle_open,
                            candle_high=candle_high, 
                            candle_low=candle_low,
                            candle_close=candle_close,
                            candle_buyer=candle_buyer,
                            candle_seller=candle_seller,
                            candle_volume=candle_volume)


            redis.publish('1min_candles', json.dumps(json_result))




            filelog.write("{},{},{},{},{},{},{},{},{}\n".format(start_time.isoformat()[:5],
                                                                stockname,
                                                                candle_open,
                                                                candle_high, 
                                                                candle_low,
                                                                candle_close,
                                                                candle_buyer,
                                                                candle_seller,
                                                                candle_volume))




    filelog.close()

redis = Redis('localhost')

pubsub = redis.pubsub()

pubsub.subscribe('ticks')

_thread.start_new_thread(buildcandle, ())

for data in pubsub.listen():

    if data['type'] != "message":
        continue

    if data != None and data.get('type', '') == 'message':

        messageQueue.put(data['data'])


    if datetime.datetime.now().time() > time(15,29,59):

        break



