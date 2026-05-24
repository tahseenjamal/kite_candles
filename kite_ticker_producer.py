import json
import logging
import os
import calendar
from datetime import datetime, date, time, timedelta
from threading import Thread
from queue import Queue

import pandas as pd
import redis as redis_lib
from kiteconnect import KiteTicker


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s:%(levelname)s:%(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(f'producer-{date.today()}.log'),
    ]
)
log = logging.getLogger(__name__)


class _DatetimeEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, (datetime, date)):
            return obj.isoformat()
        return super().default(obj)


df = pd.read_csv(os.path.expanduser("~/kite_candles/instruments.csv"), low_memory=False)
userdata = pd.read_json(os.path.expanduser("~/kite_candles/userdata"))

redis_client = redis_lib.Redis('localhost')

user_id      = userdata['YOURNAME'].user
api_key      = userdata['YOURNAME'].apikey
access_token = redis_client.hget(f"token.{user_id}", "access_token").decode("utf-8")

allinstrumentMap = {}

publish_queue  = Queue()
tick_hash_queue = Queue()


def _publish_worker():
    while True:
        bulk_ticks = publish_queue.get()
        try:
            redis_client.publish('ticks', json.dumps(bulk_ticks, cls=_DatetimeEncoder))
        except Exception:
            log.exception("Failed to publish ticks to Redis")


def _tick_hash_worker():
    while True:
        tick = tick_hash_queue.get()
        try:
            stockname = allinstrumentMap.get(tick.get('instrument_token'))
            if not stockname:
                continue
            ohlc = tick.get('ohlc', {})
            redis_client.hset("tick_last",   stockname, tick.get('last_price', 0))
            redis_client.hset("tick_open",   stockname, ohlc.get('open', 0))
            redis_client.hset("tick_high",   stockname, ohlc.get('high', 0))
            redis_client.hset("tick_low",    stockname, ohlc.get('low', 0))
            redis_client.hset("tick_close",  stockname, ohlc.get('close', 0))
            redis_client.hset("tick_buyer",  stockname, tick.get('buy_quantity', 0))
            redis_client.hset("tick_seller", stockname, tick.get('sell_quantity', 0))
        except Exception:
            log.exception("Failed to write tick hash to Redis")


def on_order_update(ws, data):
    try:
        with open(f"{user_id}-order.log", "a") as f:
            f.write(f"{json.dumps(data)}\n")
    except Exception:
        log.exception("Failed to write order update")


def on_ticks(ws, ticks):
    now = datetime.now().time()
    if time(9, 0) <= now < time(15, 31):
        publish_queue.put(ticks)
        for tick in ticks:
            tick_hash_queue.put(tick)
    else:
        log.info("Market closed, shutting down")
        kws.close()
        os._exit(0)


def on_connect(ws, response):
    global allinstrumentMap

    today   = datetime.now()
    weekday = today.weekday()

    if weekday <= 3:
        expiry = (today + timedelta(days=3 - weekday)).date().isoformat()
    else:
        expiry = (today + timedelta(days=6)).date().isoformat()

    front_month = date(today.year, today.month, 1) + timedelta(days=70)
    front_month = date(
        front_month.year,
        front_month.month,
        calendar.monthrange(front_month.year, front_month.month)[1]
    ).isoformat()

    indices = df[(df.segment == 'INDICES') & df.name.isin(['NIFTY 50', 'NIFTY BANK'])]
    vix     = df[(df.segment == 'INDICES') & (df.name == 'INDIA VIX')]
    fno     = df[(df.exchange == 'NFO') & (df.instrument_type == 'FUT') & (df.expiry <= front_month)]
    stocks  = df[
        (df.segment == 'NSE') & (df.exchange == 'NSE') &
        df.name.notna() & df.tradingsymbol.isin(fno.name.to_list())
    ]

    final = pd.concat([fno, stocks, indices, vix])

    allinstrumentMap = dict(zip(df.instrument_token, df.tradingsymbol))

    instrument_tokens = final.instrument_token.to_list()
    ws.subscribe(instrument_tokens)
    ws.set_mode(ws.MODE_FULL, instrument_tokens)

    log.info(f"Subscribed to {final.shape[0]} instruments")


def on_error(ws, code, reason):
    log.error(f"WebSocket error {code}: {reason}")


def on_close(ws, code, reason):
    log.info(f"WebSocket closed — code: {code}, reason: {reason}")


for _ in range(4):
    Thread(target=_publish_worker, daemon=True).start()

Thread(target=_tick_hash_worker, daemon=True).start()

kws = KiteTicker(api_key, access_token)
kws.on_ticks        = on_ticks
kws.on_connect      = on_connect
kws.on_order_update = on_order_update
kws.on_error        = on_error
kws.on_close        = on_close
kws.connect()
