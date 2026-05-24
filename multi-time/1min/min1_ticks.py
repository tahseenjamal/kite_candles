import calendar
import copy
import json
import logging
import os
import datetime
from datetime import date, time, timedelta
from queue import Queue, Empty
from threading import Thread
from time import sleep

import pandas as pd
from redis import Redis


logging.basicConfig(
    filename=f'log-{date.today()}',
    level=logging.INFO,
    format='%(asctime)s:%(levelname)s:%(message)s'
)
log = logging.getLogger(__name__)


df = pd.read_csv(os.path.expanduser("~/kite_candles/instruments.csv"), low_memory=False)

stocks_df = df[(df.segment == 'NSE') & (df.exchange == 'NSE') & df.name.notna()]
indices   = df[(df.segment == 'INDICES') & df.tradingsymbol.str.contains('NIFTY')]

front_month = date(datetime.datetime.now().year, datetime.datetime.now().month, 1) + timedelta(days=70)
front_month = date(
    front_month.year,
    front_month.month,
    calendar.monthrange(front_month.year, front_month.month)[1]
).isoformat()

fno   = df[(df.exchange == 'NFO') & (df.instrument_type == 'FUT') & (df.expiry <= front_month)]
final = pd.concat([fno, stocks_df, indices])

instrumentMap    = dict(zip(final.instrument_token, final.tradingsymbol))
allinstrumentMap = dict(zip(df.instrument_token, df.tradingsymbol))
tracked_symbols  = set(final.tradingsymbol.to_list())

df = indices = fno = final = stocks_df = None

messageQueue = Queue()

log.info(f'Started at {datetime.datetime.now().time()}')


def time_increment(t, hours=0, minutes=0):
    total = t.hour * 60 + t.minute + hours * 60 + minutes
    return time(total // 60, total % 60)


def buildcandle():
    candlesticks     = {}
    seen_candle_times = set()

    start_time = time(9, 0)
    nexttime   = time_increment(start_time, 0, 1)

    log.info(f'Start time {start_time}  Next time {nexttime}')

    while datetime.datetime.now().time() < start_time:
        sleep(0.001)

    log.info(f'Starting candle building at {datetime.datetime.now().time()}')

    while datetime.datetime.now().time() < time(15, 30):
        try:
            raw = messageQueue.get(timeout=1)
        except Empty:
            continue

        try:
            bulk_ticks = json.loads(raw)
        except (json.JSONDecodeError, TypeError):
            log.warning("Dropping malformed tick message")
            continue

        for tickdata in bulk_ticks:
            try:
                candleinst   = tickdata.get('instrument_token', 0)
                candlesymbol = allinstrumentMap.get(candleinst)

                if candlesymbol not in tracked_symbols:
                    continue

                currentTime  = datetime.datetime.fromisoformat(tickdata['exchange_timestamp']).time()
                candlelast   = tickdata.get('last_price', 0)
                candlebuyer  = tickdata.get('total_buy_quantity', 0)
                candleseller = tickdata.get('total_sell_quantity', 0)
                candlevolume = tickdata.get('volume_traded', 0)

                if nexttime not in seen_candle_times and currentTime >= nexttime:
                    log.info(f'Writing candle for {time_increment(nexttime, 0, -1)}')
                    candle_start = time_increment(nexttime, 0, -1)
                    Thread(
                        target=writeToFile,
                        args=(copy.deepcopy(candlesticks), candle_start),
                        daemon=True
                    ).start()
                    seen_candle_times.add(nexttime)
                    start_time = nexttime
                    nexttime   = time_increment(nexttime, 0, 1)

                bar = candlesticks.setdefault(candlesymbol, {}).setdefault(start_time, {})

                if 'open' not in bar:
                    bar['open'] = candlelast

                bar['high']   = max(candlelast, bar.get('high', 0))
                bar['low']    = min(candlelast, bar.get('low', 100_000_000_000))
                bar['last']   = candlelast
                bar['buyer']  = candlebuyer
                bar['seller'] = candleseller
                bar['volume'] = candlevolume

            except Exception:
                log.exception(f"Error processing tick (instrument_token={tickdata.get('instrument_token')})")


def writeToFile(candles, start_time):
    today    = date.today()
    filename = f"candle-{today}.csv"

    rows       = []
    redis_msgs = []

    for stockname, timeframes in candles.items():
        if start_time not in timeframes:
            continue
        bar         = timeframes[start_time]
        candle_close = bar['last']
        rows.append((
            start_time.isoformat()[:5],
            stockname,
            bar['open'],
            bar['high'],
            bar['low'],
            candle_close,
            bar['buyer'],
            bar['seller'],
            bar['volume'],
        ))
        redis_msgs.append({
            'timestamp':    start_time.isoformat()[:5],
            'stockname':    stockname,
            'candle_open':  bar['open'],
            'candle_high':  bar['high'],
            'candle_low':   bar['low'],
            'candle_close': candle_close,
            'candle_buyer': bar['buyer'],
            'candle_seller':bar['seller'],
            'candle_volume':bar['volume'],
        })

    pipeline = redis_client.pipeline()
    for msg in redis_msgs:
        pipeline.publish('1min_candles', json.dumps(msg))
    pipeline.execute()

    try:
        with open(filename, "a") as f:
            for row in rows:
                f.write(",".join(str(v) for v in row) + "\n")
    except IOError:
        log.exception(f"Failed to write candle file {filename}")


redis_client = Redis('localhost')
pubsub       = redis_client.pubsub()
pubsub.subscribe('ticks')

Thread(target=buildcandle, daemon=True).start()

log.info("Listening for ticks on Redis channel 'ticks'")

for data in pubsub.listen():
    if data['type'] != 'message':
        continue

    messageQueue.put(data['data'])

    if datetime.datetime.now().time() > time(15, 29, 59):
        break

log.info("Market closed. Exiting.")
