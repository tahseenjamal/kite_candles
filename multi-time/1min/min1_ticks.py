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


# ── Timeframe configuration ────────────────────────────────────────────────────
# List every candle width you want, in minutes.
# 60 = 1 hour,  120 = 2 hours,  360 = 6 hours.
# Each entry spawns one independent buildcandle thread and writes its own
# CSV + Redis channel.  Add or remove values freely — nothing else changes.
TIMEFRAMES = [1, 5, 15, 30, 60]
# ──────────────────────────────────────────────────────────────────────────────


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

allinstrumentMap = dict(zip(df.instrument_token, df.tradingsymbol))
tracked_symbols  = set(final.tradingsymbol.to_list())

df = indices = fno = final = stocks_df = None

# One independent queue per timeframe — main thread fans every message into all.
tf_queues = {tf: Queue() for tf in TIMEFRAMES}

log.info(f'Started at {datetime.datetime.now().time()} — timeframes: {TIMEFRAMES} minutes')


# ── Helpers ────────────────────────────────────────────────────────────────────

def time_increment(t, minutes):
    """Advance a time object by `minutes` (can be negative)."""
    total = t.hour * 60 + t.minute + minutes
    return time(total // 60, total % 60)


def label(interval_minutes):
    """'1min', '5min', '30min', '1h', '6h', etc."""
    if interval_minutes % 60 == 0:
        return f"{interval_minutes // 60}h"
    return f"{interval_minutes}min"


# ── Core candle builder (one thread per timeframe) ─────────────────────────────

def buildcandle(interval_minutes, queue):
    """
    Maintains an OHLCV state machine for every tracked symbol.
    Whenever exchange_timestamp crosses a candle boundary,
    snapshots the state and hands it to writeToFile on a new thread.
    """
    candlesticks      = {}
    seen_candle_times = set()
    lbl               = label(interval_minutes)

    start_time = time(9, 0)
    nexttime   = time_increment(start_time, interval_minutes)

    log.info(f'[{lbl}] start={start_time}  first boundary={nexttime}')

    while datetime.datetime.now().time() < start_time:
        sleep(0.001)

    log.info(f'[{lbl}] Building from {datetime.datetime.now().time()}')

    while datetime.datetime.now().time() < time(15, 30):
        try:
            raw = queue.get(timeout=1)
        except Empty:
            continue

        try:
            bulk_ticks = json.loads(raw)
        except (json.JSONDecodeError, TypeError):
            log.warning(f'[{lbl}] Dropping malformed tick message')
            continue

        for tickdata in bulk_ticks:
            try:
                token        = tickdata.get('instrument_token', 0)
                candlesymbol = allinstrumentMap.get(token)

                if candlesymbol not in tracked_symbols:
                    continue

                currentTime  = datetime.datetime.fromisoformat(tickdata['exchange_timestamp']).time()
                candlelast   = tickdata.get('last_price', 0)
                candlebuyer  = tickdata.get('total_buy_quantity', 0)
                candleseller = tickdata.get('total_sell_quantity', 0)
                candlevolume = tickdata.get('volume_traded', 0)

                # ── Candle boundary ───────────────────────────────────────────
                if nexttime not in seen_candle_times and currentTime >= nexttime:
                    log.info(f'[{lbl}] Candle closed — start={start_time}')
                    Thread(
                        target=writeToFile,
                        args=(copy.deepcopy(candlesticks), start_time, interval_minutes),
                        daemon=True
                    ).start()
                    seen_candle_times.add(nexttime)
                    start_time = nexttime
                    nexttime   = time_increment(nexttime, interval_minutes)

                # ── Accumulate bar ────────────────────────────────────────────
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
                log.exception(f'[{lbl}] Error processing tick (token={tickdata.get("instrument_token")})')


# ── File + Redis writer (one thread per candle boundary per timeframe) ──────────

def writeToFile(candles, start_time, interval_minutes):
    lbl      = label(interval_minutes)
    filename = f"candle_{lbl}-{date.today()}.csv"
    channel  = f"{lbl}_candles"

    rows       = []
    redis_msgs = []

    for stockname, timeframes in candles.items():
        if start_time not in timeframes:
            continue
        bar          = timeframes[start_time]
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
        pipeline.publish(channel, json.dumps(msg))
    pipeline.execute()

    try:
        with open(filename, "a") as f:
            for row in rows:
                f.write(",".join(str(v) for v in row) + "\n")
    except IOError:
        log.exception(f'[{lbl}] Failed to write {filename}')


# ── Startup ─────────────────────────────────────────────────────────────────────

redis_client = Redis('localhost')
pubsub       = redis_client.pubsub()
pubsub.subscribe('ticks')

for tf in TIMEFRAMES:
    Thread(target=buildcandle, args=(tf, tf_queues[tf]), daemon=True).start()

log.info(f"Listening on 'ticks' for: {[label(tf) for tf in TIMEFRAMES]}")

# ── Main loop: fan every incoming message into every timeframe queue ────────────

for data in pubsub.listen():
    if data['type'] != 'message':
        continue

    raw = data['data']
    for q in tf_queues.values():
        q.put(raw)

    if datetime.datetime.now().time() > time(15, 29, 59):
        break

log.info("Market closed. Exiting.")
