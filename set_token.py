import os
import redis
import pandas as pd


userdata = pd.read_json(os.path.expanduser("~/kite_candles/userdata"))
user_id  = userdata['YOURNAME'].user

access_token = 'YOUR ACCESS TOKEN'
public_token = 'YOUR PUBLIC TOKEN'

redis_client = redis.Redis()
redis_client.hset(f"token.{user_id}", "access_token", access_token)
redis_client.hset(f"token.{user_id}", "public_token", public_token)
