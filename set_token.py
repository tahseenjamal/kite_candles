import redis


# Write the tokens in Redis so that other applications can use them

access_token = 'YOUR ACCESS TOKEN'
public_token = 'YOUR PUBLIC TOKEN'

redis_client = redis.Redis()
redis_client.hset("token.{}".format(userid), "access_token", access_token)
redis_client.hset("token.{}".format(userid), "public_token", public_token)

