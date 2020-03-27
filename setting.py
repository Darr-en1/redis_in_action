import redis

host = "192.168.98.128"

redis_conn = redis.Redis(host=host, decode_responses=True)
