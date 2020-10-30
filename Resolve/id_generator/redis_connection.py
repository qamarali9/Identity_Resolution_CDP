import redis

def get_connection(host,port):
    redis_conn = None
    try:
        redis_conn = redis.Redis(host,port)
    except Exception as Exc:
        print(Exc)
    return redis_conn
