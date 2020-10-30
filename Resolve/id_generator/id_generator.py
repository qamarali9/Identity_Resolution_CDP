import redis_connection
import redis_config

def gen_id():
    redis_conn = redis_connection.get_connection(redis_config.REDIS_HOST,redis_config.REDIS_PORT)
    return redis_conn.hincrby(redis_config.UID_HASH,redis_config.CURRENT_UID_KEY,1)
