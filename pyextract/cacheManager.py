""" Module to wrap the connection to the redis cache """

import redis
import functools
import os


def redis_connection():
    def wrapper(function):
        @functools.wraps(function)
        def call(*args, **kwargs):
            redis_config = {}
            redis_config['host'] = os.getenv("REDIS_HOST", "redis")
            redis_config['port'] = int(os.getenv("REDIS_PORT", 6379))
            redis_config['db'] = int(os.getenv("REDIS_DB", 0))
            redis_conn = redis.StrictRedis(**redis_config)
            return function(redis_conn, *args, *kwargs)
        return call
    return wrapper