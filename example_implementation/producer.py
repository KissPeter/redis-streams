#!/usr/bin/env python3
from time import sleep

from redis import Redis

from common import STREAM, get_random_wait_time

if __name__ == "__main__":
    redis_conn = Redis()
    iteration = 0
    while True:
        sample_data = {"iteration": iteration, "message": "stuff goes here"}
        print(f" {iteration}. Adding message to steam: {sample_data}")
        redis_conn.xadd(name=STREAM, fields=sample_data)
        sleep(get_random_wait_time())
        iteration += 1
