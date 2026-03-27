#!/usr/bin/env python3
from time import sleep

from redis import Redis

from common import STREAM, get_random_wait_time
from redis_streams.producer import Producer

if __name__ == "__main__":
    producer = Producer(redis_conn=Redis(), stream=STREAM)
    iteration = 0
    while True:
        sample_data = {"iteration": iteration, "message": "stuff goes here"}
        msg_id = producer.add(sample_data)
        print(f" {iteration}. Published message {msg_id}: {sample_data}")
        sleep(get_random_wait_time())
        iteration += 1
