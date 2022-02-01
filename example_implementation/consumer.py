#!/usr/bin/env python3
from time import sleep

from redis import Redis

from common import STREAM, GROUP, get_random_wait_time
from redis_streams.consumer import Consumer, RedisMsg


def process_message(item: RedisMsg):
    """
    Message processing logic goes here
    """
    _ = item.content
    # mimic processing
    sleep(get_random_wait_time()/10)


if __name__ == "__main__":
    consumer = Consumer(
        redis_conn=Redis(decode_responses=True),
        stream=STREAM,
        consumer_group=GROUP,
        batch_size=10,
        max_wait_time_ms=30000,
    )
    while True:
        messages = consumer.get_items()
        for i, item in enumerate(messages):
            print(f"Pocessing {i+1}/{len(messages)} message:{item}")
            process_message(item=item)
            consumer.remove_item_from_stream(item_id=item.msgid)
