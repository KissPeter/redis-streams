#!/usr/bin/env python3
from time import sleep

from redis import Redis

from common import STREAM, GROUP, get_random_wait_time
from redis_batch.consumer import Consumer, RedisMsg


def process_message(item: RedisMsg):
    """
    Message processing logic goes here
    """
    _ = item.content


if __name__ == "__main__":
    import logging
    from redis_batch import PACKAGE
    logging.basicConfig(level=logging.DEBUG)
    logging.getLogger(PACKAGE)
    consumer = Consumer(
        redis_conn=Redis(decode_responses=True),
        stream=STREAM,
        consumer_group=GROUP,
        batch_size=10,
        cleanup_on_exit=False,
    )
    iteration = 0
    while True:
        messages = consumer.get_items()
        for i, item in enumerate(messages):
            sleep_time = get_random_wait_time()
            print(
                f"{iteration}. Sleeping for {sleep_time} s before acknowledge "
                f"{i}/{len(messages)} message:{item}"
            )
            process_message(item=item)
            # mimic processing
            sleep(sleep_time)
            consumer.remove_item_from_stream(item_id=item.msgid)
        iteration += 1
