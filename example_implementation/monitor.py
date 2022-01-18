#!/usr/bin/env python3
import logging
from time import sleep

from redis import Redis

from common import STREAM, GROUP
from redis_batch import PACKAGE
from redis_batch.monitor import Monitor

if __name__ == "__main__":
    logger = logging.basicConfig(level=logging.DEBUG)
    logging.getLogger(PACKAGE)

    monitor = Monitor(
        redis_conn=Redis(decode_responses=True),
        stream=STREAM,
        consumer_group=GROUP,
        batch_size=10,
    )
    while True:
        monitor.collect_monitoring_data(auto_cleanup=True)
        monitor.print_monitoring_data()
        sleep(2)
