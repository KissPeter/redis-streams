#!/usr/bin/env python3
import logging
from time import sleep

from common import GROUP, STREAM
from redis import Redis

from redis_streams import PACKAGE
from redis_streams.monitor import Monitor

if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    logging.getLogger(PACKAGE)

    monitor = Monitor(
        redis_conn=Redis(decode_responses=True,ssl=True,ssl_cert_reqs=None, username="default",password="Acj-AAIncDE3NDYwYWEzNTUwNjk0YmI3ODAxM2VhMDc2MTA4NzczM3AxNTE0NTQ", host="honest-hedgehog-51454.upstash.io"),
        stream=STREAM,
        consumer_group=GROUP,
        batch_size=10,
    )
    while True:
        monitor.collect_monitoring_data(auto_cleanup=True)
        monitor.print_monitoring_data()
        sleep(2)
