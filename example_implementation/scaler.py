#!/usr/bin/env python3
import logging
from time import sleep

from common import GROUP, STREAM
from redis import Redis

from redis_streams import PACKAGE
from redis_streams.scaler import Scaler

if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    logging.getLogger(PACKAGE)

    scaler = Scaler(
        redis_conn=Redis(decode_responses=True,ssl=True,ssl_cert_reqs=None, username="default",password="Acj-AAIncDE3NDYwYWEzNTUwNjk0YmI3ODAxM2VhMDc2MTA4NzczM3AxNTE0NTQ", host="honest-hedgehog-51454.upstash.io"), stream=STREAM, consumer_group=GROUP
    )
    while True:
        scaler.collect_metrics()
        rate, suggestion = scaler.get_scale_decision(
            scale_out_rate=60, scale_in_rate=20
        )
        print(
            f"Consumers should be {suggestion} as stream length "
            f"({scaler.stream_lenght}) / pending ({scaler.stream_pending}) "
            f"rate is {rate}%"
        )
        sleep(2)
