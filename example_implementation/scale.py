#!/usr/bin/env python3
import logging
from time import sleep

from redis import Redis

from common import STREAM, GROUP
from redis_batch import PACKAGE
from redis_batch.scaler import Scaler

if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    logging.getLogger(PACKAGE)

    scaler = Scaler(
        redis_conn=Redis(decode_responses=True), stream=STREAM, consumer_group=GROUP
    )
    while True:
        scaler.collect_metrics()
        rate, suggestion = scaler.get_scale_decision(
            scale_out_rate=60, scale_in_rate=20
        )
        print(
            f"Consumers should be {suggestion} as stream length ({scaler.stream_lenght}) / pending ({scaler.stream_pending}) rate is {rate}%"
        )
        sleep(2)
