import random
import time
from typing import Union

from redis import Redis

from redis_batch.common import BaseRedisClass


def get_random_wait_time(
    min_wait: Union[float, int] = 2, max_wait: Union[float, int] = 5
):
    return random.randint(min_wait * 10, max_wait * 10) / 10


class Producer(BaseRedisClass):
    def __init__(
        self,
        redis_conn: Redis = None,
        stream: str = None,
        consumer_group: str = None,
    ):
        super().__init__(
            redis_conn=redis_conn, stream=stream, consumer_group=consumer_group
        )
        self.prepare_redis()
        self.feed_redis()

    def _add_message_to_stream(self, data: dict):
        """
        Add to a stream.
        """
        self.redis_conn.xadd(name=self.stream, fields=data)

    def feed_redis(self):
        iteration = 1
        while True:
            sample_data = {"iteration": iteration, "type": self.consumer_group}
            print(f" {iteration}. Adding message to steam: {sample_data}")
            self._add_message_to_stream(sample_data)
            time.sleep(get_random_wait_time())
            iteration += 1
