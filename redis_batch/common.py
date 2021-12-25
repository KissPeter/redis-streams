import logging

from redis import Redis
from redis.exceptions import ResponseError

from redis_batch import PACKAGE


class BaseRedisClass:
    def __init__(
        self, redis_conn: Redis = None, stream: str = None, consumer_group: str = None
    ):
        self.redis_conn = redis_conn
        self.stream = stream
        self.consumer_group = consumer_group
        self.logger = logging.getLogger(PACKAGE)
        self.prepare_redis()

    def _create_consumer_group(self) -> None:
        """
        Create a new consumer group using the command
        XGROUP CREATE mystream mygroup $ MKSTREAM
        $ sign: deliver only new data from that point in time forward.
        When using 0, deliver all data from the beginning of the stream
        """
        try:
            self.redis_conn.xgroup_create(
                name=self.stream, groupname=self.consumer_group, id="0", mkstream=True
            )
            self.logger.debug(
                f" {self.consumer_group} consumer group has been created "
            )
        except ResponseError:
            self.logger.debug(f" {self.consumer_group} consumer group already exists")

    def prepare_redis(self) -> None:
        self._create_consumer_group()
