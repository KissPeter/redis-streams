import logging
from typing import List

from redis import Redis
from redis.exceptions import ResponseError

from redis_streams import PACKAGE


class BaseRedisClass:
    def __init__(self, redis_conn: Redis, stream: str, consumer_group: str):
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
                name=self.stream, groupname=self.consumer_group, id="0-0", mkstream=True
            )
            self.logger.debug(f"{self.consumer_group} consumer group has been created")
        except ResponseError:
            self.logger.debug(f" {self.consumer_group} consumer group already exists")

    def prepare_redis(self) -> None:
        self._create_consumer_group()


class ConsumerAndMonitor(BaseRedisClass):
    def __init__(self, redis_conn: Redis, stream: str, consumer_group: str):
        super().__init__(redis_conn, stream, consumer_group)

    def get_pending_items_of_consumer(
        self, item_count: int, consumer_id: str
    ) -> List[dict]:
        """
        name: name of the stream.
        groupname: name of the consumer group.
        idle: available from  version 6.2. filter entries by their
        idle-time, given in milliseconds (optional).
        min: minimum stream ID.
        max: maximum stream ID.
        item_count: number of messages to return
        consumername: name of a consumer to filter by (optional).
         sample return data - could be used for DLQ: {'message_id': '1641847880578-0',
         'consumer': 'test_monitor_to_many_pending_items',
         'time_since_delivered': 203,
         'times_delivered': 2}
        """
        return self.redis_conn.xpending_range(
            name=self.stream,
            groupname=self.consumer_group,
            min="-",
            max="+",
            count=item_count,
            consumername=consumer_id,
        )

    def remove_consumer(self, consumer_to_delete: str) -> int:
        """
        Removes the consumer from the consumer group,  returns the number of lost
        messages as int
        """
        return self.redis_conn.xgroup_delconsumer(
            name=self.stream,
            groupname=self.consumer_group,
            consumername=consumer_to_delete,
        )
