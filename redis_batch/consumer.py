import os
import threading
from datetime import datetime, timedelta
from enum import Enum
from typing import List, Union

from redis import Redis
from redis.exceptions import ResponseError

from redis_batch.common import BaseRedisClass


class RedisMsg:
    def __init__(self, msgid, content):
        self.msgid = msgid
        self.content = content

    def __str__(self):
        return f"id: {self.msgid}, content: {self.content}"

    def __repr__(self):
        return f"RedisMsg(msgid={self.msgid}, content={self.content})"


class MsgId(Enum):
    """
    '>' add new messages from group consumer group
    '0' get messages already added to consumer group
    """

    never_delivered = ">"
    already_deliverd = "0"


class Consumer(BaseRedisClass):
    def __init__(
        self,
        redis_conn: Redis = None,
        stream: str = None,
        consumer_group: str = None,
        consumer_id: Union[str, int] = f"{os.getpid()}{threading.get_ident()}",
        batch_size: int = 2,
        max_wait_time_ms: int = 10000,
        poll_time_ms: int = 100,
    ):
        """
        poll_time_ms: poll time of one iteration
        max_wait_time_ms: Approximate maximum time to wait for the batch to be complete.
        Client returns if time pass even if the batch is not full. 0 means: no return
        """
        super().__init__(
            redis_conn=redis_conn, stream=stream, consumer_group=consumer_group
        )
        self.assigned_messages = 0
        self.consumer_id = consumer_id
        self.batch_size = batch_size
        self.poll_time_ms = poll_time_ms
        self.max_wait_time_ms = max_wait_time_ms
        self.hard_stop_time = self._get_hard_stop_time()

    def _wait_for_more_messages(self):
        _now = datetime.utcnow()
        date_constraint = _now <= self.hard_stop_time
        message_number_constraint = self.assigned_messages < self.batch_size
        self.logger.debug(f"Is time to wait for additional messages: {date_constraint} "
                          f"({_now} / {self.hard_stop_time}) "
                          f"Is batch ready: {not message_number_constraint} "
                          f"({self.assigned_messages} / {self.batch_size})")
        return all([date_constraint, message_number_constraint])

    def _get_hard_stop_time(self):
        return datetime.utcnow() + timedelta(microseconds=self.max_wait_time_ms * 1000)

    def get_items(self):
        self.assigned_messages = self._get_no_of_messages_already_assigned()
        while self._wait_for_more_messages():
            self.assigned_messages += self._get_new_items_to_consumer(
                requested_messages=max(1, self.batch_size - self.assigned_messages),
            )
        return self._get_messages_from_stream(
            latest_or_new=MsgId.already_deliverd.value
        )

    def _get_new_items_to_consumer(self, requested_messages):
        items = self._get_messages_from_stream(
            latest_or_new=MsgId.never_delivered.value,
            requested_messages=requested_messages,
        )
        self.logger.debug(f'Received {len(items)} new items from stream')
        return len(items)

    def _get_no_of_messages_already_assigned(self):
        messages = self.get_pending_items_of_consumer(item_count=self.batch_size,
                                                      consumer_id=self.consumer_id)
        _return = len(messages)
        self.logger.debug(f"Messages already assigned to this consumer: <= {_return}")
        return _return

    def _get_messages_from_stream(
        self,
        latest_or_new: MsgId = MsgId.never_delivered.value,
        requested_messages=None,
        wait_time=None,
    ) -> List[object]:
        """
        The command to read data from a group is XREADGROUP.
        In our example, when App A starts processing data,
        it calls the consumer (Alice) to fetch data, as in:
        XREADGROUP GROUP mygroup COUNT 2 Alice self.streamS myself.stream >
                Read from a self.stream via a consumer group.
        groupname: name of the consumer group.
        consumername: name of the requesting consumer.
        self.streams: a dict of self.stream names to self.stream IDs, where
               IDs indicate the last ID already seen.
        item_count: if set, returns maximum this amount of messages, beginning with the
               earliest available.
        block: number of milliseconds to wait, if nothing already present.
        noack: do not add messages to the PEL
        latest_or_new: see MsgId
        """
        if requested_messages is None:
            requested_messages = self.batch_size
        try:
            items = self.redis_conn.xreadgroup(
                groupname=self.consumer_group,
                consumername=self.consumer_id,
                count=requested_messages,
                streams={self.stream: latest_or_new},
                block=wait_time if wait_time else self.poll_time_ms,
                noack=False,
            )
            self.logger.debug(f'Got {items}')
            return self._transform_redis_resp_to_objects(items)
        except ResponseError:
            self.logger.warning(
                f"Failed to get messages from {self.stream} from "
                f"{self.consumer_group} as {self.consumer_id}",
                exc_info=True,
            )

    def _transform_redis_resp_to_objects(self, items):
        msgs = []
        if isinstance(items, list) and len(items):
            try:
                if items[0][0] == self.stream:
                    items = items[0][1]
            except IndexError:
                self.logger.warning(items, exc_info=True)
        for item in items:
            msgs.append(RedisMsg(msgid=item[0], content=item[1]))
        return msgs

    def remove_item_from_stream(self, item_id):
        """
        The data in the pending entries lists of your consumers will remain
        there until App A and App B acknowledge to Redis self.streams that they
        have successfully consumed the data.
        XACK myself.stream mygroup 1526569411111-0 1526569411112-0
        Acknowledges the successful processing of one or more messages.
        name: name of the self.stream.
        groupname: name of the consumer group.
        *ids: message ids to acknowlege.
        """
        self.redis_conn.xack(self.stream, self.consumer_group, item_id)

    def __repr__(self):
        return f"{self.__class__.__name__}(" \
               f"redis_conn={self.redis_conn}," \
               f"stream={self.stream}," \
               f"consumer_group={self.consumer_group}," \
               f"consumer_id={self.consumer_id}," \
               f"batch_size={self.batch_size}," \
               f"max_wait_time_ms={self.max_wait_time_ms}," \
               f"poll_time_ms={self.poll_time_ms})"

    def __del__(self):
        self.remove_consumer(self.consumer_id)
