import os
import threading
from enum import Enum
from typing import Union, List

from redis import Redis
from redis.exceptions import ResponseError

from redis_batch.common import BaseRedisClass


class RedisMsg:

    def __init__(self, msgid, content):
        self.msgid = msgid
        self.content = content

    def __str__(self):
        return f'id: {self.msgid}, content: {self.content}'

    def __repr__(self):
        return f'RedisMsg(msgid={self.msgid}, content={self.content})'

class MsgId(Enum):
    """
    '>' add new messages from group consumer group
    '0' get messages already added to consumer group
    """
    never_delivered = '>'
    already_deliverd = '0'


class Consumer(BaseRedisClass):
    def __init__(
        self,
        redis_conn: Redis = None,
        stream: str = None,
        consumer_group: str = None,
        consumer_id: Union[str, int] = f"{os.getpid()}{threading.get_ident()}",
        batch_size=2,
        max_wait_time_ms=0,
        poll_time=100
    ):
        """
        poll_time_ms: poll time of one iteration
        max_wait_time_ms: Approximate maximum time to wait for the batch to be complete.
        Client returns if time pass even if the batch is not full. 0 means: no return
        """
        super().__init__(
            redis_conn=redis_conn, stream=stream, consumer_group=consumer_group
        )
        self.consumer_id = consumer_id
        self.batch_size = batch_size
        self.poll_time_ms = poll_time
        self.wait_time_ms = max_wait_time_ms

    #
    # def process_items(self, items):
    #     iteration = 0
    #
    #     for item in items:
    #         iteration += 1
    #         msg_id = item[0].decode()
    #         msg = item[1]
    #         if iteration % 10 == 0:
    #             print(f" Skip {msg_id}")
    #         redis_item_name = (
    #             f'{msg.get(b"type", b"").decode()}_{msg.get(b"data", b"").decode()}'
    #         )
    #         self.logger.info(f"{iteration}/{len(items)} Item id: {msg_id},  msg:{msg}")
    #         self.redis_conn.incr(name=redis_item_name)
    #         self.remove_item_from_stream(msg_id)
    #
    # def process_messages(self):
    #     assigned_msgs = self.get_no_of_messages_already_assigned()
    #     if assigned_msgs < self.batch_size:
    #
    #         self.get_new_messages_from_group(
    #             requested_messages=max(1, self.batch_size - assigned_msgs)
    #         )
    #     else:
    #         messages = self.get_messages_assigned_to_consumer()
    #         items = messages[0][1]
    #         self.process_items(items=items)

    def get_items(self):
        assigned_msgs = self._get_no_of_messages_already_assigned()
        print(assigned_msgs)
        while assigned_msgs < self.batch_size:
            assigned_msgs += self._get_new_items_to_consumer(
                requested_messages=max(1, self.batch_size - assigned_msgs),
            )
            print(assigned_msgs)
        return self._get_pending_items_of_consumer()

    def _get_new_items_to_consumer(self, requested_messages):
        items = self._get_messages_from_stream(
            latest_or_new=MsgId.never_delivered.value,
            requested_messages=requested_messages)
        return len(items)

    def _get_no_of_messages_already_assigned(self):
        messages = self._get_messages_from_stream(
            latest_or_new=MsgId.already_deliverd.value)
        _return = len(messages)
        self.logger.debug(f"Messages already assigned to this consumer: <= {_return}")
        return _return

    def _get_messages_from_stream(
        self, latest_or_new: MsgId = MsgId.never_delivered.value,
        requested_messages=None,
        wait_time=None
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
        count: if set, returns maximum this amount of messages, beginning with the
               earliest available.
        block: number of milliseconds to wait, if nothing already present.
        noack: do not add messages to the PEL
        latest_or_new: see MsgId
        """
        if requested_messages is None:
            requested_messages = self.batch_size
        try:
            msgs = []
            items = self.redis_conn.xreadgroup(
                groupname=self.consumer_group,
                consumername=self.consumer_id,
                count=requested_messages,
                streams={self.stream: latest_or_new},
                block=wait_time if wait_time else self.poll_time_ms,
                noack=False,
            )
            return items
        except ResponseError:
            self.logger.warning(
                f"Failed to get messages from {self.stream} from "
                f"{self.consumer_group} as {self.consumer_id}",
                exc_info=True,
            )

    def _get_pending_items_of_consumer(self) -> List[RedisMsg]:
        """
            name: name of the stream.
            groupname: name of the consumer group.
            idle: available from  version 6.2. filter entries by their
            idle-time, given in milliseconds (optional).
            min: minimum stream ID.
            max: maximum stream ID.
            count: number of messages to return
            consumername: name of a consumer to filter by (optional).
        """
        try:
            msgs = []
            items = self.redis_conn.xpending_range(
                name=self.stream,
                groupname=self.consumer_group,
                consumername=self.consumer_id,
                count=self.batch_size,
                min='-',
                max='+',
            )
            for item in items:
                print(f'1>>>>{item}')
                msgs.append(RedisMsg(msgid=item.get('message_id').decode(), content=item.get('content')))
            print(f'>>>>{len(msgs)}')
            return msgs
        except ResponseError:
            self.logger.warning(
                f"Failed to get messages from {self.stream} from "
                f"{self.consumer_group} as {self.consumer_id}",
                exc_info=True,
            )

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
