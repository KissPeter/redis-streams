import datetime

from redis_batch.consumer import Consumer
from redis_batch_test.base import TestBase
from redis_batch_test.test_utils import STREAM, GROUP, \
    get_test_name, TEST_DATASET


class TestConsumerE2E(TestBase):

    def test_end_to_end_full_batch(self):
        redis_consumer = Consumer(
            redis_conn=self.redis_conn,
            stream=STREAM,
            consumer_group=GROUP,
            poll_time_ms=500,
            batch_size=len(TEST_DATASET),
            consumer_id=get_test_name()
        )
        messages = redis_consumer.get_items()
        assert len(messages) == len(TEST_DATASET)
        for message in messages:
            self.logger.debug(message)
            assert message.content in TEST_DATASET
            redis_consumer.remove_item_from_stream(item_id=message.msgid)

    def test_end_to_end_return_before_full_batch(self):
        max_wait_time = 50
        redis_consumer = Consumer(
            redis_conn=self.redis_conn,
            stream=STREAM,
            consumer_group=GROUP,
            max_wait_time_ms=max_wait_time,
            poll_time_ms=int(max_wait_time / 10),
            batch_size=len(TEST_DATASET) + 1,
            consumer_id=get_test_name()
        )
        t1 = datetime.datetime.now()
        messages = redis_consumer.get_items()
        timediff = datetime.datetime.now() - t1
        assert timediff.total_seconds() * 1000 >= max_wait_time
        assert len(messages) == len(TEST_DATASET)
