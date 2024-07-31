from redis_streams.common import ConsumerAndMonitor
from redis_streams.consumer import Consumer
from redis_streams_test.base import TestBase
from redis_streams_test.test_utils import STREAM, GROUP, get_test_name


class TestCommon(TestBase):

    @classmethod
    def setup_class(cls):
        cls.base = ConsumerAndMonitor(
            redis_conn=cls.redis_conn, stream=STREAM, consumer_group=GROUP
        )

    def test_get_pending_items_of_consumer(self):
        redis_consumer = Consumer(
            redis_conn=self.redis_conn,
            stream=STREAM,
            consumer_group=GROUP,
            poll_time_ms=50,
            batch_size=1,
            consumer_id=get_test_name(),
        )
        redis_consumer.get_items()
        pending_items_of_consumer = self.base.get_pending_items_of_consumer(
            item_count=1, consumer_id=redis_consumer.consumer_id
        )
        assert len(pending_items_of_consumer) == 1, pending_items_of_consumer

    def test_remove_consumer(self):
        redis_consumer = Consumer(
            redis_conn=self.redis_conn,
            stream=STREAM,
            consumer_group=GROUP,
            poll_time_ms=50,
            batch_size=1,
            consumer_id=get_test_name(),
        )
        redis_consumer.get_items()
        lost_items = self.base.remove_consumer(get_test_name())
        assert lost_items == 1, lost_items
