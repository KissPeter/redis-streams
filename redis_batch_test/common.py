import pytest
from redis import Redis

from redis_batch.common import BaseRedisClass
from redis_batch.consumer import Consumer
from redis_batch_test.test_utils import redis_conn, STREAM, GROUP, \
    get_test_name, set_logger, TEST_DATASET

redis_conn = Redis(decode_responses=True)
logger = set_logger()


@pytest.fixture(autouse=True)
def prepare_redis():
    if redis_conn.xlen(name=STREAM):
        logger.info(f'Trim {STREAM}')
        redis_conn.xtrim(STREAM, maxlen=0)
    for test_data in TEST_DATASET:
        logger.debug(f"Add  test data: {test_data}")
        redis_conn.xadd(name=STREAM, fields=test_data)
    assert redis_conn.xlen(name=STREAM) == len(TEST_DATASET)
    yield
    redis_conn.xtrim(STREAM, maxlen=0)
    for consumer in redis_conn.xinfo_consumers(name=STREAM, groupname=GROUP):
        logger.debug(f'Delete consumer {consumer}')
        redis_conn.xgroup_delconsumer(name=STREAM,
                                      groupname=GROUP,
                                      consumername=consumer.get("name"))


class TestCommon:
    base = BaseRedisClass(redis_conn=redis_conn, stream=STREAM, consumer_group=GROUP)

    def test_get_pending_items_of_consumer(self):
        redis_consumer = Consumer(
            redis_conn=redis_conn,
            stream=STREAM,
            consumer_group=GROUP,
            poll_time_ms=50,
            batch_size=1,
            consumer_id=get_test_name()
        )
        redis_consumer.get_items()
        pending_items_of_consumer = self.base.get_pending_items_of_consumer(
            item_count=1, consumer_id=redis_consumer.consumer_id)
        assert len(pending_items_of_consumer) == 1, pending_items_of_consumer

    def test_remove_consumer(self):
        redis_consumer = Consumer(
            redis_conn=redis_conn,
            stream=STREAM,
            consumer_group=GROUP,
            poll_time_ms=50,
            batch_size=1,
            consumer_id=get_test_name()
        )
        redis_consumer.get_items()
        lost_items = self.base.remove_consumer(get_test_name())
        assert lost_items == 1, lost_items
