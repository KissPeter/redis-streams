import datetime

import pytest
from redis import Redis

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


class TestConsumerE2E:

    def test_end_to_end_full_batch(self):
        redis_consumer = Consumer(
            redis_conn=redis_conn,
            stream=STREAM,
            consumer_group=GROUP,
            poll_time_ms=500,
            batch_size=len(TEST_DATASET),
            consumer_id=get_test_name()
        )
        messages = redis_consumer.get_items()
        assert len(messages) == len(TEST_DATASET)
        for message in messages:
            logger.debug(message)
            assert message.content in TEST_DATASET
            redis_consumer.remove_item_from_stream(item_id=message.msgid)

    def test_end_to_end_return_before_full_batch(self):
        max_wait_time = 50
        redis_consumer = Consumer(
            redis_conn=redis_conn,
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
