import logging

import pytest
from redis import Redis

from redis_batch import PACKAGE
from redis_batch.consumer import Consumer

STREAM = f"test_stream"
GROUP = "test_group"


def set_logger(level=logging.DEBUG):
    if level == logging.DEBUG:
        formatter = logging.Formatter(
            "{%(filename)s:%(lineno)d} [%(levelname)s]  %(name)s: %(message)s"
        )
    else:
        formatter = logging.Formatter("[%(levelname)s]  %(name)s: %(message)s")
    logger = logging.getLogger(PACKAGE)
    logger.setLevel(level)
    stream = logging.StreamHandler()
    stream.setFormatter(formatter)
    logger.addHandler(stream)
    return logger


logger = set_logger()
redis_conn = Redis(decode_responses=True)


class TestE2E:
    test_dataset = [{"test": "data1"}, {"test": "data2"}]

    @pytest.fixture(scope='function')
    def prepare_redis(self):
        if redis_conn.xlen(name=STREAM):
            logger.debug(f'Trim {STREAM}')
            redis_conn.xtrim(STREAM, maxlen=0)
        for test_data in self.test_dataset:
            print(f">>>>>>>>>>>>>>>>>>>>>>>>Add {test_data}")
            redis_conn.xadd(name=STREAM, fields=test_data)
        assert redis_conn.xlen(name=STREAM) == len(self.test_dataset)

    def test_end_to_end_full_batch(self):
        redis_consumer = Consumer(
            redis_conn=redis_conn,
            stream=STREAM,
            consumer_group=GROUP,
            batch_size=len(self.test_dataset),
        )
        messages = redis_consumer.get_items()
        print(f" msg: {len(messages)}")
        print(f" dataset: {len(self.test_dataset)}")
        assert len(messages) == len(self.test_dataset)
        for message in messages:
            logger.debug(message)
            assert message.content in self.test_dataset
            redis_consumer.remove_item_from_stream(item_id=message.msgid)

    def test_end_to_end_return_before_full_batch(self):
        redis_consumer = Consumer(
            redis_conn=redis_conn,
            stream=STREAM,
            consumer_group=GROUP,
            batch_size=len(self.test_dataset) + 1,
        )
        messages = redis_consumer.get_items()
        print(f" msg: {len(messages)}")
        print(f" dataset: {len(self.test_dataset)}")
        assert len(messages) == len(self.test_dataset)
