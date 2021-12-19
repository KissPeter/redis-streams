import logging

from redis import Redis
from random import randint
from redis_batch.consumer import Consumer
from redis_batch import PACKAGE

STREAM = f'test_stream'
GROUP = 'test_group'


def set_logger(level=logging.DEBUG):
    if level == logging.DEBUG:
        formatter = logging.Formatter(
            '{%(filename)s:%(lineno)d} [%(levelname)s]  %(name)s: %(message)s')
    else:
        formatter = logging.Formatter('[%(levelname)s]  %(name)s: %(message)s')
    logger = logging.getLogger(PACKAGE)
    logger.setLevel(level)
    stream = logging.StreamHandler()
    stream.setFormatter(formatter)
    logger.addHandler(stream)
    return logger

logger = set_logger()

def test_end_to_end():
    redis_conn = Redis()
    if redis_conn.xlen(name=STREAM):
        redis_conn.xtrim(STREAM, maxlen=0)
    test_dataset = [{"test": "data1"}, {"test": "data2"}]
    for test_data in test_dataset:
        print(f'Add {test_data}')
        redis_conn.xadd(name=STREAM, fields=test_data)
    assert redis_conn.xlen(name=STREAM) == len(test_dataset)
    redis_consumer = Consumer(redis_conn=redis_conn, stream=STREAM,
                              consumer_group=GROUP, batch_size=len(test_dataset))
    messages = redis_consumer.get_items()
    print(f' msg: {len(messages)}')
    print(f' dataset: {len(test_dataset)}')
    assert len(messages) == len(test_dataset)
    for message in messages:
        logger.debug(message)
        assert message.content in test_dataset
        redis_consumer.remove_item_from_stream(item_id=message.msgid)


test_end_to_end()
