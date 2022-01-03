import logging
import os

from redis_batch import PACKAGE

STREAM = "test_stream"
GROUP = "test_group"

TEST_DATASET = [{"test": "data1"}, {"test": "data2"}]


def get_test_name():
    _, _, test_name = os.getenv("PYTEST_CURRENT_TEST").split("::", 2)
    return test_name


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

from redis import Redis

redis_conn = Redis(decode_responses=True)
