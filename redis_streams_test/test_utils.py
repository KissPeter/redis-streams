import logging
import os
import sys

from redis_streams import PACKAGE

STREAM = f"test_stream_{sys.version_info.major}.{sys.version_info.minor}"
GROUP = "test_group"
TEST_DATASET = [{"test": "data1"}, {"test": "data2"}]


def get_test_name(suffix=""):
    _, _, test_name = os.getenv("PYTEST_CURRENT_TEST").split("::", 2)
    if " " in test_name:
        # remove ' (call)' from test name
        test_name, _ = test_name.split(" ", 1)
    return f"{test_name}{suffix}"


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
