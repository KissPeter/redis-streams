import random
from typing import Union

STREAM = "BatchStream"
GROUP = "BatchGroup"


def get_random_wait_time(
    min_wait: Union[float, int] = 1, max_wait: Union[float, int] = 5
) -> float:
    """
    Support ms grade wait
    """
    return random.randint(min_wait * 100, max_wait * 100) / 1000
