"""
Redis-stream producer.

Provides a thin wrapper around ``XADD`` with optional ``maxlen`` trimming
and consistent logging.
"""

import logging
from typing import Dict, Optional, Union

from redis import Redis

from redis_streams import PACKAGE


class Producer:
    """
    Publishes messages to a Redis Stream.

    :param redis_conn: A ``redis.Redis`` connection instance.
    :param stream: Name of the target stream.
    :param maxlen: If set, the stream will be trimmed to approximately this
        length after each ``add`` call (uses Redis ``MAXLEN ~`` trimming).
    """

    def __init__(
        self,
        redis_conn: Redis,
        stream: str,
        maxlen: Optional[int] = None,
    ):
        self.redis_conn = redis_conn
        self.stream = stream
        self.maxlen = maxlen
        self.logger = logging.getLogger(PACKAGE)

    def add(self, data: Dict[str, Union[str, int, float, bytes]]) -> str:
        """
        Publish a single message to the stream.

        :param data: Field/value mapping to insert.
        :returns: The message ID assigned by Redis.
        """
        msg_id: str = self.redis_conn.xadd(  # type: ignore[assignment]
            name=self.stream,
            fields=data,  # type: ignore[arg-type]
            maxlen=self.maxlen,
            approximate=self.maxlen is not None,
        )
        self.logger.debug("Published message %s to %s", msg_id, self.stream)
        return msg_id

    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}("
            f"redis_conn={self.redis_conn},"
            f"stream={self.stream},"
            f"maxlen={self.maxlen})"
        )
