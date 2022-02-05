from enum import Enum
from typing import Tuple

from redis import Redis

from redis_streams.common import BaseRedisClass


class Scale(Enum):
    OUT = "OUT"
    IN = "IN"
    NOSCALE = "NO_SCALE"


class Scaler(BaseRedisClass):
    def __init__(
        self,
        redis_conn: Redis,
        stream: str,
        consumer_group: str,
    ):
        """
        By checking the number of messages waiting to be assigned and the number of
        pending items, utilization ratio can be calculated. Once this rate crosses a
        lower (scale in) or higher (scale out) the code will give a suggestion of
        scale in / out.
        """
        super().__init__(
            redis_conn=redis_conn, stream=stream, consumer_group=consumer_group
        )
        self.stream_lenght = 0
        self.stream_pending = 0
        self.lenght_pending_rate = 0
        self.consumers_of_group = 0

    def collect_metrics(self) -> Tuple[int, int]:
        last_delivered = None
        group_info = self.redis_conn.xinfo_groups(name=self.stream)
        for group in group_info:
            if group.get("name") == self.consumer_group:
                self.stream_pending = group.get("pending", 0)
                self.consumers_of_group = group.get("consumers", 0)
                last_delivered = group.get("last-delivered-id")
                break
        # XLEN provides the size of the stream, but doesn't consider messages processed
        # by our consumer group
        stream_info = self.redis_conn.xinfo_stream(name=self.stream)
        last_generated = stream_info.get("last-generated-id")
        if not last_delivered:
            self.stream_lenght = self.redis_conn.xlen(name=self.stream)
        elif last_generated == last_delivered:
            self.stream_lenght = 0
        else:
            # xrange output, the len of the messages should be decreased by one as it
            # includes the last delivered
            self.stream_lenght = max(
                0,
                len(
                    self.redis_conn.xrange(
                        name=self.stream, min=last_delivered, max=last_generated
                    )
                )
                - 1,
            )
        return self.stream_lenght, self.stream_pending

    @staticmethod
    def _validate_scaling_params(scale_out_rate, scale_in_rate):
        if scale_in_rate > scale_out_rate:
            raise ValueError("Scale out rate must be bigger than scale in rate")
        if scale_in_rate < 0 or scale_in_rate > 100:
            raise ValueError("Scale in rate must be within 0 and 100")
        if scale_out_rate < 0 or scale_out_rate > 100:
            raise ValueError("Scale in rate must be within 0 and 100")

    def _calculate_rate(self):
        if not all([self.stream_pending, self.stream_lenght]):
            self.collect_metrics()
        if self.stream_pending:
            self.lenght_pending_rate = round(
                max(min(self.stream_lenght / self.stream_pending * 100, 100), 1), 4
            )
        else:
            # if no pending item, no scale
            self.lenght_pending_rate = 0

    def _calculate_scale(self, scale_in_rate: int, scale_out_rate: int) -> str:

        if self.lenght_pending_rate == 0 and self.stream_lenght == 0:
            scale = Scale.NOSCALE.value
        elif self.lenght_pending_rate == 0 and self.stream_lenght >= 1:
            scale = Scale.OUT.value
        elif self.lenght_pending_rate < scale_in_rate and self.consumers_of_group > 1:
            scale = Scale.IN.value
        elif self.lenght_pending_rate >= scale_out_rate:
            scale = Scale.OUT.value
        else:
            scale = Scale.NOSCALE.value
        return scale

    def get_scale_decision(
        self, scale_out_rate=50, scale_in_rate=10
    ) -> Tuple[int, str]:
        """
        Rates are counted by stream length / number of pending items
        :param scale_out_rate: treshold rate of scale out in percent
        :param scale_in_rate:  treshold rate of scale in in percent
        :return: rate, suggestion
        """
        self._validate_scaling_params(
            scale_in_rate=scale_in_rate, scale_out_rate=scale_out_rate
        )
        self._calculate_rate()

        return self.lenght_pending_rate, self._calculate_scale(
            scale_in_rate=scale_in_rate, scale_out_rate=scale_out_rate
        )
