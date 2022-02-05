import json
import sys
from collections import defaultdict
from enum import Enum

from redis import Redis
from tabulate import tabulate

from redis_streams.common import ConsumerAndMonitor


class Status(Enum):
    OK = "OK"
    PENDING = "WARNING - too many pending items"
    IDLE = "WARNING - idle for long time"


class ConsumerMetrics:
    def __init__(
        self, consumer_id: str, pending_items: int, idle_time: int, status: str
    ):
        self.consumer_id = consumer_id
        self.pending_items = pending_items
        self.idle_time = idle_time
        self.status = status

    def __repr__(self):
        return (
            f'ConsumerMetrics(consumer_id="{self.consumer_id}", '
            f'pending_items={self.pending_items}", '
            f'idle_time={self.idle_time}", '
            f'status="{self.status}")'
        )

    def __str__(self):
        return json.dumps(
            {
                "consumer_id": self.consumer_id,
                "pending_items": self.pending_items,
                "idle_time": self.idle_time,
                "status": self.status,
            }
        )


class Monitor(ConsumerAndMonitor):
    def __init__(
        self,
        redis_conn: Redis,
        stream: str,
        consumer_group: str,
        batch_size: int = 2,
        min_wait_time_ms: int = 1000,
        idle_time_ms_warning_threshold: int = 30000,
    ):
        """
        Periodically check the activity of the consumers warns if they are idle  - not
        fetching message from the Stream for longer than the preconfigured inactivity
        threshold or have more assigned messages than the batch size. Automatic or
        on-demand cleanup are also supported.
        :param pending_items: alert if pending items of a consumer is bigger than this.
                              If you make batch collection it is a good idea to set it
                              equal to the batch size of consumers
        :param min_wait_time_ms: if messages need to be assigned to other consumer
                                 this parameter defines at least how old should they be
                                 We don't want to assign too fresh messages as
                                 they might being processed the time of re-assignment
        :param idle_time: alert threshold in milliseconds between the current time and
                          last time a consumer tried to fetch message from the stream.
                          Should be bigger to max_wait_time_ms parameter of consumer as
                          consumer need to collect messgages from the stream plus should
                          process them
        """
        super().__init__(
            redis_conn=redis_conn, stream=stream, consumer_group=consumer_group
        )
        self.batch_size = batch_size
        self.min_wait_time_ms = min_wait_time_ms
        self.idle_time_ms_warning_threshold = idle_time_ms_warning_threshold
        self.collected_consumers_data: list = []
        self.consumer_to_assign = ""
        self.unhealty_consumers: dict = defaultdict(lambda: {})

    def _get_status_by_metrics(self, pending: int, idle: int) -> str:
        status = Status.OK.value
        if pending > self.batch_size:
            status = Status.PENDING.value
        elif idle > self.idle_time_ms_warning_threshold:
            status = Status.IDLE.value
        return status

    def cleanup_unhealthy_consumer(
        self, pending_count: int, consumer_to_delete: str
    ) -> None:
        """
        1. query the pending items of consumer
        2. assign items to an active consumer
        3. remove consumer

        TODO: 2 and 3 can be done by XAUTOCLAIM if Redis supports
        """
        # 1
        messages_to_cleanup = []
        for message in self.get_pending_items_of_consumer(
            item_count=pending_count, consumer_id=consumer_to_delete
        ):
            messages_to_cleanup.append(message.get("message_id"))
        if len(messages_to_cleanup):
            self.logger.debug(
                f"Moving {len(messages_to_cleanup)} items from "
                f"{consumer_to_delete} to {self.consumer_to_assign}"
            )
            # 2
            self.assign_items_to_active_consumer(
                items=messages_to_cleanup,
                consumer_to_assign=self.consumer_to_assign,
                group=self.consumer_group,
            )
            self.logger.debug(
                f"Moved {len(messages_to_cleanup)} items from "
                f"{consumer_to_delete} to {self.consumer_to_assign}"
            )
        # 3
        resp = self.remove_consumer(consumer_to_delete=consumer_to_delete)
        if resp > 0:
            self.logger.error(f"{resp} messages lost")

    def assign_items_to_active_consumer(
        self, items: list, group: str, consumer_to_assign: str
    ) -> int:
        return self.redis_conn.xclaim(
            name=self.stream,
            groupname=group,
            consumername=consumer_to_assign,
            message_ids=items,
            min_idle_time=self.min_wait_time_ms,
        )

    def collect_monitoring_data(self, auto_cleanup=True) -> None:

        self.collected_consumers_data = []
        self.unhealty_consumers = defaultdict(lambda: {})
        self.consumer_to_assign = ""
        consumer_to_assign_pending_items = 0

        for group in self.redis_conn.xinfo_groups(self.stream):
            group_name = group.get("name")
            if group.get("consumers") > 0:
                for consumer in self.redis_conn.xinfo_consumers(
                    name=self.stream, groupname=group_name
                ):
                    consumer_id = consumer.get("name")
                    pending_items = consumer.get("pending", 0)
                    idle = consumer.get("idle")
                    status = self._get_status_by_metrics(
                        pending=pending_items, idle=idle
                    )
                    if status != Status.OK.value:
                        self.unhealty_consumers[group_name][consumer_id] = pending_items
                    else:
                        if not consumer_to_assign_pending_items:
                            pending_items = consumer_to_assign_pending_items
                        if pending_items <= consumer_to_assign_pending_items:
                            self.consumer_to_assign = consumer_id
                            consumer_to_assign_pending_items = pending_items
                    self.collected_consumers_data.append(
                        ConsumerMetrics(
                            consumer_id=consumer_id,
                            idle_time=consumer.get("idle"),
                            pending_items=consumer.get("pending"),
                            status=status,
                        )
                    )
        if auto_cleanup:
            if self.consumer_to_assign and len(self.unhealty_consumers):
                self.cleanup()
            elif self.consumer_to_assign:
                self.logger.debug("No cleanup, as no unhealty consumers")
            else:
                self.logger.debug("No cleanup, as no healty consumer to assign")
        else:
            self.logger.debug("Auto cleanup disabled")

    def cleanup(self):
        self.logger.debug("Cleaning up unhealthy consumers")
        for group in self.unhealty_consumers.keys():
            for consumer_id, pending_items in self.unhealty_consumers[group].items():
                self.cleanup_unhealthy_consumer(
                    consumer_to_delete=consumer_id,
                    pending_count=pending_items,
                )

    def _generate_table(self):
        rows = []
        for row in self.collected_consumers_data:
            rows.append([row.consumer_id, row.idle_time, row.pending_items, row.status])
        return tabulate(
            rows,
            headers=["Consumer id", "Idle time", "Pending items", "Status"],
            tablefmt="grid",
        )

    def print_monitoring_data(self, output_stream=sys.stdout):
        if hasattr(output_stream, "write"):
            output_stream.write(self._generate_table())
        else:
            print(self._generate_table())
