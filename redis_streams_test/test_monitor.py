import time

from redis_streams.monitor import Monitor
from redis_streams_test.base import TestBase
from redis_streams.consumer import Consumer
from redis_streams_test.test_utils import STREAM, GROUP, get_test_name
from io import StringIO


class TestMonitor(TestBase):

    def test_monitor_to_many_pending_items(self):
        redis_consumer1 = Consumer(
            redis_conn=self.redis_conn,
            stream=STREAM,
            consumer_group=GROUP,
            batch_size=2,
            max_wait_time_ms=100,
            consumer_id=get_test_name(),
        )
        redis_consumer1.get_items()
        monitor = Monitor(
            redis_conn=self.redis_conn,
            stream=STREAM,
            consumer_group=GROUP,
            batch_size=1,
            idle_time_ms_warning_threshold=10,
            min_wait_time_ms=1,
        )
        monitor.collect_monitoring_data(auto_cleanup=False)
        assert len(monitor.collected_consumers_data), monitor.collected_consumers_data
        redis_consumer2 = Consumer(
            redis_conn=self.redis_conn,
            stream=STREAM,
            consumer_group=GROUP,
            batch_size=2,
            max_wait_time_ms=100,
            consumer_id=get_test_name(suffix="2"),
        )
        redis_consumer2.get_items()
        # cleanup first consumer
        monitor.collect_monitoring_data()

    def test_monitor_print(self):
        redis_consumer1 = Consumer(
            redis_conn=self.redis_conn,
            stream=STREAM,
            consumer_group=GROUP,
            batch_size=2,
            max_wait_time_ms=100,
            consumer_id=get_test_name(),
        )
        redis_consumer1.get_items()
        monitor = Monitor(
            redis_conn=self.redis_conn,
            stream=STREAM,
            consumer_group=GROUP,
            batch_size=1,
            idle_time_ms_warning_threshold=10,
            min_wait_time_ms=1,
        )
        monitor.collect_monitoring_data(auto_cleanup=False)
        monitor.print_monitoring_data("NonStream")
        strm = StringIO()
        assert len(strm.getvalue()) == 0, strm.tell()
        monitor.print_monitoring_data(output_stream=strm)
        assert len(strm.getvalue()) > 0

    def test_monitor_long_idle(self):
        redis_consumer1 = Consumer(
            redis_conn=self.redis_conn,
            stream=STREAM,
            consumer_group=GROUP,
            batch_size=2,
            max_wait_time_ms=10000,
            poll_time_ms=1000,
            consumer_id=get_test_name(),
        )
        redis_consumer1.get_items()
        time.sleep(1)
        monitor = Monitor(
            redis_conn=self.redis_conn,
            stream=STREAM,
            consumer_group=GROUP,
            batch_size=2,
            idle_time_ms_warning_threshold=10,
            min_wait_time_ms=1,
        )
        monitor.collect_monitoring_data(auto_cleanup=False)
        assert len(monitor.collected_consumers_data), monitor.collected_consumers_data
        monitor.print_monitoring_data("NonStream")
