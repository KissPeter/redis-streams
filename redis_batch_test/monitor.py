from redis_batch.monitor import Monitor
from redis_batch_test.base import TestBase
from redis_batch.consumer import Consumer
from redis_batch_test.test_utils import STREAM, GROUP,get_test_name


class TestMonitor(TestBase):

    def test_monitor(self):
        redis_consumer1 = Consumer(
            redis_conn=self.redis_conn,
            stream=STREAM,
            consumer_group=GROUP,
            batch_size=2,
            consumer_id=get_test_name()
        )
        redis_consumer1.get_items()
        monitor = Monitor(redis_conn=self.redis_conn, stream=STREAM,
                          consumer_group=GROUP, batch_size=1,
                          idle_time_ms_warning_threshold=10, min_wait_time_ms=1)
        monitor.collect_monitoring_data(auto_cleanup=False)
        assert len(monitor.collected_consumers_data), monitor.collected_consumers_data
        redis_consumer2 = Consumer(
            redis_conn=self.redis_conn,
            stream=STREAM,
            consumer_group=GROUP,
            batch_size=2,
            consumer_id=get_test_name(suffix='2')
        )
        redis_consumer2.get_items()
        #monitor.assign_items_to_active_consumer(items=1, group=GROUP, consumer_to_assign=redis_consumer2.consumer_id)

