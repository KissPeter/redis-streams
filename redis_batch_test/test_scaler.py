import pytest

from redis_batch.consumer import Consumer
from redis_batch.scaler import Scaler, Scale
from redis_batch_test.base import TestBase
from redis_batch_test.test_utils import STREAM, GROUP, get_test_name, TEST_DATASET


class TestMonitor(TestBase):

    def test_scaler_no_scale(self):
        redis_consumer = Consumer(
            redis_conn=self.redis_conn,
            stream=STREAM,
            consumer_group=GROUP,
            batch_size=2,
            max_wait_time_ms=100,
            consumer_id=get_test_name()
        )
        returned_items = redis_consumer.get_items()
        assert len(returned_items) == 2, returned_items
        scaler = Scaler(redis_conn=self.redis_conn, stream=STREAM, consumer_group=GROUP)
        stream_lenght, stream_pending = scaler.collect_metrics()
        assert stream_lenght == 0, stream_lenght
        assert stream_pending == 2, stream_pending
        rate, suggestion = scaler.get_scale_decision(
            scale_out_rate=60, scale_in_rate=20
        )
        # no scale as stream length = 0
        assert suggestion == Scale.NOSCALE.value, suggestion

    def test_scaler_scale_out(self):
            redis_consumer = Consumer(
                redis_conn=self.redis_conn,
                stream=STREAM,
                consumer_group=GROUP,
                batch_size=2,
                max_wait_time_ms=100,
                consumer_id=get_test_name()
            )
            returned_items = redis_consumer.get_items()
            assert len(returned_items) == 2, returned_items
            # add extra, non-consumed item
            self.redis_conn.xadd(name=STREAM, fields={"some":"stuff"})
            scaler = Scaler(redis_conn=self.redis_conn, stream=STREAM,
                            consumer_group=GROUP)
            stream_lenght, stream_pending = scaler.collect_metrics()
            assert stream_lenght == 1, stream_lenght
            assert stream_pending == 2, stream_pending
            rate, suggestion = scaler.get_scale_decision(
                scale_out_rate=50, scale_in_rate=20
            )
            # 1 len, 2 pending should give 50
            assert rate == 50, rate
            assert suggestion == Scale.OUT.value, suggestion

    def test_scaler_scale_in(self):
        redis_consumer1 = Consumer(
            redis_conn=self.redis_conn,
            stream=STREAM,
            consumer_group=GROUP,
            batch_size=2,
            max_wait_time_ms=100,
            consumer_id=get_test_name()
        )
        returned_items = redis_consumer1.get_items()
        Consumer(
            redis_conn=self.redis_conn,
            stream=STREAM,
            consumer_group=GROUP,
            batch_size=2,
            max_wait_time_ms=100,
            consumer_id=get_test_name(suffix="2")
        ).get_items()
        assert len(returned_items) == 2, returned_items
        # add extra, non-consumed item
        self.redis_conn.xadd(name=STREAM, fields={"some": "stuff"})
        scaler = Scaler(redis_conn=self.redis_conn, stream=STREAM, consumer_group=GROUP)
        stream_lenght, stream_pending = scaler.collect_metrics()
        assert stream_lenght == 1, stream_lenght
        assert stream_pending == 2, stream_pending
        rate, suggestion = scaler.get_scale_decision(
            scale_out_rate=80, scale_in_rate=75
        )
        assert suggestion == Scale.IN.value, f"rate: {rate}, suggestion: {suggestion}"

    def test_scaler_multiple_consumer_groups(self):
        Consumer(
            redis_conn=self.redis_conn,
            stream=STREAM,
            consumer_group=GROUP,
            batch_size=2,
            max_wait_time_ms=100,
            consumer_id=get_test_name()
        ).get_items()
        Consumer(
            redis_conn=self.redis_conn,
            stream=STREAM,
            consumer_group=f"{GROUP}_2",
            batch_size=2,
            max_wait_time_ms=100,
            consumer_id=get_test_name()
        ).get_items()
        scaler = Scaler(redis_conn=self.redis_conn, stream=STREAM,
                        consumer_group=GROUP)
        scaler.collect_metrics()

    def test_scaler_no_consumers(self):
            scaler = Scaler(redis_conn=self.redis_conn, stream=STREAM,
                            consumer_group=GROUP)
            stream_lenght, stream_pending = scaler.collect_metrics()
            # due to the implementation lenght is always lower than actual TODO: Fix
            assert stream_lenght == len(TEST_DATASET)-1
            assert stream_pending == 0

    def test_scaler_invalid_scaling_threshold(self):
        scaler = Scaler(redis_conn=self.redis_conn, stream=STREAM,
                        consumer_group=GROUP)
        with pytest.raises(ValueError):
            scaler.get_scale_decision(scale_in_rate=-1, scale_out_rate=10)
        with pytest.raises(ValueError):
            scaler.get_scale_decision(scale_in_rate=1, scale_out_rate=101)
        with pytest.raises(ValueError):
            scaler.get_scale_decision(scale_in_rate=11, scale_out_rate=10)

    def test_scaler_empy_stream(self):
        scaler = Scaler(redis_conn=self.redis_conn, stream=f"{STREAM}_2",
                        consumer_group=GROUP)
        stream_lenght, stream_pending = scaler.collect_metrics()
        assert stream_lenght == 0, stream_lenght
        assert stream_pending == 0, stream_pending
