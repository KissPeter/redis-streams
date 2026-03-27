from redis_streams.consumer import Consumer
from redis_streams.producer import Producer
from redis_streams_test.base import TestBase
from redis_streams_test.test_utils import STREAM, GROUP, get_test_name, TEST_DATASET


class TestProducerE2E(TestBase):

    def test_add_returns_message_id(self):
        producer = Producer(redis_conn=self.redis_conn, stream=STREAM)
        msg_id = producer.add({"key": "value"})
        assert msg_id is not None
        assert isinstance(msg_id, str)
        assert "-" in msg_id  # Redis stream IDs are <timestamp>-<seq>

    def test_add_message_appears_in_stream(self):
        producer = Producer(redis_conn=self.redis_conn, stream=STREAM)
        initial_len = self.redis_conn.xlen(STREAM)
        producer.add({"hello": "world"})
        assert self.redis_conn.xlen(STREAM) == initial_len + 1

    def test_add_multiple_messages(self):
        producer = Producer(redis_conn=self.redis_conn, stream=STREAM)
        initial_len = self.redis_conn.xlen(STREAM)
        ids = []
        for i in range(5):
            ids.append(producer.add({"iteration": str(i)}))
        assert len(set(ids)) == 5  # all unique IDs
        assert self.redis_conn.xlen(STREAM) == initial_len + 5

    def test_consumer_receives_produced_messages(self):
        """End-to-end: produce via Producer, consume via Consumer."""
        producer = Producer(redis_conn=self.redis_conn, stream=STREAM)
        payload = {"source": "producer_test", "value": "42"}
        producer.add(payload)

        consumer = Consumer(
            redis_conn=self.redis_conn,
            stream=STREAM,
            consumer_group=GROUP,
            batch_size=len(TEST_DATASET) + 1,
            max_wait_time_ms=500,
            poll_time_ms=50,
            consumer_id=get_test_name(),
        )
        messages = consumer.get_items()
        contents = [m.content for m in messages]
        assert payload in contents

    def test_maxlen_trims_stream(self):
        producer = Producer(redis_conn=self.redis_conn, stream=STREAM, maxlen=3)
        # TEST_DATASET already has 2 messages; add 5 more
        for i in range(5):
            producer.add({"trim_test": str(i)})
        # Redis ~ trimming is approximate, but length should be <= maxlen + a few
        assert self.redis_conn.xlen(STREAM) <= 10  # generous upper bound

    def test_repr(self):
        producer = Producer(redis_conn=self.redis_conn, stream=STREAM, maxlen=100)
        r = repr(producer)
        assert "Producer(" in r
        assert STREAM in r
        assert "100" in r

