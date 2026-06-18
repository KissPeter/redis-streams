import time

import pytest

from redis_streams.common import XAUTOCLAIM_MIN_VERSION
from redis_streams.consumer import Consumer
from redis_streams.monitor import Monitor
from redis_streams_test.base import TestBase
from redis_streams_test.test_utils import GROUP, STREAM, get_test_name

ACTIVE_CONSUMER = "active_consumer"


class TestClaiming(TestBase):
    """
    Verify that re-assigning the pending messages of an unhealthy consumer works
    on both modern and old Redis servers:

    * Redis >= 6.2.0 -> native ``XAUTOCLAIM`` path
    * Redis <  6.2.0 -> ``XPENDING`` + ``XCLAIM`` fallback path

    The version-selection logic is exercised directly here (with the fallback
    forced on every server), while the CI Redis-version matrix runs the whole
    suite against several real servers (e.g. 6.0, 6.2, 7.4, 8.6.1) so the native
    path is also validated against an actual old/new server.
    """

    def _build_monitor(self, **overrides) -> Monitor:
        params = dict(
            redis_conn=self.redis_conn,
            stream=STREAM,
            consumer_group=GROUP,
            batch_size=1,
            idle_time_ms_warning_threshold=10,
            min_wait_time_ms=1,
        )
        params.update(overrides)
        return Monitor(**params)

    def _make_pending_consumer(self, suffix=""):
        consumer = Consumer(
            redis_conn=self.redis_conn,
            stream=STREAM,
            consumer_group=GROUP,
            batch_size=10,
            max_wait_time_ms=100,
            consumer_id=get_test_name(suffix=suffix),
        )
        items = consumer.get_items()
        assert len(items), "expected the consumer to pick up pending items"
        return consumer, items

    def _pending_count(self, consumer_id: str) -> int:
        return len(
            self.redis_conn.xpending_range(
                name=STREAM,
                groupname=GROUP,
                min="-",
                max="+",
                count=100,
                consumername=consumer_id,
            )
        )

    def test_version_detection_and_routing(self):
        """The chosen claim strategy must match the connected server version."""
        monitor = self._build_monitor()
        version = monitor.get_redis_version()
        assert isinstance(version, tuple) and version
        assert all(isinstance(part, int) for part in version)
        # cached on the second call
        assert monitor.get_redis_version() is version
        assert monitor.supports_xautoclaim() == (version >= XAUTOCLAIM_MIN_VERSION)

    def test_claiming_with_native_xautoclaim(self):
        """Native XAUTOCLAIM path - only runs on Redis >= 6.2.0."""
        monitor = self._build_monitor()
        if not monitor.supports_xautoclaim():
            pytest.skip(
                f"Redis {monitor.get_redis_version()} predates XAUTOCLAIM "
                f"({XAUTOCLAIM_MIN_VERSION}); native path not available"
            )
        consumer, items = self._make_pending_consumer()
        assert self._pending_count(consumer.consumer_id) == len(items)
        time.sleep(0.05)  # let the pending items become idle past min_wait_time_ms

        monitor.reassign_items_with_xautoclaim(
            consumer_to_assign=ACTIVE_CONSUMER, consumer_to_delete=consumer.consumer_id
        )

        assert self._pending_count(ACTIVE_CONSUMER) == len(items)
        assert self._pending_count(consumer.consumer_id) == 0

    def test_claiming_with_xclaim_fallback(self):
        """
        XCLAIM exists in every supported Redis version, so the fallback path
        must work everywhere - old and new servers alike. We force it on by
        pretending XAUTOCLAIM is unavailable.
        """
        consumer, items = self._make_pending_consumer()
        monitor = self._build_monitor()
        monitor.supports_xautoclaim = lambda: False  # force the legacy path
        monitor.consumer_to_assign = ACTIVE_CONSUMER
        time.sleep(0.05)

        monitor.reassign_items_with_xclaim(
            pending_count=len(items), consumer_to_delete=consumer.consumer_id
        )

        assert self._pending_count(ACTIVE_CONSUMER) == len(items)
        assert self._pending_count(consumer.consumer_id) == 0

    def test_cleanup_end_to_end_auto_selects_path(self):
        """
        The public ``cleanup_unhealthy_consumer`` auto-selects the strategy
        based on the server version, moves the pending items to an active
        consumer and removes the unhealthy one. This must hold on both new and
        old servers.
        """
        consumer, items = self._make_pending_consumer()
        monitor = self._build_monitor()
        monitor.consumer_to_assign = ACTIVE_CONSUMER
        time.sleep(0.05)

        monitor.cleanup_unhealthy_consumer(
            pending_count=len(items), consumer_to_delete=consumer.consumer_id
        )

        assert self._pending_count(ACTIVE_CONSUMER) == len(items)
        # the unhealthy consumer has been removed from the group
        remaining = {
            c.get("name")
            for c in self.redis_conn.xinfo_consumers(
                name=STREAM, groupname=GROUP
            )
        }
        assert consumer.consumer_id not in remaining
