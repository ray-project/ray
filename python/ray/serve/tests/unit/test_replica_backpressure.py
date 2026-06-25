import asyncio
import sys
from unittest.mock import MagicMock

import pytest

from ray.serve._private.common import RequestMetadata
from ray.serve._private.replica import Replica
from ray.serve._private.utils import Semaphore


def _make_metadata() -> RequestMetadata:
    return RequestMetadata(
        request_id="test-request",
        internal_request_id="test-internal-request",
        is_direct_ingress=True,
    )


def _make_fake_replica(max_ongoing_requests: int):
    """Minimal stand-in exposing the real slot/queue accounting methods."""
    fake = MagicMock()
    fake._reserved_slots = set()
    fake._num_queued_requests = 0
    fake._metrics_manager = MagicMock()
    fake._semaphore = Semaphore(lambda: max_ongoing_requests)
    fake._start_request = Replica._start_request.__get__(fake, Replica)
    fake._start_queued_request = Replica._start_queued_request.__get__(fake, Replica)
    return fake


class TestStartQueuedRequest:
    @pytest.mark.asyncio
    async def test_count_released_when_slot_acquired(self):
        fake = _make_fake_replica(max_ongoing_requests=1)

        fake._num_queued_requests += 1
        async with fake._start_queued_request(_make_metadata()):
            assert fake._num_queued_requests == 0

        assert fake._num_queued_requests == 0

    @pytest.mark.asyncio
    async def test_count_released_when_cancelled_while_waiting(self):
        fake = _make_fake_replica(max_ongoing_requests=1)

        # Hold the only slot so the next request blocks while queued.
        await fake._semaphore.acquire()

        async def queued_request():
            fake._num_queued_requests += 1
            async with fake._start_queued_request(_make_metadata()):
                pass

        task = asyncio.ensure_future(queued_request())

        # Let the request count itself and block waiting for a slot.
        for _ in range(1000):
            await asyncio.sleep(0)
            if fake._semaphore._waiters and fake._num_queued_requests == 1:
                break
        assert fake._num_queued_requests == 1

        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

        assert fake._num_queued_requests == 0


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
