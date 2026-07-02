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
    fake._num_queued_requests = 0
    fake._semaphore = Semaphore(lambda: max_ongoing_requests)
    fake._start_request = Replica._start_request.__get__(fake, Replica)
    fake._track_queued_request = Replica._track_queued_request.__get__(fake, Replica)
    return fake


async def _wait_for_semaphore_waiter(semaphore: Semaphore):
    for _ in range(1000):
        await asyncio.sleep(0)
        if semaphore._waiters:
            return
    raise AssertionError("no coroutine blocked on the semaphore")


class TestTrackQueuedRequest:
    def test_release_decrements_exactly_once(self):
        fake = _make_fake_replica(max_ongoing_requests=1)

        with fake._track_queued_request() as release:
            assert fake._num_queued_requests == 1
            release()
            assert fake._num_queued_requests == 0
            # Extra calls are a no-op.
            release()
            assert fake._num_queued_requests == 0

        # Exiting the block must not decrement below zero.
        assert fake._num_queued_requests == 0

    @pytest.mark.asyncio
    async def test_count_released_when_cancelled_while_waiting(self):
        """A request cancelled while blocked on the slot is released on block
        exit, as the direct-ingress handlers wire it up."""
        fake = _make_fake_replica(max_ongoing_requests=1)

        # Hold the only slot so the request blocks while queued.
        await fake._semaphore.acquire()

        async def handler():
            with fake._track_queued_request() as release:
                async with fake._start_request(_make_metadata()):
                    release()

        task = asyncio.ensure_future(handler())
        await _wait_for_semaphore_waiter(fake._semaphore)
        assert fake._num_queued_requests == 1

        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

        assert fake._num_queued_requests == 0


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
