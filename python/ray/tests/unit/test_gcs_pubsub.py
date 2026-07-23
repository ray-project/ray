import asyncio
from collections import deque

import grpc
import pytest

from ray._private.gcs_pubsub import (
    GcsAioNodeInfoSubscriber,
    GcsAioResourceUsageSubscriber,
    GcsSubscriberStateMissingError,
)


class _FakeRpcError(grpc.RpcError):
    def __init__(self, status_code, details=None):
        self._status_code = status_code
        self._details = details

    def code(self):
        return self._status_code

    def details(self):
        return self._details


def _make_aio_subscriber(subscriber_type):
    subscriber = subscriber_type.__new__(subscriber_type)
    subscriber._subscriber_id = b"\x01" * 28
    subscriber._last_batch_size = 0
    subscriber._max_processed_sequence_id = 0
    subscriber._publisher_id = b""
    subscriber._queue = deque()
    subscriber._close = asyncio.Event()
    return subscriber


@pytest.mark.asyncio
async def test_aio_node_subscriber_decodes_missing_state():
    subscriber = _make_aio_subscriber(GcsAioNodeInfoSubscriber)

    async def missing_subscriber_poll(request, timeout=None):
        assert request.reject_if_subscriber_missing
        raise _FakeRpcError(grpc.StatusCode.ABORTED, "NotFound")

    subscriber._poll_call = missing_subscriber_poll

    with pytest.raises(GcsSubscriberStateMissingError) as error:
        await subscriber.poll(batch_size=1)

    assert error.value.code() == grpc.StatusCode.NOT_FOUND


@pytest.mark.asyncio
async def test_aio_node_subscriber_propagates_unavailable():
    subscriber = _make_aio_subscriber(GcsAioNodeInfoSubscriber)

    async def unavailable_poll(request, timeout=None):
        assert request.reject_if_subscriber_missing
        raise _FakeRpcError(grpc.StatusCode.UNAVAILABLE)

    subscriber._poll_call = unavailable_poll

    with pytest.raises(_FakeRpcError):
        await subscriber.poll(batch_size=1)


@pytest.mark.asyncio
async def test_aio_resource_subscriber_suppresses_unavailable():
    subscriber = _make_aio_subscriber(GcsAioResourceUsageSubscriber)

    async def unavailable_poll(request, timeout=None):
        assert not request.reject_if_subscriber_missing
        raise _FakeRpcError(grpc.StatusCode.UNAVAILABLE)

    subscriber._poll_call = unavailable_poll

    await subscriber._poll()


@pytest.mark.asyncio
async def test_aio_poll_cancels_inflight_rpc_when_closed():
    subscriber = _make_aio_subscriber(GcsAioResourceUsageSubscriber)
    poll_started = asyncio.Event()
    poll_cancelled = asyncio.Event()

    async def blocking_poll(request, timeout=None):
        poll_started.set()
        try:
            await asyncio.Event().wait()
        finally:
            poll_cancelled.set()

    subscriber._poll_call = blocking_poll
    poll_task = asyncio.create_task(subscriber._poll())
    await poll_started.wait()

    subscriber._close.set()
    await poll_task

    assert poll_cancelled.is_set()
