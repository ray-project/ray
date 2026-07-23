import asyncio
import re
import sys
import threading
from collections import deque

import grpc
import pytest

import ray
from ray._private.gcs_pubsub import (
    GcsAioNodeInfoSubscriber,
    GcsAioResourceUsageSubscriber,
)


class _FakeRpcError(grpc.RpcError):
    def __init__(self, status_code):
        self._status_code = status_code

    def code(self):
        return self._status_code


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
async def test_aio_node_subscriber_detects_lost_subscription():
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


def test_publish_and_subscribe_error_info(ray_start_regular):
    address_info = ray_start_regular
    gcs_server_addr = address_info["gcs_address"]

    subscriber = ray._raylet.GcsErrorSubscriber(address=gcs_server_addr)
    subscriber.subscribe()

    gcs_client = ray._raylet.GcsClient(address=gcs_server_addr)
    gcs_client.publish_error(b"aaa_id", "", "test error message 1")
    gcs_client.publish_error(b"bbb_id", "", "test error message 2")

    (key_id1, err1) = subscriber.poll()
    assert key_id1 == b"aaa_id"
    assert err1["error_message"] == "test error message 1"
    (key_id2, err2) = subscriber.poll()
    assert key_id2 == b"bbb_id"
    assert err2["error_message"] == "test error message 2"

    subscriber.close()


def test_publish_and_subscribe_logs(ray_start_regular):
    address_info = ray_start_regular
    gcs_server_addr = address_info["gcs_address"]

    subscriber = ray._raylet.GcsLogSubscriber(address=gcs_server_addr)
    subscriber.subscribe()

    gcs_client = ray._raylet.GcsClient(address=gcs_server_addr)
    log_batch = {
        "ip": "127.0.0.1",
        "pid": 1234,
        "job": "0001",
        "is_err": False,
        "lines": ["line 1", "line 2"],
        "actor_name": "test actor",
        "task_name": "test task",
    }
    gcs_client.publish_logs(log_batch)

    # PID is treated as string.
    log_batch["pid"] = "1234"
    assert subscriber.poll() == log_batch

    subscriber.close()


@pytest.mark.asyncio
async def test_aio_publish_and_subscribe_resource_usage(ray_start_regular):
    address_info = ray_start_regular
    gcs_server_addr = address_info["gcs_address"]

    subscriber = GcsAioResourceUsageSubscriber(address=gcs_server_addr)
    await subscriber.subscribe()

    gcs_client = ray._raylet.GcsClient(address=gcs_server_addr)
    await gcs_client.async_publish_node_resource_usage("aaa_id", '{"cpu": 1}')
    await gcs_client.async_publish_node_resource_usage("bbb_id", '{"cpu": 2}')

    assert await subscriber.poll() == ("aaa_id", '{"cpu": 1}')
    assert await subscriber.poll() == ("bbb_id", '{"cpu": 2}')

    await subscriber.close()


@pytest.mark.asyncio
async def test_aio_poll_no_leaks(ray_start_regular):
    """Test that polling doesn't leak memory."""
    ctx = ray_start_regular
    gcs_server_addr = ctx.address_info["gcs_address"]

    subscriber = GcsAioResourceUsageSubscriber(address=gcs_server_addr)
    await subscriber.subscribe()

    for _ in range(10000):
        subscriber.poll()
        # There should only be 1 task, but use 10 as a buffer.
        assert len(asyncio.all_tasks()) < 10

    await subscriber.close()


def test_two_subscribers(ray_start_regular):
    """Tests concurrently subscribing to two channels work."""

    address_info = ray_start_regular
    gcs_server_addr = address_info["gcs_address"]

    num_messages = 100

    errors = []
    error_subscriber = ray._raylet.GcsErrorSubscriber(address=gcs_server_addr)
    # Make sure subscription is registered before publishing starts.
    error_subscriber.subscribe()

    def receive_errors():
        while len(errors) < num_messages:
            _, msg = error_subscriber.poll()
            errors.append(msg)

    t1 = threading.Thread(target=receive_errors)
    t1.start()

    logs = []
    log_subscriber = ray._raylet.GcsLogSubscriber(address=gcs_server_addr)
    # Make sure subscription is registered before publishing starts.
    log_subscriber.subscribe()

    log_str_pattern = re.compile("^log ([0-9]+)$")

    def receive_logs():
        while len(logs) < num_messages:
            log_batch = log_subscriber.poll()
            if log_str_pattern.match(log_batch["lines"][0]):
                logs.append(log_batch)

    t2 = threading.Thread(target=receive_logs)
    t2.start()

    gcs_client = ray._raylet.GcsClient(address=gcs_server_addr)
    for i in range(0, num_messages):
        gcs_client.publish_error(b"msg_id", "", f"error {i}")
        gcs_client.publish_logs(
            {
                "ip": "127.0.0.1",
                "pid": "gcs",
                "job": "0001",
                "is_err": False,
                "lines": [f"log {i}"],
                "actor_name": "test actor",
                "task_name": "test task",
            }
        )

    t1.join(timeout=10)
    assert len(errors) == num_messages, str(errors)
    assert not t1.is_alive(), str(errors)

    t2.join(timeout=10)
    assert len(logs) == num_messages, str(logs)
    assert not t2.is_alive(), str(logs)

    for i in range(0, num_messages):
        assert errors[i]["error_message"] == f"error {i}", str(errors)
        assert logs[i]["lines"][0] == f"log {i}", str(logs)


if __name__ == "__main__":

    sys.exit(pytest.main(["-sv", __file__]))
