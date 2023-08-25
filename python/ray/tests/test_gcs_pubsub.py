import asyncio
import sys
import threading
import re

import ray
from ray._private.gcs_pubsub import (
    GcsAioPublisher,
    GcsAioErrorSubscriber,
    GcsAioLogSubscriber,
    GcsAioResourceUsageSubscriber,
)
from ray.core.generated.gcs_pb2 import ErrorTableData
import pytest


def test_publish_and_subscribe_error_info(ray_start_regular):
    address_info = ray_start_regular
    gcs_server_addr = address_info["gcs_address"]

    subscriber = ray._raylet.GcsErrorSubscriber(address=gcs_server_addr)
    subscriber.subscribe()

    publisher = ray._raylet.GcsPublisher(address=gcs_server_addr)
    publisher.publish_error(b"aaa_id", "", "test error message 1")
    publisher.publish_error(b"bbb_id", "", "test error message 2")

    (key_id1, err1) = subscriber.poll()
    assert key_id1 == b"aaa_id"
    assert err1["error_message"] == "test error message 1"
    (key_id2, err2) = subscriber.poll()
    assert key_id2 == b"bbb_id"
    assert err2["error_message"] == "test error message 2"

    subscriber.close()


@pytest.mark.asyncio
async def test_aio_publish_and_subscribe_error_info(ray_start_regular):
    address_info = ray_start_regular
    gcs_server_addr = address_info["gcs_address"]

    subscriber = GcsAioErrorSubscriber(address=gcs_server_addr)
    await subscriber.subscribe()

    publisher = GcsAioPublisher(address=gcs_server_addr)
    err1 = ErrorTableData(error_message="test error message 1")
    err2 = ErrorTableData(error_message="test error message 2")
    await publisher.publish_error(b"aaa_id", err1)
    await publisher.publish_error(b"bbb_id", err2)

    assert await subscriber.poll() == (b"aaa_id", err1)
    assert await subscriber.poll() == (b"bbb_id", err2)

    await subscriber.close()


def test_publish_and_subscribe_logs(ray_start_regular):
    address_info = ray_start_regular
    gcs_server_addr = address_info["gcs_address"]

    subscriber = ray._raylet.GcsLogSubscriber(address=gcs_server_addr)
    subscriber.subscribe()

    publisher = ray._raylet.GcsPublisher(address=gcs_server_addr)
    log_batch = {
        "ip": "127.0.0.1",
        "pid": 1234,
        "job": "0001",
        "is_err": False,
        "lines": ["line 1", "line 2"],
        "actor_name": "test actor",
        "task_name": "test task",
    }
    publisher.publish_logs(log_batch)

    # PID is treated as string.
    log_batch["pid"] = "1234"
    assert subscriber.poll() == log_batch

    subscriber.close()


@pytest.mark.asyncio
async def test_aio_publish_and_subscribe_logs(ray_start_regular):
    address_info = ray_start_regular
    gcs_server_addr = address_info["gcs_address"]

    subscriber = GcsAioLogSubscriber(address=gcs_server_addr)
    await subscriber.subscribe()

    publisher = GcsAioPublisher(address=gcs_server_addr)
    log_batch = {
        "ip": "127.0.0.1",
        "pid": "gcs",
        "job": "0001",
        "is_err": False,
        "lines": ["line 1", "line 2"],
        "actor_name": "test actor",
        "task_name": "test task",
    }
    await publisher.publish_logs(log_batch)

    assert await subscriber.poll() == log_batch

    await subscriber.close()


@pytest.mark.asyncio
async def test_aio_publish_and_subscribe_resource_usage(ray_start_regular):
    address_info = ray_start_regular
    gcs_server_addr = address_info["gcs_address"]

    subscriber = GcsAioResourceUsageSubscriber(address=gcs_server_addr)
    await subscriber.subscribe()

    publisher = GcsAioPublisher(address=gcs_server_addr)
    await publisher.publish_resource_usage("aaa_id", '{"cpu": 1}')
    await publisher.publish_resource_usage("bbb_id", '{"cpu": 2}')

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

    publisher = ray._raylet.GcsPublisher(address=gcs_server_addr)
    for i in range(0, num_messages):
        publisher.publish_error(b"msg_id", "", f"error {i}")
        publisher.publish_logs(
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
    import os

    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
