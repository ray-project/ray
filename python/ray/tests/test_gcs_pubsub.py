import sys
import threading

from ray._private.gcs_pubsub import (
    GcsPublisher,
    GcsErrorSubscriber,
    GcsLogSubscriber,
    GcsFunctionKeySubscriber,
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

    subscriber = GcsErrorSubscriber(address=gcs_server_addr)
    subscriber.subscribe()

    publisher = GcsPublisher(address=gcs_server_addr)
    err1 = ErrorTableData(error_message="test error message 1")
    err2 = ErrorTableData(error_message="test error message 2")
    publisher.publish_error(b"aaa_id", err1)
    publisher.publish_error(b"bbb_id", err2)

    assert subscriber.poll() == (b"aaa_id", err1)
    assert subscriber.poll() == (b"bbb_id", err2)

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

    subscriber = GcsLogSubscriber(address=gcs_server_addr)
    subscriber.subscribe()

    publisher = GcsPublisher(address=gcs_server_addr)
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


def test_publish_and_subscribe_function_keys(ray_start_regular):
    address_info = ray_start_regular
    gcs_server_addr = address_info["gcs_address"]

    subscriber = GcsFunctionKeySubscriber(address=gcs_server_addr)
    subscriber.subscribe()

    publisher = GcsPublisher(address=gcs_server_addr)
    publisher.publish_function_key(b"111")
    publisher.publish_function_key(b"222")

    assert subscriber.poll() == b"111"
    assert subscriber.poll() == b"222"

    subscriber.close()


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


def test_two_subscribers(ray_start_regular):
    """Tests concurrently subscribing to two channels work."""

    address_info = ray_start_regular
    gcs_server_addr = address_info["gcs_address"]

    num_messages = 100

    errors = []
    error_subscriber = GcsErrorSubscriber(address=gcs_server_addr)
    # Make sure subscription is registered before publishing starts.
    error_subscriber.subscribe()

    def receive_errors():
        while len(errors) < num_messages:
            _, msg = error_subscriber.poll()
            errors.append(msg)

    t1 = threading.Thread(target=receive_errors)
    t1.start()

    logs = []
    log_subscriber = GcsLogSubscriber(address=gcs_server_addr)
    # Make sure subscription is registered before publishing starts.
    log_subscriber.subscribe()

    def receive_logs():
        while len(logs) < num_messages:
            log_batch = log_subscriber.poll()
            logs.append(log_batch)

    t2 = threading.Thread(target=receive_logs)
    t2.start()

    publisher = GcsPublisher(address=gcs_server_addr)
    for i in range(0, num_messages):
        publisher.publish_error(b"msg_id", ErrorTableData(error_message=f"error {i}"))
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
        assert errors[i].error_message == f"error {i}", str(errors)
        assert logs[i]["lines"][0] == f"log {i}", str(logs)


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
