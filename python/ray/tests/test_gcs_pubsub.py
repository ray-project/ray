import sys
import threading

import ray
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


def test_publish_and_subscribe_error_info_ft(ray_start_regular_with_external_redis):
    address_info = ray_start_regular_with_external_redis
    gcs_server_addr = address_info["gcs_address"]
    from threading import Barrier, Thread

    subscriber = GcsErrorSubscriber(address=gcs_server_addr)
    subscriber.subscribe()

    publisher = GcsPublisher(address=gcs_server_addr)

    err1 = ErrorTableData(error_message="test error message 1")
    err2 = ErrorTableData(error_message="test error message 2")
    err3 = ErrorTableData(error_message="test error message 3")
    err4 = ErrorTableData(error_message="test error message 4")
    b = Barrier(3)

    def publisher_func():
        print("Publisher HERE")
        publisher.publish_error(b"aaa_id", err1)
        publisher.publish_error(b"bbb_id", err2)

        b.wait()

        print("Publisher HERE")
        # Wait fo subscriber to subscribe first.
        # It's ok to loose log messages.
        from time import sleep

        sleep(5)
        publisher.publish_error(b"aaa_id", err3)
        print("pub err1")
        publisher.publish_error(b"bbb_id", err4)
        print("pub err2")
        print("DONE")

    def subscriber_func():
        print("Subscriber HERE")
        assert subscriber.poll() == (b"aaa_id", err1)
        assert subscriber.poll() == (b"bbb_id", err2)

        b.wait()
        assert subscriber.poll() == (b"aaa_id", err3)
        print("sub err1")
        assert subscriber.poll() == (b"bbb_id", err4)
        print("sub err2")

        subscriber.close()
        print("DONE")

    t1 = Thread(target=publisher_func)
    t2 = Thread(target=subscriber_func)
    t1.start()
    t2.start()
    b.wait()

    ray.worker._global_node.kill_gcs_server()
    from time import sleep

    sleep(1)
    ray.worker._global_node.start_gcs_server()
    sleep(1)

    t1.join()
    t2.join()


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


@pytest.mark.asyncio
async def test_aio_publish_and_subscribe_error_info_ft(
    ray_start_regular_with_external_redis,
):
    address_info = ray_start_regular_with_external_redis
    gcs_server_addr = address_info["gcs_address"]

    subscriber = GcsAioErrorSubscriber(address=gcs_server_addr)
    await subscriber.subscribe()

    err1 = ErrorTableData(error_message="test error message 1")
    err2 = ErrorTableData(error_message="test error message 2")
    err3 = ErrorTableData(error_message="test error message 3")
    err4 = ErrorTableData(error_message="test error message 4")

    def restart_gcs_server():
        import asyncio

        asyncio.set_event_loop(asyncio.new_event_loop())
        from time import sleep

        publisher = GcsAioPublisher(address=gcs_server_addr)
        asyncio.get_event_loop().run_until_complete(
            publisher.publish_error(b"aaa_id", err1)
        )
        asyncio.get_event_loop().run_until_complete(
            publisher.publish_error(b"bbb_id", err2)
        )

        # wait until subscribe consume everything
        sleep(5)
        ray.worker._global_node.kill_gcs_server()
        sleep(1)
        ray.worker._global_node.start_gcs_server()
        # wait until subscriber resubscribed
        sleep(5)

        asyncio.get_event_loop().run_until_complete(
            publisher.publish_error(b"aaa_id", err3)
        )
        asyncio.get_event_loop().run_until_complete(
            publisher.publish_error(b"bbb_id", err4)
        )

    t1 = threading.Thread(target=restart_gcs_server)
    t1.start()
    assert await subscriber.poll() == (b"aaa_id", err1)
    assert await subscriber.poll() == (b"bbb_id", err2)
    assert await subscriber.poll() == (b"aaa_id", err3)
    assert await subscriber.poll() == (b"bbb_id", err4)

    await subscriber.close()
    t1.join()


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
