import sys
import threading

import ray
import ray._private.gcs_utils as gcs_utils
from ray._private.gcs_pubsub import GcsPublisher, GcsErrorSubscriber, \
    GcsLogSubscriber, GcsFunctionKeySubscriber, GcsAioPublisher, \
    GcsAioSubscriber
from ray.core.generated.gcs_pb2 import ErrorTableData
import pytest


@pytest.mark.parametrize(
    "ray_start_regular", [{
        "_system_config": {
            "gcs_grpc_based_pubsub": True
        }
    }],
    indirect=True)
def test_publish_and_subscribe_error_info(ray_start_regular):
    address_info = ray_start_regular
    redis = ray._private.services.create_redis_client(
        address_info["redis_address"],
        password=ray.ray_constants.REDIS_DEFAULT_PASSWORD)

    gcs_server_addr = gcs_utils.get_gcs_address_from_redis(redis)

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
@pytest.mark.parametrize(
    "ray_start_regular", [{
        "_system_config": {
            "gcs_grpc_based_pubsub": True
        }
    }],
    indirect=True)
async def test_aio_publish_and_subscribe_error_info(ray_start_regular):
    address_info = ray_start_regular
    redis = ray._private.services.create_redis_client(
        address_info["redis_address"],
        password=ray.ray_constants.REDIS_DEFAULT_PASSWORD)

    gcs_server_addr = gcs_utils.get_gcs_address_from_redis(redis)

    subscriber = GcsAioSubscriber(address=gcs_server_addr)
    await subscriber.subscribe_error()

    publisher = GcsAioPublisher(address=gcs_server_addr)
    err1 = ErrorTableData(error_message="test error message 1")
    err2 = ErrorTableData(error_message="test error message 2")
    await publisher.publish_error(b"aaa_id", err1)
    await publisher.publish_error(b"bbb_id", err2)

    assert await subscriber.poll_error() == (b"aaa_id", err1)
    assert await subscriber.poll_error() == (b"bbb_id", err2)

    await subscriber.close()


@pytest.mark.parametrize(
    "ray_start_regular", [{
        "_system_config": {
            "gcs_grpc_based_pubsub": True
        }
    }],
    indirect=True)
def test_publish_and_subscribe_logs(ray_start_regular):
    address_info = ray_start_regular
    redis = ray._private.services.create_redis_client(
        address_info["redis_address"],
        password=ray.ray_constants.REDIS_DEFAULT_PASSWORD)

    gcs_server_addr = gcs_utils.get_gcs_address_from_redis(redis)

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
@pytest.mark.parametrize(
    "ray_start_regular", [{
        "_system_config": {
            "gcs_grpc_based_pubsub": True
        }
    }],
    indirect=True)
async def test_aio_publish_and_subscribe_logs(ray_start_regular):
    address_info = ray_start_regular
    redis = ray._private.services.create_redis_client(
        address_info["redis_address"],
        password=ray.ray_constants.REDIS_DEFAULT_PASSWORD)

    gcs_server_addr = gcs_utils.get_gcs_address_from_redis(redis)

    subscriber = GcsAioSubscriber(address=gcs_server_addr)
    await subscriber.subscribe_logs()

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

    assert await subscriber.poll_logs() == log_batch

    await subscriber.close()


@pytest.mark.parametrize(
    "ray_start_regular", [{
        "_system_config": {
            "gcs_grpc_based_pubsub": True
        }
    }],
    indirect=True)
def test_publish_and_subscribe_function_keys(ray_start_regular):
    address_info = ray_start_regular
    redis = ray._private.services.create_redis_client(
        address_info["redis_address"],
        password=ray.ray_constants.REDIS_DEFAULT_PASSWORD)

    gcs_server_addr = gcs_utils.get_gcs_address_from_redis(redis)

    subscriber = GcsFunctionKeySubscriber(address=gcs_server_addr)
    subscriber.subscribe()

    publisher = GcsPublisher(address=gcs_server_addr)
    publisher.publish_function_key(b"111")
    publisher.publish_function_key(b"222")

    assert subscriber.poll() == b"111"
    assert subscriber.poll() == b"222"

    subscriber.close()


@pytest.mark.parametrize(
    "ray_start_regular", [{
        "_system_config": {
            "gcs_grpc_based_pubsub": True
        }
    }],
    indirect=True)
def test_subscribe_two_channels(ray_start_regular):
    """Tests concurrently subscribing to two channels work."""

    address_info = ray_start_regular
    redis = ray._private.services.create_redis_client(
        address_info["redis_address"],
        password=ray.ray_constants.REDIS_DEFAULT_PASSWORD)

    gcs_server_addr = gcs_utils.get_gcs_address_from_redis(redis)

    num_messages = 100

    errors = []

    def receive_errors():
        subscriber = GcsErrorSubscriber(address=gcs_server_addr)
        subscriber.subscribe()
        while len(errors) < num_messages:
            _, msg = subscriber.poll()
            errors.append(msg)

    logs = []

    def receive_logs():
        subscriber = GcsLogSubscriber(address=gcs_server_addr)
        subscriber.subscribe()
        while len(logs) < num_messages:
            log_batch = subscriber.poll()
            logs.append(log_batch)

    t1 = threading.Thread(target=receive_errors)
    t1.start()

    t2 = threading.Thread(target=receive_logs)
    t2.start()

    publisher = GcsPublisher(address=gcs_server_addr)
    for i in range(0, num_messages):
        publisher.publish_error(
            b"msg_id", ErrorTableData(error_message=f"error {i}"))
        publisher.publish_logs({
            "ip": "127.0.0.1",
            "pid": "gcs",
            "job": "0001",
            "is_err": False,
            "lines": [f"line {i}"],
            "actor_name": "test actor",
            "task_name": "test task",
        })

    t1.join(timeout=10)
    assert not t1.is_alive(), len(errors)
    assert len(errors) == num_messages, len(errors)

    t2.join(timeout=10)
    assert not t2.is_alive(), len(logs)
    assert len(logs) == num_messages, len(logs)

    for i in range(0, num_messages):
        assert errors[i].error_message == f"error {i}"
        assert logs[i]["lines"][0] == f"line {i}"


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
