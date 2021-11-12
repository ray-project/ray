import asyncio
import random
import sys
import threading
import time

import grpc
import ray
import ray._private.gcs_utils as gcs_utils
from ray._private.gcs_pubsub import GcsPublisher, GcsSubscriber, \
    GcsAioPublisher, GcsAioSubscriber
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

    subscriber = GcsSubscriber(address=gcs_server_addr)
    subscriber.subscribe_error()

    publisher = GcsPublisher(address=gcs_server_addr)
    err1 = ErrorTableData(error_message="test error message 1")
    err2 = ErrorTableData(error_message="test error message 2")
    publisher.publish_error(b"aaa_id", err1)
    publisher.publish_error(b"bbb_id", err2)

    assert subscriber.poll_error() == (b"aaa_id", err1)
    assert subscriber.poll_error() == (b"bbb_id", err2)

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


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "ray_start_regular", [{
        "_system_config": {
            "gcs_grpc_based_pubsub": True
        }
    }],
    indirect=True)
async def test_subscribe_with_client_rpc_failures(ray_start_regular):
    # This test uses short client RPC deadlines to simulate the subscriber
    # client encountering connection issues, and publisher receiving multiple
    # outstanding polling requests from a subscriber. These cases should be
    # handled with no message loss, with sequence numbers.

    address_info = ray_start_regular
    redis = ray._private.services.create_redis_client(
        address_info["redis_address"],
        password=ray.ray_constants.REDIS_DEFAULT_PASSWORD)

    gcs_server_addr = gcs_utils.get_gcs_address_from_redis(redis)

    num_messages = 200

    messages = []

    def receive_messages():
        subscriber = GcsSubscriber(address=gcs_server_addr)
        subscriber.subscribe_error()
        while len(messages) < num_messages:
            try:
                _, msg = subscriber.poll_error(timeout=random.uniform(0, 0.1))
            except grpc.RpcError as e:
                if e.code() == grpc.StatusCode.DEADLINE_EXCEEDED:
                    continue
                raise
            messages.append(msg)

    async_messages = []

    async def async_receive_messages():
        subscriber = GcsAioSubscriber(address=gcs_server_addr)
        await subscriber.subscribe_error()
        while len(async_messages) < num_messages:
            try:
                _, msg = await subscriber.poll_error(
                    timeout=random.uniform(0, 0.1))
            except grpc.RpcError as e:
                if e.code() == grpc.StatusCode.DEADLINE_EXCEEDED:
                    continue
                raise
            async_messages.append(msg)

    def run_async_receive_messages():
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(async_receive_messages())
        loop.close()

    t1 = threading.Thread(target=receive_messages)
    t1.start()

    t2 = threading.Thread(target=run_async_receive_messages)
    t2.start()

    publisher = GcsPublisher(address=gcs_server_addr)
    for i in range(0, num_messages):
        time.sleep(random.uniform(0, 0.2))
        publisher.publish_error(
            b"msg_id", ErrorTableData(error_message=f"error {i}"))

    t1.join(timeout=1)
    assert not t1.is_alive()
    assert len(messages) == num_messages

    t2.join(timeout=1)
    assert not t2.is_alive()
    assert len(async_messages) == num_messages

    for i in range(0, num_messages):
        err = ErrorTableData(error_message=f"error {i}")
        assert messages[i] == err
        assert async_messages[i] == err


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
