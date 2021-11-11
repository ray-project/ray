import sys

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


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
