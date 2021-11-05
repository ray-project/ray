import sys

import ray
import ray._private.gcs_utils as gcs_utils
from ray.core.generated.gcs_pb2 import (ErrorTableData)
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
    redis_cli = ray._private.services.create_redis_client(
        address_info["redis_address"],
        password=ray.ray_constants.REDIS_DEFAULT_PASSWORD)
    gcs_server_addr = redis_cli.get("GcsServerAddress").decode("utf-8")

    gcs_utils.init_gcs_address(gcs_server_addr)
    assert gcs_utils.get_gcs_address() == gcs_server_addr

    subscriber = gcs_utils.GcsSubscriber()
    subscriber.subscribe_error()

    publisher = gcs_utils.GcsPublisher()
    err1 = ErrorTableData(error_message="test error message 1")
    err2 = ErrorTableData(error_message="test error message 2")
    publisher.publish_error(b"aaa_id", err1)
    publisher.publish_error(b"bbb_id", err2)

    errors = subscriber.poll_error()
    assert len(errors) == 2
    assert errors[0] == (b"aaa_id", err1)
    assert errors[1] == (b"bbb_id", err2)


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
