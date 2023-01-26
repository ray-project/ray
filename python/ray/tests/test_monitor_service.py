import pytest

import ray
import grpc
from ray.core.generated import monitor_pb2, monitor_pb2_grpc


@pytest.fixture
def monitor_stub(ray_start_regular_shared):
    channel = grpc.insecure_channel(ray_start_regular_shared["gcs_address"])

    return monitor_pb2_grpc.MonitorGcsServiceStub(channel)


def test_ray_version(monitor_stub):
    request = monitor_pb2.GetRayVersionRequest()
    response = monitor_stub.GetRayVersion(request)
    assert response.version == ray.__version__
