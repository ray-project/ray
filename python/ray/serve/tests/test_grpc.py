import pytest

from ray.serve.drivers import DefaultgRPCDriver, gRPCIngress
import ray
from ray import serve
from ray.serve.generated import serve_pb2, serve_pb2_grpc
import grpc
from ray.cluster_utils import Cluster
from ray.serve._private.constants import SERVE_NAMESPACE
from ray._private.test_utils import wait_for_condition
from ray.serve.exceptions import RayServeException

from unittest.mock import patch


pytestmark = pytest.mark.asyncio


@pytest.fixture
def serve_start_shutdown():
    ray.init()
    yield
    serve.shutdown()
    ray.shutdown()


@pytest.fixture
def ray_cluster():
    cluster = Cluster()
    yield Cluster()
    serve.shutdown()
    ray.shutdown()
    cluster.shutdown()


@patch("ray.serve._private.api.FLAG_DISABLE_HTTP_PROXY", True)
async def test_deploy_basic(serve_start_shutdown):
    @serve.deployment
    class D1:
        def __call__(self, input):
            return input["a"]

    serve.run(DefaultgRPCDriver.bind(D1.bind()))

    async def send_request():
        async with grpc.aio.insecure_channel("localhost:9000") as channel:
            stub = serve_pb2_grpc.PredictAPIsServiceStub(channel)
            response = await stub.Predict(
                serve_pb2.PredictRequest(input={"a": bytes("123", "utf-8")})
            )
        return response

    resp = await send_request()

    assert resp.prediction == b"123"


@patch("ray.serve._private.api.FLAG_DISABLE_HTTP_PROXY", True)
def test_controller_without_http(serve_start_shutdown):
    @serve.deployment
    class D1:
        def __call__(self, input):
            return input["a"]

    serve.run(DefaultgRPCDriver.bind(D1.bind()))
    assert (
        ray.get(serve.context._global_client._controller.get_http_proxies.remote())
        == {}
    )


@patch("ray.serve._private.api.FLAG_DISABLE_HTTP_PROXY", True)
def test_deploy_grpc_driver_to_node(ray_cluster):
    cluster = ray_cluster
    cluster.add_node(num_cpus=2)
    cluster.connect(namespace=SERVE_NAMESPACE)

    @serve.deployment
    class D1:
        def __call__(self, input):
            return input["a"]

    serve.run(DefaultgRPCDriver.bind(D1.bind()))
    replicas = ray.get(
        serve.context._global_client._controller._all_running_replicas.remote()
    )
    assert len(replicas["DefaultgRPCDriver"]) == 1

    worker_node = cluster.add_node(num_cpus=2)

    wait_for_condition(
        lambda: len(
            ray.get(
                serve.context._global_client._controller._all_running_replicas.remote()
            )["DefaultgRPCDriver"]
        )
        == 2
    )

    # Kill the worker node.
    cluster.remove_node(worker_node)

    wait_for_condition(
        lambda: len(
            ray.get(
                serve.context._global_client._controller._all_running_replicas.remote()
            )["DefaultgRPCDriver"]
        )
        == 1
    )


def test_schemas_attach_grpc_server():

    # Failed with initiate solely
    with pytest.raises(RayServeException):
        _ = gRPCIngress()

    class MyDriver(gRPCIngress):
        def __init__(self):
            super().__init__()

    # Failed with no schema gRPC binding function
    with pytest.raises(RayServeException):
        _ = MyDriver()


if __name__ == "__main__":
    import sys
    import pytest

    sys.exit(pytest.main(["-v", "-s", __file__]))
