import pytest

from ray.serve.drivers import gRPCDriver
import ray
from ray import serve
import os
from ray.serve.generated import serve_pb2, serve_pb2_grpc
import grpc
from ray.cluster_utils import Cluster
from ray.serve._private.constants import SERVE_NAMESPACE


pytestmark = pytest.mark.asyncio


@pytest.fixture
def serve_start_shutdown():
    os.environ["EXPERIMENT_DISABLE_HTTP_PROXY"] = "1"
    ray.init()
    yield
    serve.shutdown()
    ray.shutdown()


@pytest.fixture
def ray_cluster():
    os.environ["EXPERIMENT_DISABLE_HTTP_PROXY"] = "1"
    cluster = Cluster()
    yield Cluster()
    serve.shutdown()
    ray.shutdown()
    cluster.shutdown()


async def test_controller_basic(serve_start_shutdown):
    @serve.deployment
    class D1:
        def __call__(self, input):
            return input["a"]

    serve.run(gRPCDriver.bind(D1.bind()))

    async def send_request():
        async with grpc.aio.insecure_channel("localhost:9000") as channel:
            stub = serve_pb2_grpc.PredictAPIsServiceStub(channel)
            response = await stub.Predict(
                serve_pb2.PredictRequest(input={"a": bytes("123", "utf-8")})
            )
        return response

    resp = await send_request()

    assert resp.prediction == b"123"


def test_controller_without_http(serve_start_shutdown):
    @serve.deployment
    class D1:
        def __call__(self, input):
            return input["a"]

    serve.run(gRPCDriver.bind(D1.bind()))
    assert (
        ray.get(serve.context._global_client._controller.get_http_proxies.remote())
        == {}
    )


def test_deploy_grpc_driver_to_node(ray_cluster):
    cluster = ray_cluster
    cluster.add_node(num_cpus=2)
    cluster.connect(namespace=SERVE_NAMESPACE)

    @serve.deployment
    class D1:
        def __call__(self, input):
            return input["a"]

    serve.run(gRPCDriver.bind(D1.bind()))
    replicas = ray.get(
        serve.context._global_client._controller._all_running_replicas.remote()
    )
    assert len(replicas) == 2


if __name__ == "__main__":
    import sys
    import pytest

    sys.exit(pytest.main(["-v", "-s", __file__]))
