# coding: utf-8
import pytest
import sys
import os
from ray.serve.drivers import DefaultgRPCDriver, gRPCIngress
import ray
from ray import serve
from ray.cluster_utils import Cluster
from ray.serve._private.constants import SERVE_NAMESPACE
from ray._private.test_utils import wait_for_condition, run_string_as_driver
from ray.serve.exceptions import RayServeException

from ray.serve._private.constants import (
    SERVE_DEFAULT_APP_NAME,
    DEPLOYMENT_NAME_PREFIX_SEPARATOR,
)

from unittest.mock import patch
from ray._private.test_utils import (
    setup_tls,
    teardown_tls,
)


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


@pytest.fixture
def use_tls(request):
    if request.param:
        key_filepath, cert_filepath, temp_dir = setup_tls()
    yield request.param
    if request.param:
        teardown_tls(key_filepath, cert_filepath, temp_dir)


def tls_enabled():
    return os.environ.get("RAY_USE_TLS", "0").lower() in ("1", "true")


@pytest.mark.skipif(
    sys.platform == "darwin",
    reason=("Cryptography (TLS dependency) doesn't install in Mac build pipeline"),
)
@pytest.mark.parametrize("use_tls", [True], indirect=True)
def test_deploy_basic(use_tls):
    if use_tls:
        run_string_as_driver(
            """
# coding: utf-8
import os
from ray.serve.drivers import DefaultgRPCDriver, gRPCIngress
import ray
from ray import serve
from ray.serve.generated import serve_pb2, serve_pb2_grpc
import grpc
from ray.serve.exceptions import RayServeException
from ray._private.tls_utils import load_certs_from_env
import logging
import asyncio
try:
    ray.init()
    @serve.deployment
    class D1:
        def __call__(self, input):
            return input["a"]

    serve.run(DefaultgRPCDriver.bind(D1.bind()))

    async def send_request():
        server_cert_chain, private_key, ca_cert = load_certs_from_env()
        credentials = grpc.ssl_channel_credentials(
            certificate_chain=server_cert_chain,
            private_key=private_key,
            root_certificates=ca_cert,
        )

        async with grpc.aio.secure_channel("localhost:9000", credentials) as channel:
            stub = serve_pb2_grpc.PredictAPIsServiceStub(channel)
            response = await stub.Predict(
                serve_pb2.PredictRequest(input={"a": bytes("123", "utf-8")})
            )
        return response

    resp = asyncio.run(send_request())
    assert resp.prediction == b"123"
finally:
    serve.shutdown()
    ray.shutdown()
        """,
            env=os.environ.copy(),
        )
    else:
        run_string_as_driver(
            """
# coding: utf-8
import os
from ray.serve.drivers import DefaultgRPCDriver, gRPCIngress
import ray
from ray import serve
from ray.serve.generated import serve_pb2, serve_pb2_grpc
import grpc
from ray.serve.exceptions import RayServeException
from ray._private.tls_utils import load_certs_from_env
import logging
import asyncio
try:
    ray.init()
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

    resp = asyncio.run(send_request())
    assert resp.prediction == b"123"
finally:
    serve.shutdown()
    ray.shutdown()
        """,
            env=os.environ.copy(),
        )


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
    deployment_name = (
        f"{SERVE_DEFAULT_APP_NAME}{DEPLOYMENT_NAME_PREFIX_SEPARATOR}DefaultgRPCDriver"
    )
    assert len(replicas[deployment_name]) == 1

    worker_node = cluster.add_node(num_cpus=2)

    wait_for_condition(
        lambda: len(
            ray.get(
                serve.context._global_client._controller._all_running_replicas.remote()
            )[deployment_name]
        )
        == 2
    )

    # Kill the worker node.
    cluster.remove_node(worker_node)

    wait_for_condition(
        lambda: len(
            ray.get(
                serve.context._global_client._controller._all_running_replicas.remote()
            )[deployment_name]
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
