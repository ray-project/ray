"""Authentication tests for the replica's inter-deployment gRPC server.

Every Serve replica starts an internal ``ASGIService`` gRPC server that carries
handle-path (inter-deployment) traffic. That server must be created through
Ray's shared gRPC helpers so the authentication interceptor is attached when
``RAY_AUTH_MODE=token`` is set. Without it the server accepts calls from any
peer that can reach the port and unpickles the attacker-supplied
``pickled_request_metadata`` field before any validation runs.

The server and the client (the router's channel to the replica) must stay in
sync: attaching the interceptor without also sending the token from the client
would reject all legitimate inter-deployment traffic, so
``test_inter_deployment_traffic_still_works`` covers the other half.
"""

import sys

import grpc
import pytest

import ray
from ray import serve
from ray._private.authentication.authentication_token_generator import (
    generate_new_authentication_token,
)
from ray._private.authentication_test_utils import (
    authentication_env_guard,
    reset_auth_token_state,
    set_auth_mode,
    set_env_auth_token,
)
from ray.serve._private.common import DeploymentID
from ray.serve._private.constants import SERVE_CONTROLLER_NAME, SERVE_NAMESPACE
from ray.serve.generated import serve_pb2, serve_pb2_grpc

# A payload that is not a valid pickle. If the auth interceptor is missing, the
# replica reaches `pickle.loads()` on this field and fails with some non-auth
# status code, which is exactly what these tests distinguish against.
_UNPICKLABLE_PAYLOAD = b"this-is-not-a-pickle"


@serve.deployment
class Downstream:
    def __call__(self) -> str:
        return "hello"


@serve.deployment
class Ingress:
    def __init__(self, handle):
        self._handle = handle

    async def __call__(self) -> str:
        # `_by_reference=False` sends the request over the downstream replica's
        # inter-deployment gRPC server instead of the Ray actor call path.
        return await self._handle.options(_by_reference=False).remote()


@pytest.fixture(scope="module")
def serve_instance_with_token_auth():
    """Start a Ray cluster and Serve instance with token authentication on."""
    token = generate_new_authentication_token()
    with authentication_env_guard():
        set_auth_mode("token")
        set_env_auth_token(token)
        reset_auth_token_state()

        ray.init(
            address="local",
            num_cpus=8,
            namespace="test_replica_grpc_auth",
        )
        serve.start()
        try:
            yield token
        finally:
            serve.shutdown()
            ray.shutdown()
            reset_auth_token_state()


@pytest.fixture(scope="module")
def downstream_replica_address(serve_instance_with_token_auth):
    """Deploy the app and return the downstream replica's gRPC address."""
    serve.run(Ingress.bind(Downstream.bind()), name="auth_test_app")

    controller = ray.get_actor(SERVE_CONTROLLER_NAME, namespace=SERVE_NAMESPACE)
    all_replicas = ray.get(controller._all_running_replicas.remote())
    replicas = all_replicas[DeploymentID(name="Downstream", app_name="auth_test_app")]
    assert len(replicas) == 1, replicas

    info = replicas[0]
    assert info.port is not None, "replica did not report an inter-deployment gRPC port"
    return f"{info.node_ip}:{info.port}"


def _call_handle_request(address: str, metadata=None):
    """Call ASGIService.HandleRequest over a raw channel, bypassing Ray's client.

    This is what an unauthorized peer with network access to the replica port
    can do; the channel deliberately does not go through ``init_grpc_channel``
    so no token is attached unless ``metadata`` supplies one.
    """
    with grpc.insecure_channel(address) as channel:
        stub = serve_pb2_grpc.ASGIServiceStub(channel)
        request = serve_pb2.ASGIRequest(
            pickled_request_metadata=_UNPICKLABLE_PAYLOAD,
        )
        return stub.HandleRequest(request, metadata=metadata, timeout=10)


@pytest.mark.parametrize("with_token", [False, True], ids=["no_token", "wrong_token"])
def test_unauthenticated_call_is_rejected(downstream_replica_address, with_token):
    """A call with a missing or wrong token must be rejected before unpickling."""
    metadata = None
    if with_token:
        wrong_token = generate_new_authentication_token()
        metadata = (("authorization", f"Bearer {wrong_token}"),)

    with pytest.raises(grpc.RpcError) as exc_info:
        _call_handle_request(downstream_replica_address, metadata=metadata)

    assert exc_info.value.code() == grpc.StatusCode.UNAUTHENTICATED


def test_valid_token_passes_authentication(
    downstream_replica_address, serve_instance_with_token_auth
):
    """A correctly-tokenned call must get past the interceptor.

    The payload is still garbage, so the call fails — but it must fail inside
    the servicer rather than with UNAUTHENTICATED, which is what proves the
    interceptor is not rejecting legitimate callers.
    """
    token = serve_instance_with_token_auth
    with pytest.raises(grpc.RpcError) as exc_info:
        _call_handle_request(
            downstream_replica_address,
            metadata=(("authorization", f"Bearer {token}"),),
        )

    assert exc_info.value.code() != grpc.StatusCode.UNAUTHENTICATED


def test_inter_deployment_traffic_still_works(downstream_replica_address):
    """Legitimate handle-path gRPC traffic must keep working under token auth."""
    handle = serve.get_app_handle("auth_test_app")
    assert handle.remote().result() == "hello"


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
