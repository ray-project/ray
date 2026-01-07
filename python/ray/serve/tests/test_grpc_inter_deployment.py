"""Tests for gRPC inter-deployment communication.

This tests the feature where deployments can communicate with each other
via gRPC instead of Ray actor calls. This is enabled by setting
`_by_reference=False` on the deployment handle.
"""

import re
import sys

import pytest

from ray import serve
from ray._common.test_utils import SignalActor
from ray.serve._private.common import OBJ_REF_NOT_SUPPORTED_ERROR, RequestMetadata
from ray.serve._private.replica_result import ActorReplicaResult
from ray.serve._private.serialization import RPCSerializer
from ray.serve.handle import DeploymentHandle
from ray.serve.tests.conftest import *  # noqa
from ray.serve.tests.conftest import _shared_serve_instance  # noqa


class TestSerializationMethods:
    """Test different serialization methods for gRPC transport."""

    @pytest.mark.parametrize(
        "serialization_method",
        ["cloudpickle", "pickle", "msgpack", "orjson", "noop"],
    )
    def test_serializer_roundtrip(self, serialization_method):
        """Test that data can be serialized and deserialized correctly."""
        serializer = RPCSerializer(serialization_method, serialization_method)

        # Test with different data types
        if serialization_method == "noop":
            # noop only works with bytes
            test_data = b"hello world"
        elif serialization_method == "orjson":
            # orjson only works with JSON-serializable types
            test_data = {"key": "value", "number": 123, "list": [1, 2, 3]}
        else:
            # cloudpickle and pickle can handle most Python objects
            test_data = {"key": "value", "number": 123, "list": [1, 2, 3]}

        serialized = serializer.dumps_request(test_data)
        assert isinstance(serialized, bytes)

        deserialized = serializer.loads_request(serialized)
        assert deserialized == test_data

    def test_serializer_caching(self):
        """Test that serializers are cached and reused."""
        s1 = RPCSerializer.get_cached_serializer("cloudpickle", "cloudpickle")
        s2 = RPCSerializer.get_cached_serializer("cloudpickle", "cloudpickle")
        assert s1 is s2

        s3 = RPCSerializer.get_cached_serializer("pickle", "pickle")
        assert s1 is not s3


class TestRequestMetadataGRPCFields:
    """Test that RequestMetadata carries gRPC-related fields correctly."""

    def test_request_metadata_serialization_fields(self):
        """Test RequestMetadata includes serialization fields."""
        metadata = RequestMetadata(
            request_id="test-id",
            internal_request_id="internal-id",
            _by_reference=False,
            request_serialization="pickle",
            response_serialization="msgpack",
        )

        assert metadata._by_reference is False
        assert metadata.request_serialization == "pickle"
        assert metadata.response_serialization == "msgpack"

    def test_request_metadata_defaults(self):
        """Test RequestMetadata default values for new fields."""
        metadata = RequestMetadata(
            request_id="test-id",
            internal_request_id="internal-id",
        )

        # Default should be by-reference (Ray actor calls)
        assert metadata._by_reference is True
        assert metadata.request_serialization == "cloudpickle"
        assert metadata.response_serialization == "cloudpickle"


class TestRPCSerializerErrorHandling:
    """Test error handling in RPCSerializer."""

    def test_invalid_serialization_method(self):
        """Test that invalid serialization method raises error."""
        with pytest.raises(ValueError, match="Unsupported serialization method"):
            RPCSerializer("invalid_method", "cloudpickle")

    def test_orjson_non_json_data(self):
        """Test that orjson raises error for non-JSON-serializable data."""
        serializer = RPCSerializer("orjson", "orjson")
        # Lambda functions are not JSON-serializable
        with pytest.raises(TypeError):
            serializer.dumps_request(lambda x: x)

    def test_noop_non_bytes_data(self):
        """Test that noop raises error for non-bytes data."""
        serializer = RPCSerializer("noop", "noop")
        with pytest.raises((TypeError, AttributeError)):
            serializer.dumps_request("not bytes")


# ============================================================================
# Handle tests
# ============================================================================


@pytest.mark.parametrize(
    "by_reference,expected_result",
    [(True, ActorReplicaResult), (False, "gRPCReplicaResult")],
)
def test_init_by_reference(serve_instance, by_reference: bool, expected_result):
    from ray.serve._private.replica_result import gRPCReplicaResult

    @serve.deployment
    def f():
        return "hi"

    h = serve.run(f.bind())

    resp = h.options(_by_reference=by_reference).remote()
    assert resp.result() == "hi"
    if expected_result == "gRPCReplicaResult":
        assert isinstance(resp._replica_result, gRPCReplicaResult)
    else:
        assert isinstance(resp._replica_result, expected_result)


@pytest.mark.timeout(60)
async def test_by_reference_false_raises_error(serve_instance):
    signal = SignalActor.remote()

    @serve.deployment
    async def f():
        await signal.wait.remote()
        return "hi"

    h = serve.run(f.bind())
    with pytest.raises(
        RuntimeError, match=re.escape(OBJ_REF_NOT_SUPPORTED_ERROR.args[0])
    ):
        response = h.options(_by_reference=False).remote()
        await response._to_object_ref()

    await signal.send.remote()


@pytest.mark.parametrize("inner_by_reference", [True, False])
@pytest.mark.parametrize("outer_by_reference", [True, False])
def test_compose_deployments_in_app(
    serve_instance, inner_by_reference, outer_by_reference
):
    """Test composing deployment handle refs within a deployment."""

    @serve.deployment
    class Downstream:
        def __init__(self, msg: str):
            self._msg = msg

        def __call__(self, inp1: str, inp2: str):
            return f"{self._msg}|{inp1}|{inp2}"

    @serve.deployment
    class Deployment:
        def __init__(self, handle1: DeploymentHandle, handle2: DeploymentHandle):
            self._handle1 = handle1.options(_by_reference=outer_by_reference)
            self._handle2 = handle2.options(_by_reference=inner_by_reference)

        async def __call__(self):
            result = await self._handle1.remote(
                self._handle2.remote("hi1", inp2="hi2"),
                inp2=self._handle2.remote("hi3", inp2="hi4"),
            )
            return f"driver|{result}"

    handle = serve.run(
        Deployment.bind(
            Downstream.options(name="downstream1").bind("downstream1"),
            Downstream.options(name="downstream2").bind("downstream2"),
        ),
    )
    assert (
        handle.remote().result()
        == "driver|downstream1|downstream2|hi1|hi2|downstream2|hi3|hi4"
    )


@pytest.mark.parametrize("inner_by_reference", [True, False])
@pytest.mark.parametrize("outer_by_reference", [True, False])
def test_compose_apps(serve_instance, inner_by_reference, outer_by_reference):
    """Test composing deployment handle refs outside of a deployment."""

    @serve.deployment
    class Deployment:
        def __init__(self, msg: str):
            self._msg = msg

        def __call__(self, inp1: str, inp2: str):
            return f"{self._msg}|{inp1}|{inp2}"

    handle1 = serve.run(
        Deployment.bind("app1"), name="app1", route_prefix="/app1"
    ).options(_by_reference=outer_by_reference)
    handle2 = serve.run(
        Deployment.bind("app2"), name="app2", route_prefix="/app2"
    ).options(_by_reference=inner_by_reference)

    assert (
        handle1.remote(
            handle2.remote("hi1", inp2="hi2"),
            inp2=handle2.remote("hi3", inp2="hi4"),
        ).result()
        == "app1|app2|hi1|hi2|app2|hi3|hi4"
    )


# ============================================================================
# E2E tests
# ============================================================================


def test_custom_serialization_method(serve_instance):
    @serve.deployment
    class Downstream:
        def __call__(self, message: str):
            return f"Hello {message}!"

    h = serve.run(Downstream.bind())
    assert (
        h.options(
            _by_reference=False,
            request_serialization="pickle",
            response_serialization="pickle",
        )
        .remote("world1")
        .result()
        == "Hello world1!"
    )

    assert (
        h.options(
            _by_reference=False,
            request_serialization="pickle",
            response_serialization="cloudpickle",
        )
        .remote("world2")
        .result()
        == "Hello world2!"
    )


@serve.deployment
class Downstream:
    def __call__(self):
        return "hi"


@serve.deployment
class Ingress:
    def __init__(self, handle):
        self._handle = handle

    async def __call__(self):
        return await self._handle.options(_by_reference=False).remote()


def test_basic_grpc_transport(serve_instance):
    """Test basic gRPC transport between deployments."""
    h = serve.run(Ingress.bind(Downstream.bind()))

    for _ in range(10):
        assert h.options(_by_reference=False).remote().result() == "hi"


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
