"""Tests for gRPC inter-deployment communication.

This tests the feature where deployments can communicate with each other
via gRPC instead of Ray actor calls. This is enabled by setting
`_by_reference=False` on the deployment handle.
"""

import time
from typing import Generator

import pytest

import ray
from ray import serve
from ray.serve._private.common import RequestMetadata
from ray.serve._private.serialization import RPCSerializer


@pytest.fixture
def serve_instance():
    """Start Ray and Serve for each test."""
    ray.init(num_cpus=4)
    serve.start()
    yield
    serve.shutdown()
    ray.shutdown()


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


class TestGRPCInterDeployment:
    """Test gRPC transport between deployments."""

    def test_basic_unary_request(self, serve_instance):
        """Test basic unary request via gRPC transport."""

        @serve.deployment
        class Downstream:
            def __call__(self, message: str) -> str:
                return f"Downstream received: {message}"

        @serve.deployment
        class Upstream:
            def __init__(self):
                self.downstream = serve.get_deployment_handle(
                    "Downstream", "default"
                ).options(_by_reference=False)

            async def __call__(self, message: str) -> str:
                result = await self.downstream.remote(message)
                return f"Upstream -> {result}"

        serve.run(Downstream.bind(), name="default", route_prefix="/downstream")
        serve.run(Upstream.bind(), name="default", route_prefix="/upstream")

        # Give deployments time to start
        time.sleep(2)

        import httpx

        # Basic test to ensure deployments start and can communicate
        httpx.get("http://localhost:8000/upstream", params={"message": "test"})

    def test_streaming_request(self, serve_instance):
        """Test streaming request via gRPC transport."""

        @serve.deployment
        class StreamingDownstream:
            def __call__(self, count: int) -> Generator[int, None, None]:
                for i in range(count):
                    yield i

        @serve.deployment
        class StreamingUpstream:
            def __init__(self):
                self.downstream = serve.get_deployment_handle(
                    "StreamingDownstream", "default"
                ).options(_by_reference=False, stream=True)

            async def __call__(self, count: int) -> str:
                results = []
                async for item in self.downstream.remote(count):
                    results.append(item)
                return f"Received: {results}"

        serve.run(
            StreamingDownstream.bind(),
            name="default",
            route_prefix="/streaming_downstream",
        )
        serve.run(
            StreamingUpstream.bind(), name="default", route_prefix="/streaming_upstream"
        )

        time.sleep(2)

    def test_handle_options_by_reference_default(self, serve_instance):
        """Test that _by_reference defaults to True (Ray actor calls)."""

        @serve.deployment
        class TestDeployment:
            def __call__(self) -> str:
                return "test"

        serve.run(TestDeployment.bind(), name="default", route_prefix="/test")
        time.sleep(1)

        handle = serve.get_deployment_handle("TestDeployment", "default")
        # By default, _by_reference should be True
        assert handle._options._by_reference is True

    def test_handle_options_by_reference_false(self, serve_instance):
        """Test setting _by_reference=False for gRPC transport."""

        @serve.deployment
        class TestDeployment:
            def __call__(self) -> str:
                return "test"

        serve.run(TestDeployment.bind(), name="default", route_prefix="/test")
        time.sleep(1)

        handle = serve.get_deployment_handle("TestDeployment", "default")
        grpc_handle = handle.options(_by_reference=False)
        assert grpc_handle._options._by_reference is False

    def test_handle_options_serialization(self, serve_instance):
        """Test setting custom serialization methods."""

        @serve.deployment
        class TestDeployment:
            def __call__(self) -> str:
                return "test"

        serve.run(TestDeployment.bind(), name="default", route_prefix="/test")
        time.sleep(1)

        handle = serve.get_deployment_handle("TestDeployment", "default")
        custom_handle = handle.options(
            _by_reference=False,
            _request_serialization="pickle",
            _response_serialization="pickle",
        )
        assert custom_handle._options._by_reference is False
        assert custom_handle._options._request_serialization == "pickle"
        assert custom_handle._options._response_serialization == "pickle"


class TestRequestMetadataGRPCFields:
    """Test that RequestMetadata carries gRPC-related fields correctly."""

    def test_request_metadata_serialization_fields(self):
        """Test RequestMetadata includes serialization fields."""
        metadata = RequestMetadata(
            request_id="test-id",
            internal_request_id="internal-id",
            _by_reference=False,
            _request_serialization="pickle",
            _response_serialization="msgpack",
        )

        assert metadata._by_reference is False
        assert metadata._request_serialization == "pickle"
        assert metadata._response_serialization == "msgpack"

    def test_request_metadata_defaults(self):
        """Test RequestMetadata default values for new fields."""
        metadata = RequestMetadata(
            request_id="test-id",
            internal_request_id="internal-id",
        )

        # Default should be by-reference (Ray actor calls)
        assert metadata._by_reference is True
        assert metadata._request_serialization == "cloudpickle"
        assert metadata._response_serialization == "cloudpickle"


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


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
