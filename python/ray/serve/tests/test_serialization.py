import sys

import numpy as np
import pandas as pd
import pytest

from ray.serve._private.serialization import (
    RPCSerializer,
    SerializationMethod,
    _serializer_cache,
    clear_serializer_cache,
)


class TestRPCSerializer:
    """Tests for RPCSerializer class."""

    def test_rpc_serializer_default_initialization(self):
        """Test RPCSerializer with default parameters."""
        serializer = RPCSerializer()
        assert serializer.request_method == SerializationMethod.CLOUDPICKLE
        assert serializer.response_method == SerializationMethod.CLOUDPICKLE

    def test_rpc_serializer_custom_initialization(self):
        """Test RPCSerializer with custom parameters."""
        serializer = RPCSerializer(
            request_method=SerializationMethod.PICKLE,
            response_method=SerializationMethod.CLOUDPICKLE,
        )
        assert serializer.request_method == SerializationMethod.PICKLE
        assert serializer.response_method == SerializationMethod.CLOUDPICKLE

    def test_rpc_serializer_case_insensitive(self):
        """Test RPCSerializer handles case-insensitive method names."""
        serializer = RPCSerializer(
            request_method="CLOUDPICKLE", response_method="PICKLE"
        )
        assert serializer.request_method == SerializationMethod.CLOUDPICKLE
        assert serializer.response_method == SerializationMethod.PICKLE

    def test_invalid_serialization_method(self):
        """Test RPCSerializer raises ValueError for invalid methods."""
        with pytest.raises(
            ValueError, match="Unsupported request serialization method"
        ):
            RPCSerializer(request_method="invalid_method")

        with pytest.raises(
            ValueError, match="Unsupported response serialization method"
        ):
            RPCSerializer(response_method="invalid_method")

    def test_cloudpickle_serialization(self):
        """Test cloudpickle serialization with various data types."""
        serializer = RPCSerializer(
            request_method=SerializationMethod.CLOUDPICKLE,
            response_method=SerializationMethod.CLOUDPICKLE,
        )

        test_cases = [
            42,
            "hello world",
            [1, 2, 3],
            {"key": "value"},
            lambda x: x + 1,  # Functions work with cloudpickle
            np.array([1, 2, 3]),
        ]

        for test_data in test_cases:
            # Test request serialization
            serialized = serializer.dumps_request(test_data)
            assert isinstance(serialized, bytes)
            deserialized = serializer.loads_request(serialized)

            if callable(test_data):
                assert callable(deserialized)
                assert deserialized(5) == test_data(5)
            elif isinstance(test_data, np.ndarray):
                np.testing.assert_array_equal(deserialized, test_data)
            else:
                assert deserialized == test_data

            # Test response serialization
            serialized = serializer.dumps_response(test_data)
            assert isinstance(serialized, bytes)
            deserialized = serializer.loads_response(serialized)

            if callable(test_data):
                assert callable(deserialized)
                assert deserialized(5) == test_data(5)
            elif isinstance(test_data, np.ndarray):
                np.testing.assert_array_equal(deserialized, test_data)
            else:
                assert deserialized == test_data

    def test_pickle_serialization(self):
        """Test pickle serialization with various data types."""
        serializer = RPCSerializer(
            request_method=SerializationMethod.PICKLE,
            response_method=SerializationMethod.PICKLE,
        )

        test_cases = [
            42,
            "hello world",
            [1, 2, 3],
            {"key": "value"},
            np.array([1, 2, 3]),
            pd.DataFrame({"a": [1, 2], "b": [3, 4]}),
        ]

        for test_data in test_cases:
            # Test request serialization
            serialized = serializer.dumps_request(test_data)
            assert isinstance(serialized, bytes)
            deserialized = serializer.loads_request(serialized)

            if isinstance(test_data, np.ndarray):
                np.testing.assert_array_equal(deserialized, test_data)
            elif isinstance(test_data, pd.DataFrame):
                pd.testing.assert_frame_equal(deserialized, test_data)
            else:
                assert deserialized == test_data

    def test_msgpack_serialization(self):
        """Test msgpack serialization with supported data types."""
        try:
            import ormsgpack  # noqa: F401
        except ImportError:
            pytest.skip("ormsgpack is not installed. Skipping msgpack tests.")
            return

        serializer = RPCSerializer(
            request_method=SerializationMethod.MSGPACK,
            response_method=SerializationMethod.MSGPACK,
        )

        test_cases = [
            42,
            "hello world",
            [1, 2, 3],
            {"key": "value"},
            True,
            None,
            3.14,
        ]

        for test_data in test_cases:
            # Test request serialization
            serialized = serializer.dumps_request(test_data)
            assert isinstance(serialized, bytes)
            deserialized = serializer.loads_request(serialized)
            assert deserialized == test_data

            # Test response serialization
            serialized = serializer.dumps_response(test_data)
            assert isinstance(serialized, bytes)
            deserialized = serializer.loads_response(serialized)
            assert deserialized == test_data

    def test_orjson_serialization(self):
        """Test orjson serialization with JSON-compatible data types."""
        try:
            import orjson  # noqa: F401
        except ImportError:
            pytest.skip("orjson is not installed. Skipping orjson tests.")
            return

        serializer = RPCSerializer(
            request_method=SerializationMethod.ORJSON,
            response_method=SerializationMethod.ORJSON,
        )

        test_cases = [
            42,
            "hello world",
            [1, 2, 3],
            {"key": "value"},
            True,
            None,
            3.14,
        ]

        for test_data in test_cases:
            # Test request serialization
            serialized = serializer.dumps_request(test_data)
            assert isinstance(serialized, bytes)
            deserialized = serializer.loads_request(serialized)
            assert deserialized == test_data

            # Test response serialization
            serialized = serializer.dumps_response(test_data)
            assert isinstance(serialized, bytes)
            deserialized = serializer.loads_response(serialized)
            assert deserialized == test_data

    def test_orjson_unsupported_types(self):
        """Test orjson raises TypeError for unsupported types."""
        try:
            import orjson  # noqa: F401
        except ImportError:
            pytest.skip("orjson is not installed. Skipping orjson tests.")
            return

        serializer = RPCSerializer(
            request_method=SerializationMethod.ORJSON,
            response_method=SerializationMethod.ORJSON,
        )

        # Lambda functions are not JSON-serializable
        with pytest.raises(TypeError, match="orjson serialization failed"):
            serializer.dumps_request(lambda x: x)

        # Custom objects are not JSON-serializable
        class CustomObject:
            pass

        with pytest.raises(TypeError, match="orjson serialization failed"):
            serializer.dumps_request(CustomObject())

    def test_noop_serialization(self):
        """Test noop serialization with binary data."""
        serializer = RPCSerializer(
            request_method=SerializationMethod.NOOP,
            response_method=SerializationMethod.NOOP,
        )

        test_data = b"binary data"

        # Test request serialization
        serialized = serializer.dumps_request(test_data)
        assert serialized == test_data
        deserialized = serializer.loads_request(serialized)
        assert deserialized == test_data

        # Test response serialization
        serialized = serializer.dumps_response(test_data)
        assert serialized == test_data
        deserialized = serializer.loads_response(serialized)
        assert deserialized == test_data

    def test_noop_serialization_error(self):
        """Test noop serialization raises error for non-bytes."""
        serializer = RPCSerializer(
            request_method=SerializationMethod.NOOP,
            response_method=SerializationMethod.NOOP,
        )

        with pytest.raises(
            TypeError,
            match="a bytes-like object is required, got str. "
            "Use a different serialization method for non-binary data.",
        ):
            serializer.dumps_request("not bytes")

    def test_mixed_serialization_methods(self):
        """Test using different methods for request and response."""

        serializer = RPCSerializer(
            request_method=SerializationMethod.CLOUDPICKLE,
            response_method=SerializationMethod.PICKLE,
        )

        # JSON-compatible request
        request_data = {"key": "value"}
        serialized_request = serializer.dumps_request(request_data)
        deserialized_request = serializer.loads_request(serialized_request)
        assert deserialized_request == request_data

        # Complex response (needs pickle)
        response_data = np.array([1, 2, 3])
        serialized_response = serializer.dumps_response(response_data)
        deserialized_response = serializer.loads_response(serialized_response)
        np.testing.assert_array_equal(deserialized_response, response_data)

    def test_serialization_error_logging(self):
        """Test that deserialization errors are logged properly."""
        serializer = RPCSerializer(
            request_method=SerializationMethod.PICKLE,
            response_method=SerializationMethod.PICKLE,
        )

        # Invalid pickle data
        invalid_data = b"invalid pickle data"

        with pytest.raises(Exception):
            serializer.loads_request(invalid_data)

        with pytest.raises(Exception):
            serializer.loads_response(invalid_data)

    def test_cached_serializer(self):
        """Test the cached serializer functionality."""

        # Clear cache first
        clear_serializer_cache()

        # First call should create new instance
        serializer1 = RPCSerializer.get_cached_serializer(
            SerializationMethod.PICKLE, SerializationMethod.PICKLE
        )
        assert len(_serializer_cache) == 1

        # Second call with same params should return cached instance
        serializer2 = RPCSerializer.get_cached_serializer(
            SerializationMethod.PICKLE, SerializationMethod.PICKLE
        )
        assert serializer1 is serializer2
        assert len(_serializer_cache) == 1

        # Different params should create new instance
        serializer3 = RPCSerializer.get_cached_serializer(
            SerializationMethod.CLOUDPICKLE, SerializationMethod.CLOUDPICKLE
        )
        assert serializer3 is not serializer1
        assert len(_serializer_cache) == 2

    def test_cached_serializer_case_normalization(self):
        """Test that cached serializer normalizes case properly."""
        clear_serializer_cache()

        # Test with different case variations
        serializer1 = RPCSerializer.get_cached_serializer("PICKLE", "PICKLE")
        serializer2 = RPCSerializer.get_cached_serializer("pickle", "pickle")
        serializer3 = RPCSerializer.get_cached_serializer("Pickle", "Pickle")

        assert serializer1 is serializer2
        assert serializer2 is serializer3
        assert len(_serializer_cache) == 1


class TestSerializationIntegration:
    """Integration tests for serialization functionality."""

    def test_real_world_data_serialization(self):
        """Test serialization with real-world data structures."""
        # Create a complex data structure
        complex_data = {
            "user_id": 12345,
            "username": "testuser",
            "profile": {
                "age": 30,
                "preferences": ["sci-fi", "fantasy", "mystery"],
                "scores": np.array([95.5, 87.2, 92.8]),
                "metadata": {
                    "created_at": "2023-01-01",
                    "last_login": "2023-12-01",
                    "is_active": True,
                },
            },
            "data_frame": pd.DataFrame(
                {
                    "name": ["Alice", "Bob", "Charlie"],
                    "age": [25, 30, 35],
                    "salary": [50000, 60000, 70000],
                }
            ),
        }

        # Test with cloudpickle (should handle everything)
        serializer = RPCSerializer(
            request_method=SerializationMethod.CLOUDPICKLE,
            response_method=SerializationMethod.CLOUDPICKLE,
        )

        serialized = serializer.dumps_request(complex_data)
        deserialized = serializer.loads_request(serialized)

        assert deserialized["user_id"] == complex_data["user_id"]
        assert deserialized["username"] == complex_data["username"]
        assert deserialized["profile"]["age"] == complex_data["profile"]["age"]
        np.testing.assert_array_equal(
            deserialized["profile"]["scores"], complex_data["profile"]["scores"]
        )
        pd.testing.assert_frame_equal(
            deserialized["data_frame"], complex_data["data_frame"]
        )

    def test_serialization_with_edge_cases(self):
        """Test serialization with edge cases and corner cases."""
        edge_cases = [
            None,
            "",
            [],
            {},
            0,
            False,
            float("inf"),
            float("-inf"),
            b"",
            np.array([]),
            pd.DataFrame(),
        ]

        for method in [SerializationMethod.CLOUDPICKLE, SerializationMethod.PICKLE]:
            serializer = RPCSerializer(request_method=method, response_method=method)

            for test_data in edge_cases:
                serialized = serializer.dumps_request(test_data)
                deserialized = serializer.loads_request(serialized)

                if isinstance(test_data, np.ndarray):
                    np.testing.assert_array_equal(deserialized, test_data)
                elif isinstance(test_data, pd.DataFrame):
                    pd.testing.assert_frame_equal(deserialized, test_data)
                else:
                    assert deserialized == test_data or (
                        test_data != test_data
                        and deserialized != deserialized  # NaN case
                    )

    def test_concurrent_serialization(self):
        """Test that serialization works correctly in concurrent scenarios."""
        import threading

        serializer = RPCSerializer(
            request_method=SerializationMethod.PICKLE,
            response_method=SerializationMethod.PICKLE,
        )

        results = []
        errors = []

        def serialize_worker(worker_id):
            try:
                test_data = {"worker_id": worker_id, "data": list(range(100))}
                serialized = serializer.dumps_request(test_data)
                deserialized = serializer.loads_request(serialized)
                results.append((worker_id, deserialized))
            except Exception as e:
                errors.append((worker_id, e))

        # Start multiple threads
        threads = []
        for i in range(10):
            thread = threading.Thread(target=serialize_worker, args=(i,))
            threads.append(thread)
            thread.start()

        # Wait for all threads
        for thread in threads:
            thread.join()

        # Check results
        assert len(errors) == 0, f"Errors occurred: {errors}"
        assert len(results) == 10

        for worker_id, result in results:
            assert result["worker_id"] == worker_id
            assert result["data"] == list(range(100))


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
