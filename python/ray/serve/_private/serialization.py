import logging
import pickle
from typing import Any, Dict, Tuple

from ray import cloudpickle
from ray.serve._private.constants import SERVE_LOGGER_NAME

try:
    import orjson
except ImportError:
    orjson = None
try:
    import ormsgpack
except ImportError:
    ormsgpack = None


logger = logging.getLogger(SERVE_LOGGER_NAME)


class SerializationMethod:
    """Available serialization methods for RPC communication."""

    CLOUDPICKLE = "cloudpickle"
    PICKLE = "pickle"
    MSGPACK = "msgpack"
    ORJSON = "orjson"
    NOOP = "noop"


# Global cache for serializer instances to avoid per-request instantiation overhead
_serializer_cache: Dict[Tuple[str, str], "RPCSerializer"] = {}


class RPCSerializer:
    """Serializer for RPC communication with configurable serialization methods."""

    def __init__(
        self,
        request_method: str = SerializationMethod.CLOUDPICKLE,
        response_method: str = SerializationMethod.CLOUDPICKLE,
    ):
        self.request_method = request_method.lower()
        self.response_method = response_method.lower()
        self._validate_methods()
        self._setup_serializers()

    @classmethod
    def get_cached_serializer(
        cls,
        request_method: str = SerializationMethod.CLOUDPICKLE,
        response_method: str = SerializationMethod.CLOUDPICKLE,
    ) -> "RPCSerializer":
        """Get a cached serializer instance to avoid per-request instantiation overhead.

        This method maintains a cache of serializer instances based on
        (request_method, response_method) pairs, significantly reducing overhead
        in high-throughput systems.
        """
        # Normalize method names
        req_method = request_method.lower()
        resp_method = response_method.lower()
        cache_key = (req_method, resp_method)

        if cache_key not in _serializer_cache:
            _serializer_cache[cache_key] = cls(req_method, resp_method)

        return _serializer_cache[cache_key]

    def _validate_methods(self):
        """Validate that the serialization methods are supported."""
        valid_methods = {
            SerializationMethod.CLOUDPICKLE,
            SerializationMethod.PICKLE,
            SerializationMethod.MSGPACK,
            SerializationMethod.ORJSON,
            SerializationMethod.NOOP,
        }

        if self.request_method not in valid_methods:
            raise ValueError(
                f"Unsupported request serialization method: {self.request_method}. "
                f"Valid options: {valid_methods}"
            )

        if self.response_method not in valid_methods:
            raise ValueError(
                f"Unsupported response serialization method: {self.response_method}. "
                f"Valid options: {valid_methods}"
            )

    def _setup_serializers(self):
        """Setup the serialization functions based on the selected methods."""
        self._request_dumps, self._request_loads = self._get_serializer_funcs(
            self.request_method
        )
        self._response_dumps, self._response_loads = self._get_serializer_funcs(
            self.response_method
        )

    def _get_serializer_funcs(self, method: str) -> Tuple[Any, Any]:
        """Get dumps and loads functions for a given serialization method."""
        if method == SerializationMethod.CLOUDPICKLE:
            return cloudpickle.dumps, cloudpickle.loads
        elif method == SerializationMethod.PICKLE:
            return self._get_pickle_funcs()
        elif method == SerializationMethod.MSGPACK:
            return self._get_msgpack_funcs()
        elif method == SerializationMethod.ORJSON:
            return self._get_orjson_funcs()
        elif method == SerializationMethod.NOOP:
            return self._get_noop_funcs()

    def _get_noop_funcs(self) -> Tuple[Any, Any]:
        """Get no-op serialization functions for binary data."""

        def _noop_dumps(obj: Any) -> bytes:
            if not isinstance(obj, bytes):
                raise TypeError(
                    f"a bytes-like object is required, got {type(obj).__name__}. "
                    "Use a different serialization method for non-binary data."
                )
            return obj

        def _noop_loads(data: bytes) -> Any:
            if not isinstance(data, bytes):
                raise TypeError(
                    f"a bytes-like object is required, got {type(data).__name__}. "
                    "Use a different serialization method for non-binary data."
                )
            return data

        return _noop_dumps, _noop_loads

    def _get_pickle_funcs(self) -> Tuple[Any, Any]:
        """Get pickle serialization functions with highest protocol."""

        def _pickle_dumps(obj: Any) -> bytes:
            return pickle.dumps(obj, protocol=pickle.HIGHEST_PROTOCOL)

        def _pickle_loads(data: bytes) -> Any:
            return pickle.loads(data)

        return _pickle_dumps, _pickle_loads

    def _get_msgpack_funcs(self) -> Tuple[Any, Any]:
        """Get msgpack serialization functions."""

        if ormsgpack is None:
            raise ImportError(
                "ormsgpack is not installed. Please install it with `pip install ormsgpack`."
            )

        # Configure ormsgpack with appropriate options
        def _msgpack_dumps(obj: Any) -> bytes:
            return ormsgpack.packb(obj)

        def _msgpack_loads(data: bytes) -> Any:
            return ormsgpack.unpackb(data)

        return _msgpack_dumps, _msgpack_loads

    def _get_orjson_funcs(self) -> Tuple[Any, Any]:
        """Get orjson serialization functions."""

        if orjson is None:
            raise ImportError(
                "orjson is not installed. Please install it with `pip install orjson`."
            )

        # orjson only supports JSON-serializable types
        def _orjson_dumps(obj: Any) -> bytes:
            try:
                return orjson.dumps(obj)
            except TypeError as e:
                raise TypeError(
                    f"orjson serialization failed: {e}. "
                    "Only JSON-serializable types are supported with orjson. "
                    "Consider using 'cloudpickle' or 'pickle' for complex objects."
                )

        def _orjson_loads(data: bytes) -> Any:
            return orjson.loads(data)

        return _orjson_dumps, _orjson_loads

    def dumps_request(self, obj: Any) -> bytes:
        """Serialize a request object to bytes."""
        return self._request_dumps(obj)

    def loads_request(self, data: bytes) -> Any:
        """Deserialize bytes to a request object."""
        return self._request_loads(data)

    def dumps_response(self, obj: Any) -> bytes:
        """Serialize a response object to bytes."""
        return self._response_dumps(obj)

    def loads_response(self, data: bytes) -> Any:
        """Deserialize bytes to a response object."""
        return self._response_loads(data)


def clear_serializer_cache():
    """Clear the cached serializer instances. Useful for testing or memory management."""
    global _serializer_cache
    _serializer_cache.clear()
