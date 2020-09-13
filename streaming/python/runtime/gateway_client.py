# -*- coding: UTF-8 -*-
"""Module to interact between java and python
"""

import msgpack
import ray


class GatewayClient:
    """GatewayClient is used to interact with `PythonGateway` java actor"""

    _PYTHON_GATEWAY_CLASSNAME = \
        b"io.ray.streaming.runtime.python.PythonGateway"

    def __init__(self):
        self._python_gateway_actor = ray.java_actor_class(
            GatewayClient._PYTHON_GATEWAY_CLASSNAME).remote()

    def create_streaming_context(self):
        call = self._python_gateway_actor.createStreamingContext.remote()
        return deserialize(ray.get(call))

    def with_config(self, conf):
        call = self._python_gateway_actor.withConfig.remote(serialize(conf))
        ray.get(call)

    def execute(self, job_name):
        call = self._python_gateway_actor.execute.remote(serialize(job_name))
        ray.get(call)

    def create_py_stream_source(self, serialized_func):
        assert isinstance(serialized_func, bytes)
        call = self._python_gateway_actor.createPythonStreamSource \
            .remote(serialized_func)
        return deserialize(ray.get(call))

    def create_py_func(self, serialized_func):
        assert isinstance(serialized_func, bytes)
        call = self._python_gateway_actor.createPyFunc.remote(serialized_func)
        return deserialize(ray.get(call))

    def create_py_partition(self, serialized_partition):
        assert isinstance(serialized_partition, bytes)
        call = self._python_gateway_actor.createPyPartition \
            .remote(serialized_partition)
        return deserialize(ray.get(call))

    def union(self, *streams):
        serialized_streams = serialize(streams)
        call = self._python_gateway_actor.union \
            .remote(serialized_streams)
        return deserialize(ray.get(call))

    def call_function(self, java_class, java_function, *args):
        java_params = serialize([java_class, java_function] + list(args))
        call = self._python_gateway_actor.callFunction.remote(java_params)
        return deserialize(ray.get(call))

    def call_method(self, java_object, java_method, *args):
        java_params = serialize([java_object, java_method] + list(args))
        call = self._python_gateway_actor.callMethod.remote(java_params)
        return deserialize(ray.get(call))

    def new_instance(self, java_class_name):
        call = self._python_gateway_actor.newInstance.remote(
            serialize(java_class_name))
        return deserialize(ray.get(call))


def serialize(obj) -> bytes:
    """Serialize a python object which can be deserialized by `PythonGateway`
    """
    return msgpack.packb(obj, use_bin_type=True)


def deserialize(data: bytes):
    """Deserialize the binary data serialized by `PythonGateway`"""
    return msgpack.unpackb(data, raw=False, strict_map_key=False)
