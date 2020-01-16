# -*- coding: UTF-8 -*-
"""Module to interact between java and python
"""

import msgpack
import ray


class GatewayClient:
    _PYTHON_GATEWAY_CLASSNAME = \
        b"org.ray.streaming.runtime.python.PythonGateway"

    def __init__(self):
        self._python_gateway_actor = ray.java_actor_class(
            GatewayClient._PYTHON_GATEWAY_CLASSNAME).remote()

    def create_streaming_context(self):
        call = self._python_gateway_actor.createStreamingContext.remote()
        return deserialize(ray.get(call))

    def with_config(self, conf):
        call = self._python_gateway_actor.withConfig.remote(serialize(conf))
        ray.get(call)

    def execute(self):
        call = self._python_gateway_actor.execute.remote()
        ray.get(call)

    def create_py_stream_source(self, serialized_func):
        call = self._python_gateway_actor.createPythonStreamSource\
            .remote(serialize(serialized_func))
        return deserialize(ray.get(call))

    def create_py_func(self, serialized_func):
        call = self._python_gateway_actor.createPyFunc.remote(serialize(serialized_func))
        return deserialize(ray.get(call))

    def create_py_partition(self, serialized_partition):
        call = self._python_gateway_actor.createPyPartition\
            .remote(serialize(serialized_partition))
        return deserialize(ray.get(call))

    def call_function(self, java_class, java_function, *args):
        java_params = serialize([java_class, java_function] + list(args))
        call = self._python_gateway_actor.callFunction.remote(java_params)
        return deserialize(ray.get(call))

    def call_method(self, java_object, java_method, *args):
        java_params = serialize([java_object, java_method] + list(args))
        call = self._python_gateway_actor.callMethod.remote(java_params)
        return deserialize(ray.get(call))


def serialize(obj):
    return msgpack.packb(obj)


def deserialize(data):
    return msgpack.unpackb(data)
