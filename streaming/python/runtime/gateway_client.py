# -*- coding: UTF-8 -*-
"""Module to interact between java and python
"""

import ray
from ray.function_manager import FunctionDescriptor
import msgpack


class GatewayClient:
    _PYTHON_GATEWAY_CLASSNAME = \
        b"org.ray.streaming.runtime.python.PythonGateway"

    def __init__(self, python_gateway_actor):
        self._python_gateway_actor = python_gateway_actor

    def create_streaming_context(self):
        function_descriptor = FunctionDescriptor.from_bytes_list([
            GatewayClient._PYTHON_GATEWAY_CLASSNAME, b"createStreamingContext",
            b"()[B"
        ])
        call = self._python_gateway_actor.submit_task(function_descriptor)
        return deserialize(ray.get(call))

    def with_config(self, conf):
        function_descriptor = FunctionDescriptor.from_bytes_list([
            GatewayClient._PYTHON_GATEWAY_CLASSNAME, b"withConfig", b"([B)[B"
        ])
        call = self._python_gateway_actor.submit_task(function_descriptor,
                                                      serialize(conf))
        ray.get(call)

    def execute(self):
        function_descriptor = FunctionDescriptor.from_bytes_list(
            [GatewayClient._PYTHON_GATEWAY_CLASSNAME, b"execute", b"()"])
        call = self._python_gateway_actor.submit_task(function_descriptor)
        ray.get(call)

    def create_py_stream_source(self, serialized_func):
        function_descriptor = FunctionDescriptor.from_bytes_list([
            GatewayClient._PYTHON_GATEWAY_CLASSNAME,
            b"createPythonStreamSource", b"([B)[B"
        ])
        call = self._python_gateway_actor.submit_task(
            function_descriptor, serialize(serialized_func))
        return deserialize(ray.get(call))

    def create_py_func(self, serialized_func):
        function_descriptor = FunctionDescriptor.from_bytes_list([
            GatewayClient._PYTHON_GATEWAY_CLASSNAME, b"createPyFunc", b"([B)[B"
        ])
        call = self._python_gateway_actor.submit_task(
            function_descriptor, serialize(serialized_func))
        return deserialize(ray.get(call))

    def create_py_partition(self, serialized_partition):
        function_descriptor = FunctionDescriptor.from_bytes_list([
            GatewayClient._PYTHON_GATEWAY_CLASSNAME, b"createPyPartition",
            b"([B)[B"
        ])
        call = self._python_gateway_actor.submit_task(
            function_descriptor, serialize(serialized_partition))
        return deserialize(ray.get(call))

    def call_function(self, java_class, java_function, *args):
        function_descriptor = FunctionDescriptor.from_bytes_list([
            GatewayClient._PYTHON_GATEWAY_CLASSNAME, b"callFunction", b"([B)[B"
        ])
        java_params = serialize([java_class, java_function] + list(args))
        call = self._python_gateway_actor.submit_task(function_descriptor,
                                                      java_params)
        return deserialize(ray.get(call))

    def call_method(self, java_object, java_method, *args):
        function_descriptor = FunctionDescriptor.from_bytes_list([
            GatewayClient._PYTHON_GATEWAY_CLASSNAME, b"callMethod", b"([B)[B"
        ])
        java_params = serialize([java_object, java_method] + list(args))
        call = self._python_gateway_actor.submit_task(function_descriptor,
                                                      java_params)
        return deserialize(ray.get(call))


def serialize(obj):
    return msgpack.packb(obj)


def deserialize(data):
    return msgpack.unpackb(data)
