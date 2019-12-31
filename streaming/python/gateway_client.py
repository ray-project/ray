# -*- coding: UTF-8 -*-
"""Module to interact between java and python
"""

import ray
from ray.function_manager import FunctionDescriptor
import msgpack


class GatewayClient:
    _PYTHON_GATEWAY_CLASSNAME = b"org.ray.streaming.python.PythonGateway"

    def __init__(self, python_gateway_actor):
        self._python_gateway_actor = python_gateway_actor

    def create_streaming_context(self):
        function_descriptor = FunctionDescriptor.from_bytes_list(
            [GatewayClient._PYTHON_GATEWAY_CLASSNAME,
             b'createStreamingContext',
             b'()[B'])
        call = self._python_gateway_actor.submit_task(function_descriptor)
        return ray.get(call)

    def with_config(self, conf):
        function_descriptor = FunctionDescriptor.from_bytes_list(
            [GatewayClient._PYTHON_GATEWAY_CLASSNAME,
             b'withConfig',
             b'([B)[B'])
        call = self._python_gateway_actor.submit_task(function_descriptor,
                                                      msgpack.packb(conf))
        ray.get(call)

    def execute(self,):
        function_descriptor = FunctionDescriptor.from_bytes_list(
            [GatewayClient._PYTHON_GATEWAY_CLASSNAME,
             b'execute',
             b'()'])
        call = self._python_gateway_actor.submit_task(function_descriptor)
        ray.get(call)

    def create_py_func(self, serialized_func):
        function_descriptor = FunctionDescriptor.from_bytes_list(
            [GatewayClient._PYTHON_GATEWAY_CLASSNAME,
             b'createPyFunc',
             b'([B)[B'])
        call = self._python_gateway_actor.submit_task(function_descriptor,
                                                      msgpack.packb(serialized_func))
        ray.get(call)

    def create_py_partition(self, serialized_partition):
        function_descriptor = FunctionDescriptor.from_bytes_list(
            [GatewayClient._PYTHON_GATEWAY_CLASSNAME,
             b'createPyPartition',
             b'([B)[B'])
        call = self._python_gateway_actor.submit_task(function_descriptor,
                                                      msgpack.packb(serialized_partition))
        ray.get(call)

    def func_call(self, java_class, java_function, *args):
        function_descriptor = FunctionDescriptor.from_bytes_list(
            [GatewayClient._PYTHON_GATEWAY_CLASSNAME,
             b'funcCall',
             b'([B)[B'])
        java_params = msgpack.packb([java_class, java_function] + list(args))
        call = self._python_gateway_actor.submit_task(function_descriptor, java_params)
        ray.get(call)

    def method_call(self, java_object, java_method, *args):
        function_descriptor = FunctionDescriptor.from_bytes_list(
            [GatewayClient._PYTHON_GATEWAY_CLASSNAME,
             b'methodCall',
             b'([B)[B'])
        java_params = msgpack.packb([java_object, java_method] + list(args))
        call = self._python_gateway_actor.submit_task(function_descriptor, java_params)
        ray.get(call)