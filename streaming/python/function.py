import importlib
import inspect
import sys
from abc import ABC, abstractmethod

import cloudpickle
import msgpack


class Function(ABC):
    def open(self, conf):
        pass

    def close(self):
        pass


class SourceContext(ABC):
    @abstractmethod
    def collect(self, element):
        pass


class SourceFunction(Function):
    """Interface of Source functions."""

    @abstractmethod
    def init(self, parallel, index):
        pass

    @abstractmethod
    def run(self, ctx: SourceContext):
        pass

    def close(self):
        pass


class MapFunction(Function):
    def map(self, value):
        pass


class FlatMapFunction(Function):
    def flat_map(self, value, collector):
        pass


class FilterFunction(Function):
    def filter(self, value):
        pass


class KeyFunction(Function):
    def key_by(self, value):
        pass


class ReduceFunction(Function):
    def reduce(self, old_value, new_value):
        pass


class SinkFunction(Function):
    def sink(self, value):
        pass


class ListSourceFunction(SourceFunction):
    def __init__(self, values):
        self.values = values

    def init(self, parallel, index):
        pass

    def run(self, ctx: SourceContext):
        for v in self.values:
            ctx.collect(v)


class SimpleMapFunction(MapFunction):
    def __init__(self, func):
        self.func = func

    def map(self, value):
        return self.func(value)


class SimpleFlatMapFunction(FlatMapFunction):
    def __init__(self, func):
        self.func = func

    def flat_map(self, value, collector):
        self.func(value, collector)


class SimpleFilterFunction(FilterFunction):
    def __init__(self, func):
        self.func = func

    def filter(self, value):
        return self.func(value)


class SimpleKeyFunction(KeyFunction):
    def __init__(self, func):
        self.func = func

    def key_by(self, value):
        return self.func(value)


class SimpleReduceFunction(ReduceFunction):
    def __init__(self, func):
        self.func = func

    def reduce(self, old_value, new_value):
        return self.func(old_value, new_value)


class SimpleSinkFunction(SinkFunction):
    def __init__(self, func):
        self.func = func

    def sink(self, value):
        return self.func(value)


def serialize(func):
    return cloudpickle.dumps(func)


def deserialize(func_bytes):
    return cloudpickle.loads(func_bytes)


def load_function(descriptor_func_bytes):
    function_bytes, module_name, class_name, function_name, function_interface\
        = msgpack.loads(descriptor_func_bytes)
    if function_bytes:
        return deserialize(function_bytes)
    else:
        assert module_name
        assert function_interface
        function_interface = getattr(sys.modules[__name__], function_interface)
        mod = importlib.import_module(module_name)
        if class_name:
            assert function_name is None
            cls = getattr(mod, class_name)
            assert issubclass(cls, function_interface)
            return cls()
        else:
            assert function_name
            func = getattr(mod, function_name)
            simple_func_class = get_simple_function_class(function_interface)
            return simple_func_class(func)


def get_simple_function_class(function_interface):
    for name, obj in inspect.getmembers(sys.modules[__name__]):
        if inspect.isclass(obj) and issubclass(obj, function_interface):
            if obj is not function_interface:
                return obj
    raise Exception(
        "SimpleFunction for %s doesn't exist".format(function_interface))
