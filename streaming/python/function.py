import enum
import importlib
import inspect
import sys
from abc import ABC, abstractmethod

from ray import cloudpickle
from ray.streaming.runtime import gateway_client


class Language(enum.Enum):
    JAVA = 0
    PYTHON = 1


class Function(ABC):
    """The base interface for all user-defined functions."""

    def open(self, runtime_context):
        pass

    def close(self):
        pass


class EmptyFunction(Function):
    """Default function which does nothing"""

    def open(self, runtime_context):
        pass

    def close(self):
        pass


class SourceContext(ABC):
    """
    Interface that source functions use to emit elements, and possibly
     watermarks."""

    @abstractmethod
    def collect(self, element):
        """Emits one element from the source, without attaching a timestamp."""
        pass


class SourceFunction(Function):
    """Interface of Source functions."""

    @abstractmethod
    def init(self, parallel, index):
        """
        Args:
            parallel: parallelism of source function
            index: task index of this function and goes up from 0 to
             parallel-1.
        """
        pass

    @abstractmethod
    def run(self, ctx: SourceContext):
        """Starts the source. Implementations can use the
         :class:`SourceContext` to emit elements.
        """
        pass


class MapFunction(Function):
    """
    Base interface for Map functions. Map functions take elements and transform
    them element wise. A Map function always produces a single result element
    for each input element.
    """

    @abstractmethod
    def map(self, value):
        pass


class FlatMapFunction(Function):
    """
    Base interface for flatMap functions. FlatMap functions take elements and
    transform them into zero, one, or more elements.
    """

    @abstractmethod
    def flat_map(self, value, collector):
        """Takes an element from the input data set and transforms it into zero,
        one, or more elements.

        Args:
            value: The input value.
            collector: The collector for returning result values.
        """
        pass


class FilterFunction(Function):
    """
    A filter function is a predicate applied individually to each record.
    The predicate decides whether to keep the element, or to discard it.
    """

    @abstractmethod
    def filter(self, value):
        """The filter function that evaluates the predicate.

        Args:
            value: The value to be filtered.

        Returns:
            True for values that should be retained, false for values to be
            filtered out.
        """
        pass


class KeyFunction(Function):
    """
    A key function is extractor which takes an object and returns the
    deterministic key for that object.
    """

    @abstractmethod
    def key_by(self, value):
        """User-defined function that deterministically extracts the key from
         an object.

        Args:
            value: The object to get the key from.

        Returns:
            The extracted key.
        """
        pass


class ReduceFunction(Function):
    """
    Base interface for Reduce functions. Reduce functions combine groups of
    elements to a single value, by taking always two elements and combining
    them into one.
    """

    @abstractmethod
    def reduce(self, old_value, new_value):
        """
        The core method of ReduceFunction, combining two values into one value
        of the same type. The reduce function is consecutively applied to all
        values of a group until only a single value remains.

        Args:
            old_value: The old value to combine.
            new_value: The new input value to combine.

        Returns:
            The combined value of both values.
        """
        pass


class SinkFunction(Function):
    """Interface for implementing user defined sink functionality."""

    @abstractmethod
    def sink(self, value):
        """Writes the given value to the sink. This function is called for
         every record."""
        pass


class CollectionSourceFunction(SourceFunction):
    def __init__(self, values):
        self.values = values

    def init(self, parallel, index):
        pass

    def run(self, ctx: SourceContext):
        for v in self.values:
            ctx.collect(v)


class LocalFileSourceFunction(SourceFunction):
    def __init__(self, filename):
        self.filename = filename

    def init(self, parallel, index):
        pass

    def run(self, ctx: SourceContext):
        with open(self.filename, "r") as f:
            line = f.readline()
            while line != "":
                ctx.collect(line[:-1])
                line = f.readline()


class SimpleMapFunction(MapFunction):
    def __init__(self, func):
        self.func = func

    def map(self, value):
        return self.func(value)


class SimpleFlatMapFunction(FlatMapFunction):
    """
    Wrap a python function as :class:`FlatMapFunction`

    >>> assert SimpleFlatMapFunction(lambda x: x.split())
    >>> def flat_func(x, collector):
    ...     for item in x.split():
    ...         collector.collect(item)
    >>> assert SimpleFlatMapFunction(flat_func)
    """

    def __init__(self, func):
        """
        Args:
            func: a python function which takes an element from input augment
            and transforms it into zero, one, or more elements.
            Or takes an element from input augment, and used provided collector
            to collect zero, one, or more elements.
        """
        self.func = func
        self.process_func = None
        sig = inspect.signature(func)
        assert len(sig.parameters) <= 2, \
            "func should receive value [, collector] as arguments"
        if len(sig.parameters) == 2:

            def process(value, collector):
                func(value, collector)

            self.process_func = process
        else:

            def process(value, collector):
                for elem in func(value):
                    collector.collect(elem)

            self.process_func = process

    def flat_map(self, value, collector):
        self.process_func(value, collector)


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


def serialize(func: Function):
    """Serialize a streaming :class:`Function`"""
    return cloudpickle.dumps(func)


def deserialize(func_bytes):
    """Deserialize a binary function serialized by `serialize` method."""
    return cloudpickle.loads(func_bytes)


def load_function(descriptor_func_bytes: bytes):
    """
    Deserialize `descriptor_func_bytes` to get function info, then
    get or load streaming function.
    Note that this function must be kept in sync with
     `io.ray.streaming.runtime.python.GraphPbBuilder.serializeFunction`

    Args:
        descriptor_func_bytes: serialized function info

    Returns:
        a streaming function
    """
    assert len(descriptor_func_bytes) > 0
    function_bytes, module_name, function_name, function_interface \
        = gateway_client.deserialize(descriptor_func_bytes)
    if function_bytes:
        return deserialize(function_bytes)
    else:
        assert module_name
        assert function_interface
        function_interface = getattr(sys.modules[__name__], function_interface)
        mod = importlib.import_module(module_name)
        assert function_name
        func = getattr(mod, function_name)
        # If func is a python function, user function is a simple python
        # function, which will be wrapped as a SimpleXXXFunction.
        # If func is a python class, user function is a sub class
        # of XXXFunction.
        if inspect.isfunction(func):
            simple_func_class = _get_simple_function_class(function_interface)
            return simple_func_class(func)
        else:
            assert issubclass(func, function_interface)
            return func()


def _get_simple_function_class(function_interface):
    """Get the wrapper function for the given `function_interface`."""
    for name, obj in inspect.getmembers(sys.modules[__name__]):
        if inspect.isclass(obj) and issubclass(obj, function_interface):
            if obj is not function_interface and obj.__name__.startswith(
                    "Simple"):
                return obj
    raise Exception(
        "SimpleFunction for %s doesn't exist".format(function_interface))
