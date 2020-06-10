import enum
import importlib
from abc import ABC, abstractmethod

from ray import streaming
from ray.streaming import function
from ray.streaming import message
from ray.streaming.runtime import gateway_client


class OperatorType(enum.Enum):
    SOURCE = 0  # Sources are where your program reads its input from
    ONE_INPUT = 1  # This operator has one data stream as it's input stream.
    TWO_INPUT = 2  # This operator has two data stream as it's input stream.


class Operator(ABC):
    """
    Abstract base class for all operators.
    An operator is used to run a :class:`function.Function`.
    """

    @abstractmethod
    def open(self, collectors, runtime_context):
        pass

    @abstractmethod
    def finish(self):
        pass

    @abstractmethod
    def close(self):
        pass

    @abstractmethod
    def operator_type(self) -> OperatorType:
        pass


class OneInputOperator(Operator, ABC):
    """Interface for stream operators with one input."""

    @abstractmethod
    def process_element(self, record):
        pass

    def operator_type(self):
        return OperatorType.ONE_INPUT


class TwoInputOperator(Operator, ABC):
    """Interface for stream operators with two input"""

    @abstractmethod
    def process_element(self, record1, record2):
        pass

    def operator_type(self):
        return OperatorType.TWO_INPUT


class StreamOperator(Operator, ABC):
    """
    Basic interface for stream operators. Implementers would implement one of
    :class:`OneInputOperator` or :class:`TwoInputOperator` to to create
    operators that process elements.
    """

    def __init__(self, func):
        self.func = func
        self.collectors = None
        self.runtime_context = None

    def open(self, collectors, runtime_context):
        self.collectors = collectors
        self.runtime_context = runtime_context
        self.func.open(runtime_context)

    def finish(self):
        pass

    def close(self):
        self.func.close()

    def collect(self, record):
        for collector in self.collectors:
            collector.collect(record)


class SourceOperator(StreamOperator):
    """
    Operator to run a :class:`function.SourceFunction`
    """

    class SourceContextImpl(function.SourceContext):
        def __init__(self, collectors):
            self.collectors = collectors

        def collect(self, value):
            for collector in self.collectors:
                collector.collect(message.Record(value))

    def __init__(self, func):
        assert isinstance(func, function.SourceFunction)
        super().__init__(func)
        self.source_context = None

    def open(self, collectors, runtime_context):
        super().open(collectors, runtime_context)
        self.source_context = SourceOperator.SourceContextImpl(collectors)
        self.func.init(runtime_context.get_parallelism(),
                       runtime_context.get_task_index())

    def run(self):
        self.func.run(self.source_context)

    def operator_type(self):
        return OperatorType.SOURCE


class MapOperator(StreamOperator, OneInputOperator):
    """
    Operator to run a :class:`function.MapFunction`
    """

    def __init__(self, map_func: function.MapFunction):
        assert isinstance(map_func, function.MapFunction)
        super().__init__(map_func)

    def process_element(self, record):
        self.collect(message.Record(self.func.map(record.value)))


class FlatMapOperator(StreamOperator, OneInputOperator):
    """
    Operator to run a :class:`function.FlatMapFunction`
    """

    def __init__(self, flat_map_func: function.FlatMapFunction):
        assert isinstance(flat_map_func, function.FlatMapFunction)
        super().__init__(flat_map_func)
        self.collection_collector = None

    def open(self, collectors, runtime_context):
        super().open(collectors, runtime_context)
        self.collection_collector = streaming.collector.CollectionCollector(
            collectors)

    def process_element(self, record):
        self.func.flat_map(record.value, self.collection_collector)


class FilterOperator(StreamOperator, OneInputOperator):
    """
    Operator to run a :class:`function.FilterFunction`
    """

    def __init__(self, filter_func: function.FilterFunction):
        assert isinstance(filter_func, function.FilterFunction)
        super().__init__(filter_func)

    def process_element(self, record):
        if self.func.filter(record.value):
            self.collect(record)


class KeyByOperator(StreamOperator, OneInputOperator):
    """
    Operator to run a :class:`function.KeyFunction`
    """

    def __init__(self, key_func: function.KeyFunction):
        assert isinstance(key_func, function.KeyFunction)
        super().__init__(key_func)

    def process_element(self, record):
        key = self.func.key_by(record.value)
        self.collect(message.KeyRecord(key, record.value))


class ReduceOperator(StreamOperator, OneInputOperator):
    """
    Operator to run a :class:`function.ReduceFunction`
    """

    def __init__(self, reduce_func: function.ReduceFunction):
        assert isinstance(reduce_func, function.ReduceFunction)
        super().__init__(reduce_func)
        self.reduce_state = {}

    def open(self, collectors, runtime_context):
        super().open(collectors, runtime_context)

    def process_element(self, record: message.KeyRecord):
        key = record.key
        value = record.value
        if key in self.reduce_state:
            old_value = self.reduce_state[key]
            new_value = self.func.reduce(old_value, value)
            self.reduce_state[key] = new_value
            self.collect(message.Record(new_value))
        else:
            self.reduce_state[key] = value
            self.collect(record)


class SinkOperator(StreamOperator, OneInputOperator):
    """
    Operator to run a :class:`function.SinkFunction`
    """

    def __init__(self, sink_func: function.SinkFunction):
        assert isinstance(sink_func, function.SinkFunction)
        super().__init__(sink_func)

    def process_element(self, record):
        self.func.sink(record.value)


class UnionOperator(StreamOperator, OneInputOperator):
    """Operator for union operation"""

    def __init__(self):
        super().__init__(function.EmptyFunction())

    def process_element(self, record):
        self.collect(record)


_function_to_operator = {
    function.SourceFunction: SourceOperator,
    function.MapFunction: MapOperator,
    function.FlatMapFunction: FlatMapOperator,
    function.FilterFunction: FilterOperator,
    function.KeyFunction: KeyByOperator,
    function.ReduceFunction: ReduceOperator,
    function.SinkFunction: SinkOperator,
}


def load_operator(descriptor_operator_bytes: bytes):
    """
    Deserialize `descriptor_operator_bytes` to get operator info, then
    create streaming operator.
    Note that this function must be kept in sync with
     `io.ray.streaming.runtime.python.GraphPbBuilder.serializeOperator`

    Args:
        descriptor_operator_bytes: serialized operator info

    Returns:
        a streaming operator
    """
    assert len(descriptor_operator_bytes) > 0
    function_desc_bytes, module_name, class_name \
        = gateway_client.deserialize(descriptor_operator_bytes)
    if function_desc_bytes:
        return create_operator_with_func(
            function.load_function(function_desc_bytes))
    else:
        assert module_name
        assert class_name
        mod = importlib.import_module(module_name)
        cls = getattr(mod, class_name)
        assert issubclass(cls, Operator)
        print("cls", cls)
        return cls()


def create_operator_with_func(func: function.Function):
    """Create an operator according to a :class:`function.Function`

    Args:
        func: a subclass of function.Function

    Returns:
        an operator
    """
    operator_class = None
    super_classes = func.__class__.mro()
    for super_class in super_classes:
        operator_class = _function_to_operator.get(super_class, None)
        if operator_class is not None:
            break
    assert operator_class is not None
    return operator_class(func)
