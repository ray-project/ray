import enum
import importlib
import logging
from abc import ABC, abstractmethod

from ray.streaming import function
from ray.streaming import message
from ray.streaming.collector import Collector
from ray.streaming.collector import CollectionCollector
from ray.streaming.function import SourceFunction
from ray.streaming.runtime import gateway_client

logger = logging.getLogger(__name__)


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

    @abstractmethod
    def save_checkpoint(self):
        pass

    @abstractmethod
    def load_checkpoint(self, checkpoint_obj):
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

    def save_checkpoint(self):
        self.func.save_checkpoint()

    def load_checkpoint(self, checkpoint_obj):
        self.func.load_checkpoint(checkpoint_obj)


class SourceOperator(Operator, ABC):
    @abstractmethod
    def fetch(self):
        pass


class SourceOperatorImpl(SourceOperator, StreamOperator):
    """
    Operator to run a :class:`function.SourceFunction`
    """

    class SourceContextImpl(function.SourceContext):
        def __init__(self, collectors):
            self.collectors = collectors

        def collect(self, value):
            for collector in self.collectors:
                collector.collect(message.Record(value))

    def __init__(self, func: SourceFunction):
        assert isinstance(func, function.SourceFunction)
        super().__init__(func)
        self.source_context = None

    def open(self, collectors, runtime_context):
        super().open(collectors, runtime_context)
        self.source_context = SourceOperatorImpl.SourceContextImpl(collectors)
        self.func.init(runtime_context.get_parallelism(),
                       runtime_context.get_task_index())

    def fetch(self):
        self.func.fetch(self.source_context)

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
        self.collection_collector = CollectionCollector(collectors)

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


class ChainedOperator(StreamOperator, ABC):
    class ForwardCollector(Collector):
        def __init__(self, succeeding_operator):
            self.succeeding_operator = succeeding_operator

        def collect(self, record):
            self.succeeding_operator.process_element(record)

    def __init__(self, operators, configs):
        super().__init__(operators[0].func)
        self.operators = operators
        self.configs = configs

    def open(self, collectors, runtime_context):
        # Dont' call super.open() as we `open` every operator separately.
        num_operators = len(self.operators)
        succeeding_collectors = [
            ChainedOperator.ForwardCollector(operator)
            for operator in self.operators[1:]
        ]
        for i in range(0, num_operators - 1):
            forward_collectors = [succeeding_collectors[i]]
            self.operators[i].open(
                forward_collectors,
                self.__create_runtime_context(runtime_context, i))
        self.operators[-1].open(
            collectors,
            self.__create_runtime_context(runtime_context, num_operators - 1))

    def operator_type(self) -> OperatorType:
        return self.operators[0].operator_type()

    def __create_runtime_context(self, runtime_context, index):
        def get_config():
            return self.configs[index]

        runtime_context.get_config = get_config
        return runtime_context

    @staticmethod
    def new_chained_operator(operators, configs):
        operator_type = operators[0].operator_type()
        logger.info(
            "Building ChainedOperator from operators {} and configs {}."
            .format(operators, configs))
        if operator_type == OperatorType.SOURCE:
            return ChainedSourceOperator(operators, configs)
        elif operator_type == OperatorType.ONE_INPUT:
            return ChainedOneInputOperator(operators, configs)
        elif operator_type == OperatorType.TWO_INPUT:
            return ChainedTwoInputOperator(operators, configs)
        else:
            raise Exception("Current operator type is not supported")


class ChainedSourceOperator(SourceOperator, ChainedOperator):
    def __init__(self, operators, configs):
        super().__init__(operators, configs)

    def fetch(self):
        self.operators[0].fetch()


class ChainedOneInputOperator(ChainedOperator):
    def __init__(self, operators, configs):
        super().__init__(operators, configs)

    def process_element(self, record):
        self.operators[0].process_element(record)


class ChainedTwoInputOperator(ChainedOperator):
    def __init__(self, operators, configs):
        super().__init__(operators, configs)

    def process_element(self, record1, record2):
        self.operators[0].process_element(record1, record2)


def load_chained_operator(chained_operator_bytes: bytes):
    """Load chained operator from serialized operators and configs"""
    serialized_operators, configs = gateway_client.deserialize(
        chained_operator_bytes)
    operators = [
        load_operator(desc_bytes) for desc_bytes in serialized_operators
    ]
    return ChainedOperator.new_chained_operator(operators, configs)


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


_function_to_operator = {
    function.SourceFunction: SourceOperatorImpl,
    function.MapFunction: MapOperator,
    function.FlatMapFunction: FlatMapOperator,
    function.FilterFunction: FilterOperator,
    function.KeyFunction: KeyByOperator,
    function.ReduceFunction: ReduceOperator,
    function.SinkFunction: SinkOperator,
}


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
