from abc import ABC, abstractmethod
import enum
import ray.streaming as streaming
import ray.streaming.function as function
import ray.streaming.message as message


class OperatorType(enum.Enum):
    SOURCE = 0
    ONE_INPUT = 1
    TWO_INPUT = 2


class Operator(ABC):
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
    def operator_type(self):
        pass


class OneInputOperator(Operator, ABC):
    @abstractmethod
    def process_element(self, record):
        pass

    def operator_type(self):
        return OperatorType.ONE_INPUT


class TwoInputOperator(Operator, ABC):
    @abstractmethod
    def process_element(self, record1, record2):
        pass

    def operator_type(self):
        return OperatorType.TWO_INPUT


class StreamOperator(Operator, ABC):
    def __init__(self, func):
        self.func = func
        self.collectors = None
        self.runtime_context = None

    def open(self, collectors, runtime_context):
        self.collectors = collectors
        self.runtime_context = runtime_context

    def finish(self):
        pass

    def close(self):
        pass

    def collect(self, record):
        for collector in self.collectors:
            collector.collect(record)


class SourceOperator(StreamOperator):
    class SourceContextImpl(function.SourceContext):
        def __init__(self, collectors):
            self.collectors = collectors

        def collect(self, value):
            for collector in self.collectors:
                collector.collect(message.Record(value))

    def __init__(self, func):
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
    def __init__(self, map_func):
        super().__init__(map_func)

    def process_element(self, record):
        self.collect(message.Record(self.func.map(record.value)))


class FlatMapOperator(StreamOperator, OneInputOperator):
    def __init__(self, flat_map_func):
        super().__init__(flat_map_func)
        self.collection_collector = None

    def open(self, collectors, runtime_context):
        super().open(collectors, runtime_context)
        self.collection_collector = streaming.collector.CollectionCollector(
            collectors)

    def process_element(self, record):
        self.collect(message.Record(self.func.map(record.value)))
        self.func.flat_map(record.value, self.collection_collector)


class FilterOperator(StreamOperator, OneInputOperator):
    pass


class KeyByOperator(StreamOperator, OneInputOperator):
    def __init__(self, key_func):
        super().__init__(key_func)

    def process_element(self, record):
        key = self.func.key_by(record.value)
        self.collect(message.KeyRecord(key, record.value))


class ReduceOperator(StreamOperator, OneInputOperator):
    def __init__(self, reduce_func):
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
    def __init__(self, sink_func):
        super().__init__(sink_func)

    def process_element(self, record):
        self.func.sink(record.value)


_function_to_operator = {
    function.SourceFunction: SourceOperator,
    function.MapFunction: MapOperator,
    function.FlatMapFunction: FlatMapOperator,
    function.FilterFunction: FilterOperator,
    function.KeyFunction: KeyByOperator,
    function.ReduceFunction: ReduceOperator,
    function.SinkFunction: SinkOperator,
}


def create_operator(func):
    func_interface = None
    super_classes = func.__class__.mro()
    for i in range(len(super_classes)):
        if super_classes[i] == function.Function:
            func_interface = super_classes[i - 1]
    assert func_interface is not None
    operator_class = _function_to_operator[func_interface]
    return operator_class(func)
