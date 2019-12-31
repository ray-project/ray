from abc import ABC
import ray.streaming.function as function
import ray.streaming.partition as partition
import cloudpickle


class Stream(ABC):
    """Abstract base class of all stream types."""

    def __init__(self, input_stream):
        self.input_stream = input_stream

    def get_streaming_context(self):
        pass

    def get_parallelism(self):
        pass

    def set_parallelism(self, parallelism: int):
        pass

    def get_input_stream(self):
        pass

    def get_id(self):
        pass

    def _gateway_client(self):
        return self.get_streaming_context()._gateway_client


class DataStream(Stream):
    """Represents a stream of data.
     Wrapper of java org.ray.streaming.python.stream.PythonDataStream
     """

    def __init__(self, input_stream, j_stream):
        super().__init__(input_stream, j_stream)

    def map(self, func):
        if not isinstance(func, function.MapFunction):
            func = function.SimpleMapFunction(func)
        j_func = self._gateway_client().create_py_func(cloudpickle.dumps(func))
        j_stream = self._gateway_client(). \
            method_call(self.input_stream._j_stream, "map",
                        j_func)

    def flat_map(self, func):
        if not isinstance(func, function.FlatMapFunction):
            func = function.SimpleFlatMapFunction(func)
        j_func = self._gateway_client().create_py_func(cloudpickle.dumps(func))
        j_stream = self._gateway_client(). \
            method_call(self.input_stream._j_stream, "flatMap",
                        j_func)

    def filter(self, func):
        if not isinstance(func, function.FilterFunction):
            func = function.SimpleFilterFunction(func)
        j_func = self._gateway_client().create_py_func(cloudpickle.dumps(func))
        j_stream = self._gateway_client(). \
            method_call(self.input_stream._j_stream, "filter",
                        j_func)

    def key_by(self, func):
        if not isinstance(func, function.KeyFunction):
            func = function.SimpleKeyFunction(func)
        j_func = self._gateway_client().create_py_func(cloudpickle.dumps(func))
        j_stream = self._gateway_client(). \
            method_call(self.input_stream._j_stream, "keyBy",
                        j_func)

    def broadcast(self):
        self._gateway_client().method_call(self.input_stream._j_stream, "broadcast")

    def partition_by(self, partition_func):
        if not isinstance(partition_func, partition.SimplePartition):
            partition_func = partition.SimplePartition(partition_func)
        j_partition = self._gateway_client().create_py_func(cloudpickle.dumps(partition_func))
        self._gateway_client().method_call(self.input_stream._j_stream, "partitionBy",
                                           j_partition)

    def sink(self, func):
        if not isinstance(func, function.SinkFunction):
            func = function.SimpleSinkFunction(func)
        j_func = self._gateway_client().create_py_func(cloudpickle.dumps(func))
        j_stream = self._gateway_client(). \
            method_call(self.input_stream._j_stream, "sink",
                        j_func)


class KeyDataStream(Stream):
    """Represents a DataStream returned by a key-by operation.
     Wrapper of java org.ray.streaming.python.stream.PythonKeyDataStream
    """

    def reduce(self, func):
        pass


class StreamSource(DataStream):
    """Represents a source of the DataStream.
     Wrapper of java org.ray.streaming.python.stream.PythonStreamSource
    """

    def __init__(self, streaming_context, source_func, input_stream):
        super().__init__(input_stream)

    @staticmethod
    def build_source(streaming_context, values: list):
        """Build a StreamSource source from a collection.
        Args:
            streaming_context: Stream context
            values: A collection of values
        Returns:
            A StreamSource
        """
        pass


class StreamSink(Stream):
    """Represents a sink of the DataStream.
     Wrapper of java org.ray.streaming.python.stream.PythonStreamSink
    """
