from abc import ABC
import ray.streaming.function as function
import ray.streaming.partition as partition


class Stream(ABC):
    """Abstract base class of all stream types."""

    def __init__(self, input_stream, j_stream, streaming_context=None):
        self.input_stream = input_stream
        self._j_stream = j_stream
        if streaming_context is None:
            assert input_stream is not None
            self.streaming_context = input_stream.streaming_context
        else:
            self.streaming_context = streaming_context
        self.parallelism = 1

    def get_streaming_context(self):
        return self.streaming_context

    def get_parallelism(self):
        return self.parallelism

    def set_parallelism(self, parallelism: int):
        self.parallelism = parallelism
        self._gateway_client(). \
            call_method(self._j_stream, "setParallelism", parallelism)

    def get_input_stream(self):
        return self.input_stream

    def get_id(self):
        return self._gateway_client(). \
            call_method(self._j_stream, "getId")

    def _gateway_client(self):
        return self.get_streaming_context()._gateway_client


class DataStream(Stream):
    """Represents a stream of data.
     Wrapper of java org.ray.streaming.python.stream.PythonDataStream
     """

    def __init__(self, input_stream, j_stream, streaming_context=None):
        super().__init__(
            input_stream, j_stream, streaming_context=streaming_context)

    def map(self, func):
        if not isinstance(func, function.MapFunction):
            func = function.SimpleMapFunction(func)
        j_func = self._gateway_client().create_py_func(
            function.serialize(func))
        j_stream = self._gateway_client(). \
            call_method(self._j_stream, "map", j_func)
        return DataStream(self, j_stream)

    def flat_map(self, func):
        if not isinstance(func, function.FlatMapFunction):
            func = function.SimpleFlatMapFunction(func)
        j_func = self._gateway_client().create_py_func(
            function.serialize(func))
        j_stream = self._gateway_client(). \
            call_method(self._j_stream, "flatMap", j_func)
        return DataStream(self, j_stream)

    def filter(self, func):
        if not isinstance(func, function.FilterFunction):
            func = function.SimpleFilterFunction(func)
        j_func = self._gateway_client().create_py_func(
            function.serialize(func))
        j_stream = self._gateway_client(). \
            call_method(self._j_stream, "filter", j_func)
        return DataStream(self, j_stream)

    def key_by(self, func):
        if not isinstance(func, function.KeyFunction):
            func = function.SimpleKeyFunction(func)
        j_func = self._gateway_client().create_py_func(
            function.serialize(func))
        j_stream = self._gateway_client(). \
            call_method(self._j_stream, "keyBy", j_func)
        return KeyDataStream(self, j_stream)

    def broadcast(self):
        self._gateway_client().call_method(self._j_stream, "broadcast")
        return self

    def partition_by(self, partition_func):
        if not isinstance(partition_func, partition.SimplePartition):
            partition_func = partition.SimplePartition(partition_func)
        j_partition = self._gateway_client().create_py_func(
            partition.serialize(partition_func))
        self._gateway_client(). \
            call_method(self._j_stream, "partitionBy", j_partition)
        return self

    def sink(self, func):
        if not isinstance(func, function.SinkFunction):
            func = function.SimpleSinkFunction(func)
        j_func = self._gateway_client().create_py_func(
            function.serialize(func))
        j_stream = self._gateway_client(). \
            call_method(self._j_stream, "sink", j_func)
        return StreamSink(self, j_stream, func)


class KeyDataStream(Stream):
    """Represents a DataStream returned by a key-by operation.
     Wrapper of java org.ray.streaming.python.stream.PythonKeyDataStream
    """

    def __init__(self, input_stream, j_stream):
        super().__init__(input_stream, j_stream)

    def reduce(self, func):
        if not isinstance(func, function.ReduceFunction):
            func = function.SimpleReduceFunction(func)
        j_func = self._gateway_client().create_py_func(
            function.serialize(func))
        j_stream = self._gateway_client(). \
            call_method(self._j_stream, "reduce", j_func)
        return DataStream(self, j_stream)


class StreamSource(DataStream):
    """Represents a source of the DataStream.
     Wrapper of java org.ray.streaming.python.stream.PythonStreamSource
    """

    def __init__(self, j_stream, streaming_context, source_func):
        super().__init__(None, j_stream, streaming_context=streaming_context)
        self.source_func = source_func

    @staticmethod
    def build_source(streaming_context, func):
        """Build a StreamSource source from a collection.
        Args:
            streaming_context: Stream context
            func: A instance of `SourceFunction`
        Returns:
            A StreamSource
        """
        j_stream = streaming_context._gateway_client. \
            create_py_stream_source(function.serialize(func))
        return StreamSource(j_stream, streaming_context, func)


class StreamSink(Stream):
    """Represents a sink of the DataStream.
     Wrapper of java org.ray.streaming.python.stream.PythonStreamSink
    """

    def __init__(self, input_stream, j_stream, func):
        super().__init__(input_stream, j_stream)
