from abc import ABC
import ray.streaming.function as function


class Stream(ABC):
    """Abstract base class of all stream types."""

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


class DataStream(Stream):
    """Represents a stream of data.
     Wrapper of java org.ray.streaming.python.stream.PythonDataStream
     """

    def map(self, func):
        if not isinstance(func, function.MapFunction):
            func = function.SimpleMapFunction(func)
        pass

    def flat_map(self, func):
        if not isinstance(func, function.FlatMapFunction):
            func = function.SimpleFlatMapFunction(func)
        pass

    def filter(self, func):
        if not isinstance(func, function.FilterFunction):
            func = function.SimpleFilterFunction(func)
        pass

    def key_by(self, func):
        if not isinstance(func, function.KeyFunction):
            func = function.SimpleKeyFunction(func)
        pass

    def broadcast(self):
        pass

    def partition_by(self, func):
        pass

    def sink(self, func):
        if not isinstance(func, function.SinkFunction):
            func = function.SimpleSinkFunction(func)
        pass

    def set_parallelism(self, parallelism: int):
        pass


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

    def __init__(self, streaming_context, source_func):
        pass

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

    def set_parallelism(self, parallelism: int):
        pass


class StreamSink(Stream):
    """Represents a sink of the DataStream.
     Wrapper of java org.ray.streaming.python.stream.PythonStreamSink
    """

    def set_parallelism(self, parallelism: int):
        pass
