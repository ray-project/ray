from abc import ABC

from ray.streaming import function
from ray.streaming import partition


class Stream(ABC):
    """
    Abstract base class of all stream types. A Stream represents a stream of
     elements of the same type. A Stream can be transformed into another Stream
     by applying a transformation.
    """

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
        """
        Returns:
            the parallelism of this transformation
        """
        return self.parallelism

    def set_parallelism(self, parallelism: int):
        """Sets the parallelism of this transformation

        Args:
            parallelism: The new parallelism to set on this transformation

        Returns:
            self
        """
        self.parallelism = parallelism
        self._gateway_client(). \
            call_method(self._j_stream, "setParallelism", parallelism)
        return self

    def get_input_stream(self):
        """
        Returns:
            input stream of this stream
        """
        return self.input_stream

    def get_id(self):
        """
        Returns:
            An unique id identifies this stream.
        """
        return self._gateway_client(). \
            call_method(self._j_stream, "getId")

    def _gateway_client(self):
        return self.get_streaming_context()._gateway_client


class DataStream(Stream):
    """
    Represents a stream of data which applies a transformation executed by
    python. It's also a wrapper of java
    `io.ray.streaming.python.stream.PythonDataStream`
    """

    def __init__(self, input_stream, j_stream, streaming_context=None):
        super().__init__(
            input_stream, j_stream, streaming_context=streaming_context)

    def map(self, func):
        """
        Applies a Map transformation on a :class:`DataStream`.
        The transformation calls a :class:`ray.streaming.function.MapFunction`
        for each element of the DataStream.

        Args:
            func: The MapFunction that is called for each element of the
            DataStream. If `func` is a python function instead of a subclass
            of MapFunction, it will be wrapped as SimpleMapFunction.

        Returns:
            A new data stream transformed by the MapFunction.
        """
        if not isinstance(func, function.MapFunction):
            func = function.SimpleMapFunction(func)
        j_func = self._gateway_client().create_py_func(
            function.serialize(func))
        j_stream = self._gateway_client(). \
            call_method(self._j_stream, "map", j_func)
        return DataStream(self, j_stream)

    def flat_map(self, func):
        """
        Applies a FlatMap transformation on a :class:`DataStream`. The
        transformation calls a :class:`ray.streaming.function.FlatMapFunction`
        for each element of the DataStream.
        Each FlatMapFunction call can return any number of elements including
        none.

        Args:
            func: The FlatMapFunction that is called for each element of the
            DataStream. If `func` is a python function instead of a subclass
            of FlatMapFunction, it will be wrapped as SimpleFlatMapFunction.

        Returns:
            The transformed DataStream
        """
        if not isinstance(func, function.FlatMapFunction):
            func = function.SimpleFlatMapFunction(func)
        j_func = self._gateway_client().create_py_func(
            function.serialize(func))
        j_stream = self._gateway_client(). \
            call_method(self._j_stream, "flatMap", j_func)
        return DataStream(self, j_stream)

    def filter(self, func):
        """
        Applies a Filter transformation on a :class:`DataStream`. The
        transformation calls a :class:`ray.streaming.function.FilterFunction`
        for each element of the DataStream.
        DataStream and retains only those element for which the function
        returns True.

        Args:
            func: The FilterFunction that is called for each element of the
            DataStream. If `func` is a python function instead of a subclass of
            FilterFunction, it will be wrapped as SimpleFilterFunction.

        Returns:
            The filtered DataStream
        """
        if not isinstance(func, function.FilterFunction):
            func = function.SimpleFilterFunction(func)
        j_func = self._gateway_client().create_py_func(
            function.serialize(func))
        j_stream = self._gateway_client(). \
            call_method(self._j_stream, "filter", j_func)
        return DataStream(self, j_stream)

    def key_by(self, func):
        """
        Creates a new :class:`KeyDataStream` that uses the provided key to
        partition data stream by key.

        Args:
            func: The KeyFunction that is used for extracting the key for
            partitioning. If `func` is a python function instead of a subclass
            of KeyFunction, it will be wrapped as SimpleKeyFunction.

        Returns:
             A KeyDataStream
        """
        if not isinstance(func, function.KeyFunction):
            func = function.SimpleKeyFunction(func)
        j_func = self._gateway_client().create_py_func(
            function.serialize(func))
        j_stream = self._gateway_client(). \
            call_method(self._j_stream, "keyBy", j_func)
        return KeyDataStream(self, j_stream)

    def broadcast(self):
        """
        Sets the partitioning of the :class:`DataStream` so that the output
        elements are broadcast to every parallel instance of the next
        operation.

        Returns:
            The DataStream with broadcast partitioning set.
        """
        self._gateway_client().call_method(self._j_stream, "broadcast")
        return self

    def partition_by(self, partition_func):
        """
        Sets the partitioning of the :class:`DataStream` so that the elements
        of stream are partitioned by specified partition function.

        Args:
            partition_func: partition function.
            If `func` is a python function instead of a subclass of Partition,
            it will be wrapped as SimplePartition.

        Returns:
            The DataStream with specified partitioning set.
        """
        if not isinstance(partition_func, partition.Partition):
            partition_func = partition.SimplePartition(partition_func)
        j_partition = self._gateway_client().create_py_func(
            partition.serialize(partition_func))
        self._gateway_client(). \
            call_method(self._j_stream, "partitionBy", j_partition)
        return self

    def sink(self, func):
        """
        Create a StreamSink with the given sink.

        Args:
            func: sink function.

        Returns:
            a StreamSink.
        """
        if not isinstance(func, function.SinkFunction):
            func = function.SimpleSinkFunction(func)
        j_func = self._gateway_client().create_py_func(
            function.serialize(func))
        j_stream = self._gateway_client(). \
            call_method(self._j_stream, "sink", j_func)
        return StreamSink(self, j_stream, func)


class KeyDataStream(Stream):
    """Represents a DataStream returned by a key-by operation.
     Wrapper of java io.ray.streaming.python.stream.PythonKeyDataStream
    """

    def __init__(self, input_stream, j_stream):
        super().__init__(input_stream, j_stream)

    def reduce(self, func):
        """
        Applies a reduce transformation on the grouped data stream grouped on
        by the given key function.
        The :class:`ray.streaming.function.ReduceFunction` will receive input
        values based on the key value. Only input values with the same key will
        go to the same reducer.

        Args:
            func: The ReduceFunction that will be called for every element of
            the input values with the same key. If `func` is a python function
            instead of a subclass of ReduceFunction, it will be wrapped as
            SimpleReduceFunction.

        Returns:
             A transformed DataStream.
        """
        if not isinstance(func, function.ReduceFunction):
            func = function.SimpleReduceFunction(func)
        j_func = self._gateway_client().create_py_func(
            function.serialize(func))
        j_stream = self._gateway_client(). \
            call_method(self._j_stream, "reduce", j_func)
        return DataStream(self, j_stream)


class StreamSource(DataStream):
    """Represents a source of the DataStream.
     Wrapper of java io.ray.streaming.python.stream.PythonStreamSource
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
     Wrapper of java io.ray.streaming.python.stream.PythonStreamSink
    """

    def __init__(self, input_stream, j_stream, func):
        super().__init__(input_stream, j_stream)
