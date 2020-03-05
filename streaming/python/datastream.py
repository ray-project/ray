from abc import ABC, abstractmethod

from ray.streaming import function
from ray.streaming import partition
from ray.streaming.runtime import gateway_client


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

    def get_streaming_context(self):
        return self.streaming_context

    def get_parallelism(self):
        """
        Returns:
            the parallelism of this transformation
        """
        bytes_data = self._gateway_client(). \
            call_method(self._j_stream, "getParallelism")
        return gateway_client.deserialize(bytes_data)

    def set_parallelism(self, parallelism: int):
        """Sets the parallelism of this transformation

        Args:
            parallelism: The new parallelism to set on this transformation

        Returns:
            self
        """
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
        bytes_data = self._gateway_client(). \
            call_method(self._j_stream, "getId")
        return gateway_client.deserialize(bytes_data)

    @abstractmethod
    def get_language(self):
        pass

    def _gateway_client(self):
        return self.get_streaming_context()._gateway_client


class DataStream(Stream):
    """
    Represents a stream of data which applies a transformation executed by
    python. It's also a wrapper of java
    `org.ray.streaming.python.stream.PythonDataStream`
    """

    def __init__(self, input_stream, j_stream, streaming_context=None):
        super().__init__(
            input_stream, j_stream, streaming_context=streaming_context)

    def get_language(self):
        return function.Language.PYTHON

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
        self._check_partition_call()
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
        self._check_partition_call()
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
        self._check_partition_call()
        if not isinstance(partition_func, partition.Partition):
            partition_func = partition.SimplePartition(partition_func)
        j_partition = self._gateway_client().create_py_func(
            partition.serialize(partition_func))
        self._gateway_client(). \
            call_method(self._j_stream, "partitionBy", j_partition)
        return self

    def _check_partition_call(self):
        """
        If parent stream is a java stream, we can't call partition related
        methods in the python stream
        """
        if self.input_stream.get_language() == function.Language.JAVA:
            raise Exception("Partition related methods can't be called on a"
                            "python stream if parent stream is a java stream.")

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

    def as_java(self):
        """
        Convert this stream as a java JavaDataStream.
        The converted stream and this stream are the same logical stream,
        which has same stream id. Changes in converted stream will be reflected
        in this stream and vice versa.
        """
        j_stream = self._gateway_client(). \
            call_method(self._j_stream, "asJavaStream")
        return JavaDataStream(self, j_stream)


class JavaDataStream(Stream):
    """
    Represents a stream of data which applies a transformation executed by
    java. It's also a wrapper of java
    `org.ray.streaming.api.stream.DataStream`
    """

    def __init__(self, input_stream, j_stream, streaming_context=None):
        super().__init__(
            input_stream, j_stream, streaming_context=streaming_context)

    def get_language(self):
        return function.Language.JAVA

    def map(self, java_func_class):
        """See org.ray.streaming.api.stream.DataStream.map"""
        return JavaDataStream(self, self._unary_call("map", java_func_class))

    def flat_map(self, java_func_class):
        """See org.ray.streaming.api.stream.DataStream.flatMap"""
        return JavaDataStream(self, self._unary_call("flatMap",
                                                     java_func_class))

    def filter(self, java_func_class):
        """See org.ray.streaming.api.stream.DataStream.filter"""
        return JavaDataStream(self, self._unary_call("filter",
                                                     java_func_class))

    def key_by(self, java_func_class):
        """See org.ray.streaming.api.stream.DataStream.keyBy"""
        self._check_partition_call()
        return JavaKeyDataStream(self,
                                 self._unary_call("keyBy", java_func_class))

    def broadcast(self, java_func_class):
        """See org.ray.streaming.api.stream.DataStream.broadcast"""
        self._check_partition_call()
        return JavaDataStream(self,
                              self._unary_call("broadcast", java_func_class))

    def partition_by(self, java_func_class):
        """See org.ray.streaming.api.stream.DataStream.partitionBy"""
        self._check_partition_call()
        return JavaDataStream(self,
                              self._unary_call("partitionBy", java_func_class))

    def sink(self, java_func_class):
        """See org.ray.streaming.api.stream.DataStream.sink"""
        return JavaStreamSink(self, self._unary_call("sink", java_func_class))

    def as_python(self):
        """
        Convert this stream as a python DataStream.
        The converted stream and this stream are the same logical stream,
        which has same stream id. Changes in converted stream will be reflected
        in this stream and vice versa.
        """
        j_stream = self._gateway_client(). \
            call_method(self._j_stream, "asPythonStream")
        return DataStream(self, j_stream)

    def _check_partition_call(self):
        """
        If parent stream is a python stream, we can't call partition related
        methods in the java stream
        """
        if self.input_stream.get_language() == function.Language.JAVA:
            raise Exception("Partition related methods can't be called on a"
                            "java stream if parent stream is a python stream.")

    def _unary_call(self, func_name, java_func_class):
        j_func = self._gateway_client().new_instance(java_func_class)
        j_stream = self._gateway_client(). \
            call_method(self._j_stream, func_name, j_func)
        return j_stream


class KeyDataStream(DataStream):
    """Represents a DataStream returned by a key-by operation.
     Wrapper of java org.ray.streaming.python.stream.PythonKeyDataStream
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

    def as_java(self):
        """
        Convert this stream as a java KeyDataStream.
        The converted stream and this stream are the same logical stream,
        which has same stream id. Changes in converted stream will be reflected
        in this stream and vice versa.
        """
        j_stream = self._gateway_client(). \
            call_method(self._j_stream, "asJavaStream")
        return JavaKeyDataStream(self, j_stream)


class JavaKeyDataStream(JavaDataStream):
    """
    Represents a DataStream returned by a key-by operation in java.
     Wrapper of org.ray.streaming.api.stream.KeyDataStream
    """

    def __init__(self, input_stream, j_stream):
        super().__init__(input_stream, j_stream)

    def reduce(self, java_func_class):
        """See org.ray.streaming.api.stream.KeyDataStream.reduce"""
        return JavaDataStream(self,
                              super()._unary_call("reduce", java_func_class))

    def as_python(self):
        """
        Convert this stream as a python KeyDataStream.
        The converted stream and this stream are the same logical stream,
        which has same stream id. Changes in converted stream will be reflected
        in this stream and vice versa.
        """
        j_stream = self._gateway_client(). \
            call_method(self._j_stream, "asPythonStream")
        return KeyDataStream(self, j_stream)


class StreamSource(DataStream):
    """Represents a source of the DataStream.
     Wrapper of java org.ray.streaming.python.stream.PythonStreamSource
    """

    def __init__(self, j_stream, streaming_context, source_func):
        super().__init__(None, j_stream, streaming_context=streaming_context)
        self.source_func = source_func

    def get_language(self):
        return function.Language.PYTHON

    @staticmethod
    def build_source(streaming_context, func):
        """Build a StreamSource source from a source function.
        Args:
            streaming_context: Stream context
            func: A instance of `SourceFunction`
        Returns:
            A StreamSource
        """
        j_stream = streaming_context._gateway_client. \
            create_py_stream_source(function.serialize(func))
        return StreamSource(j_stream, streaming_context, func)


class JavaStreamSource(JavaDataStream):
    """Represents a source of the java DataStream.
     Wrapper of java org.ray.streaming.api.stream.DataStreamSource
    """

    def __init__(self, j_stream, streaming_context):
        super().__init__(None, j_stream, streaming_context=streaming_context)

    def get_language(self):
        return function.Language.JAVA

    @staticmethod
    def build_source(streaming_context, java_source_func_class):
        """Build a java StreamSource source from a java source function.
        Args:
            streaming_context: Stream context
            java_source_func_class: qualified class name of java SourceFunction
        Returns:
            A java StreamSource
        """
        j_func = streaming_context._gateway_client() \
            .new_instance(java_source_func_class)
        j_stream = streaming_context._gateway_client() \
            .call_function("org.ray.streaming.api.stream.DataStreamSource"
                           "fromSource", streaming_context._j_ctx, j_func)
        return JavaStreamSource(j_stream, streaming_context)


class StreamSink(Stream):
    """Represents a sink of the DataStream.
     Wrapper of java org.ray.streaming.python.stream.PythonStreamSink
    """

    def __init__(self, input_stream, j_stream, func):
        super().__init__(input_stream, j_stream)

    def get_language(self):
        return function.Language.PYTHON


class JavaStreamSink(Stream):
    """Represents a sink of the java DataStream.
     Wrapper of java org.ray.streaming.api.stream.StreamSink
    """

    def __init__(self, input_stream, j_stream):
        super().__init__(input_stream, j_stream)

    def get_language(self):
        return function.Language.JAVA
