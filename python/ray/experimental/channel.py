import asyncio
import concurrent
import io
import logging
from typing import Any, List, Optional

import ray
from ray.util.annotations import DeveloperAPI, PublicAPI

# Logger for this module. It should be configured at the entry point
# into the program using Ray. Ray provides a default configuration at
# entry/init points.
logger = logging.getLogger(__name__)


def _get_node_id(self) -> "ray.NodeID":
    return ray.get_runtime_context().get_node_id()


def _create_channel_ref(
    self,
    buffer_size_bytes: int,
) -> "ray.ObjectRef":
    """
    Create a channel that can be read and written by co-located Ray processes.

    The channel has no buffer, so the writer will block until reader(s) have
    read the previous value.

    Args:
        buffer_size_bytes: The number of bytes to allocate for the object data and
            metadata. Writes to the channel must produce serialized data and
            metadata less than or equal to this value.
    Returns:
        Channel: A wrapper around ray.ObjectRef.
    """
    worker = ray._private.worker.global_worker
    worker.check_connected()

    value = b"0" * buffer_size_bytes

    try:
        object_ref = worker.put_object(
            value, owner_address=None, _is_experimental_channel=True
        )
    except ray.exceptions.ObjectStoreFullError:
        logger.info(
            "Put failed since the value was either too large or the "
            "store was full of pinned objects."
        )
        raise
    return object_ref


@PublicAPI(stability="alpha")
class Channel:
    """
    A wrapper type for ray.ObjectRef. Currently supports ray.get but not
    ray.wait.
    """

    def __init__(
        self,
        readers: List[Optional[ray.actor.ActorHandle]],
        buffer_size_bytes: int,
        _writer_node_id: Optional["ray.NodeID"] = None,
        _reader_node_id: Optional["ray.NodeID"] = None,
        _writer_ref: Optional["ray.ObjectRef"] = None,
        _reader_ref: Optional["ray.ObjectRef"] = None,
    ):
        """
        Create a channel that can be read and written by co-located Ray processes.

        Anyone may write to or read from the channel. The channel has no
        buffer, so the writer will block until reader(s) have read the previous
        value.

        Args:
            buffer_size_bytes: The number of bytes to allocate for the object data and
                metadata. Writes to the channel must produce serialized data and
                metadata less than or equal to this value.
        Returns:
            Channel: A wrapper around ray.ObjectRef.
        """
        is_creator = False
        assert len(readers) > 0

        if _writer_ref is None:
            if not isinstance(buffer_size_bytes, int):
                raise ValueError("buffer_size_bytes must be an integer")

            self._writer_node_id = (
                ray.runtime_context.get_runtime_context().get_node_id()
            )
            self._writer_ref = _create_channel_ref(self, buffer_size_bytes)

            if readers[0] is None:
                # Reader is the driver. We assume that the reader and the writer are on
                # the same node.
                self._reader_node_id = self._writer_node_id
                self._reader_ref = self._writer_ref
            else:
                # Reader and writer are on different nodes.
                fn = readers[0].__ray_call__
                self._reader_node_id = ray.get(fn.remote(_get_node_id))
                for reader in readers:
                    fn = reader.__ray_call__
                    reader_node_id = ray.get(fn.remote(_get_node_id))
                    if reader_node_id != self._reader_node_id:
                        raise NotImplementedError(
                            "All readers must be on the same node for now."
                        )
                if self.is_remote():
                    self._reader_ref = ray.get(
                        fn.remote(_create_channel_ref, buffer_size_bytes)
                    )
                else:
                    self._reader_ref = self._writer_ref

            is_creator = True
        else:
            assert (
                _writer_node_id is not None
            ), "_writer_node_id must also be passed to the constructor when "
            "_writer_ref is."
            assert (
                _reader_ref is not None
            ), "_reader_ref must also be passed to the constructor when _writer_ref is."

            self._writer_ref = _writer_ref
            self._writer_node_id = _writer_node_id
            self._reader_node_id = _reader_node_id
            self._reader_ref = _reader_ref

        self._readers = readers
        self._buffer_size_bytes = buffer_size_bytes
        self._num_readers = len(self._readers)
        if self.is_remote():
            self._num_readers = 1

        self._worker = ray._private.worker.global_worker
        self._worker.check_connected()

        self._writer_registered = False
        self._reader_registered = False

        if is_creator:
            self.ensure_registered_as_writer()
            assert self._reader_ref is not None

    @staticmethod
    def is_local_node(node_id):
        return ray.runtime_context.get_runtime_context().get_node_id() == node_id

    def is_remote(self):
        return self._writer_node_id != self._reader_node_id

    def ensure_registered_as_writer(self):
        if self._writer_registered:
            return

        if not self.is_local_node(self._writer_node_id):
            raise ValueError(
                "`ensure_registered_as_writer()` must only be called on the node that "
                "the writer is on."
            )

        assert (
            self._reader_ref
        ), "`self._reader_ref` must be not be None when registering a writer, because "
        "it should have been initialized in the constructor."

        if len(self._readers) == 1 and self._readers[0] is None:
            actor_id = ray.ActorID.nil()
        else:
            actor_id = self._readers[0]._actor_id
        self._worker.core_worker.experimental_channel_register_writer(
            self._writer_ref,
            self._reader_ref,
            self._writer_node_id,
            self._reader_node_id,
            actor_id,
            len(self._readers),
        )
        self._writer_registered = True

    def ensure_registered_as_reader(self):
        if self._reader_registered:
            return

        self._worker.core_worker.experimental_channel_register_reader(
            self._reader_ref,
        )
        self._reader_registered = True

    @staticmethod
    def _deserialize_reader_channel(
        readers: list,
        buffer_size_bytes: int,
        writer_node_id,
        reader_node_id,
        writer_ref: "ray.ObjectRef",
        reader_ref: "ray.ObjectRef",
    ) -> "Channel":
        chan = Channel(
            readers,
            buffer_size_bytes,
            _writer_node_id=writer_node_id,
            _reader_node_id=reader_node_id,
            _writer_ref=writer_ref,
            _reader_ref=reader_ref,
        )
        return chan

    def __reduce__(self):
        assert self._reader_ref is not None
        return self._deserialize_reader_channel, (
            self._readers,
            self._buffer_size_bytes,
            self._writer_node_id,
            self._reader_node_id,
            self._writer_ref,
            self._reader_ref,
        )

    def write(self, value: Any):
        """
        Write a value to the channel.

        Blocks if there are still pending readers for the previous value. The
        writer may not write again until the specified number of readers have
        called ``end_read_channel``.

        Args:
            value: The value to write.
        """
        self.ensure_registered_as_writer()

        try:
            serialized_value = self._worker.get_serialization_context().serialize(value)
        except TypeError as e:
            sio = io.StringIO()
            ray.util.inspect_serializability(value, print_file=sio)
            msg = (
                "Could not serialize the put value "
                f"{repr(value)}:\n"
                f"{sio.getvalue()}"
            )
            raise TypeError(msg) from e

        self._worker.core_worker.experimental_channel_put_serialized(
            serialized_value,
            self._writer_ref,
            self._num_readers,
        )

    def begin_read(self) -> Any:
        """
        Read the latest value from the channel. This call will block until a
        value is available to read.

        Subsequent calls to begin_read() will *block*, until end_read() is
        called and the next value is available to read.

        Returns:
            Any: The deserialized value.
        """
        self.ensure_registered_as_reader()
        return ray.get(self._reader_ref)

    def end_read(self):
        """
        Signal to the writer that the channel is ready to write again.

        If begin_read is not called first, then this call will block until a
        value is written, then drop the value.
        """
        self.ensure_registered_as_reader()
        self._worker.core_worker.experimental_channel_read_release([self._reader_ref])

    def close(self) -> None:
        """
        Close this channel by setting the error bit on both the writer_ref and the
        reader_ref.

        Does not block. Any existing values in the channel may be lost after the
        channel is closed.
        """
        self._worker.core_worker.experimental_channel_set_error(self._writer_ref)
        if self.is_local_node(self._reader_node_id):
            self.ensure_registered_as_reader()
        self._worker.core_worker.experimental_channel_set_error(self._reader_ref)


# Interfaces for channel I/O.
@DeveloperAPI
class ReaderInterface:
    def __init__(self, input_channels: List[Channel]):
        if isinstance(input_channels, List):
            for chan in input_channels:
                assert isinstance(chan, Channel)
            self._has_single_output = False
        else:
            assert isinstance(input_channels, Channel)
            self._has_single_output = True
            input_channels = [input_channels]

        self._input_channels = input_channels
        self._closed = False
        self._num_reads = 0

    def get_num_reads(self) -> int:
        return self._num_reads

    def start(self):
        raise NotImplementedError

    def _begin_read_list(self) -> Any:
        raise NotImplementedError

    def begin_read(self) -> Any:
        outputs = self._begin_read_list()
        self._num_reads += 1
        if self._has_single_output:
            return outputs[0]
        else:
            return outputs

    def end_read(self) -> Any:
        raise NotImplementedError

    def close(self) -> None:
        self._closed = True
        for channel in self._input_channels:
            channel.close()


@DeveloperAPI
class SynchronousReader(ReaderInterface):
    def __init__(self, input_channels: List[Channel]):
        super().__init__(input_channels)

    def start(self):
        pass

    def _begin_read_list(self) -> Any:
        return [c.begin_read() for c in self._input_channels]

    def end_read(self) -> Any:
        for c in self._input_channels:
            c.end_read()


@DeveloperAPI
class AwaitableBackgroundReader(ReaderInterface):
    """
    Asyncio-compatible channel reader.

    The reader is constructed with an async queue of futures whose values it
    will fulfill. It uses a threadpool to execute the blocking calls to read
    from the input channel(s).
    """

    def __init__(self, input_channels: List[Channel], fut_queue: asyncio.Queue):
        super().__init__(input_channels)
        self._fut_queue = fut_queue
        self._background_task = None
        self._background_task_executor = concurrent.futures.ThreadPoolExecutor(
            max_workers=1, thread_name_prefix="channel.AwaitableBackgroundReader"
        )

    def start(self):
        self._background_task = asyncio.ensure_future(self.run())

    def _run(self):
        vals = [c.begin_read() for c in self._input_channels]
        if self._has_single_output:
            vals = vals[0]
        return vals

    async def run(self):
        loop = asyncio.get_running_loop()
        while not self._closed:
            res, fut = await asyncio.gather(
                loop.run_in_executor(self._background_task_executor, self._run),
                self._fut_queue.get(),
                return_exceptions=True,
            )

            # Set the result on the main thread.
            fut.set_result(res)

    def end_read(self) -> Any:
        for c in self._input_channels:
            c.end_read()

    def close(self):
        self._background_task.cancel()
        super().close()


@DeveloperAPI
class WriterInterface:
    def __init__(self, output_channel: Channel):
        self._output_channel = output_channel
        self._closed = False
        self._num_writes = 0

    def get_num_writes(self) -> int:
        return self._num_writes

    def start(self):
        raise NotImplementedError()

    def write(self, val: Any) -> None:
        raise NotImplementedError()

    def close(self) -> None:
        self._closed = True
        self._output_channel.close()


@DeveloperAPI
class SynchronousWriter(WriterInterface):
    def start(self):
        self._output_channel.ensure_registered_as_writer()
        pass

    def write(self, val: Any) -> None:
        self._output_channel.write(val)
        self._num_writes += 1


@DeveloperAPI
class AwaitableBackgroundWriter(WriterInterface):
    def __init__(self, output_channel: Channel, max_queue_size: Optional[int] = None):
        super().__init__(output_channel)
        if max_queue_size is None:
            max_queue_size = 0
        self._queue = asyncio.Queue(max_queue_size)
        self._background_task = None
        self._background_task_executor = concurrent.futures.ThreadPoolExecutor(
            max_workers=1, thread_name_prefix="channel.AwaitableBackgroundWriter"
        )

    def start(self):
        self._output_channel.ensure_registered_as_writer()
        self._background_task = asyncio.ensure_future(self.run())

    def _run(self, res):
        self._output_channel.write(res)

    async def run(self):
        loop = asyncio.get_event_loop()
        while True:
            res = await self._queue.get()
            await loop.run_in_executor(self._background_task_executor, self._run, res)

    async def write(self, val: Any) -> None:
        if self._closed:
            raise RuntimeError("DAG execution cancelled")
        await self._queue.put(val)
        self._num_writes += 1

    def close(self):
        self._background_task.cancel()
        super().close()
