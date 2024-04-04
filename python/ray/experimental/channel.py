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


def _create_channel_ref(
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
        buffer_size_bytes: Optional[int] = None,
        num_readers: int = 1,
        num_buffers: int = 1,
        _base_refs: Optional[List["ray.ObjectRef"]] = None,
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
        if buffer_size_bytes is None:
            if _base_refs is None:
                raise ValueError(
                    "One of `buffer_size_bytes` or `_base_refs` must be provided"
                )
            self._base_refs = _base_refs
        else:
            if not isinstance(buffer_size_bytes, int):
                raise ValueError("buffer_size_bytes must be an integer")
            self._base_refs = [
                _create_channel_ref(buffer_size_bytes) for _ in range(num_buffers)
            ]

        if not isinstance(num_readers, int):
            raise ValueError("num_readers must be an integer")

        self._begin_read_ref_idx = 0
        self._end_read_ref_idx = 0
        self._write_ref_idx = 0
        self._num_readers = num_readers
        self._worker = ray._private.worker.global_worker
        self._worker.check_connected()

        self._writer_registered = False
        self._reader_registered = False

    def _ensure_registered_as_writer(self):
        if self._writer_registered:
            return

        for ref in self._base_refs:
            self._worker.core_worker.experimental_channel_register_writer(ref)
        self._writer_registered = True

    def _ensure_registered_as_reader(self):
        if self._reader_registered:
            return

        for ref in self._base_refs:
            self._worker.core_worker.experimental_channel_register_reader(ref)
        self._reader_registered = True

    @staticmethod
    def _from_base_refs(
        base_refs: List["ray.ObjectRef"], num_readers: int
    ) -> "Channel":
        return Channel(num_readers=num_readers, _base_refs=base_refs)

    def __reduce__(self):
        return self._from_base_refs, (self._base_refs, self._num_readers)

    def _get_next_ref(self) -> "ray.ObjectRef":
        ref = self._base_refs[self._next_ref_idx]
        self._next_ref_idx += 1
        self._next_ref_idx %= len(self._base_refs)
        return ref

    def write(self, value: Any, num_readers: Optional[int] = None):
        """
        Write a value to the channel.

        Blocks if there are still pending readers for the previous value. The
        writer may not write again until the specified number of readers have
        called ``end_read_channel``.

        Args:
            value: The value to write.
            num_readers: The number of readers that must read and release the value
                before we can write again.
        """
        if num_readers is None:
            num_readers = self._num_readers
        if num_readers <= 0:
            raise ValueError("``num_readers`` must be a positive integer.")

        self._ensure_registered_as_writer()

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
            self._base_refs[self._write_ref_idx],
            num_readers,
        )

        self._write_ref_idx += 1
        self._write_ref_idx %= len(self._base_refs)

    def begin_read(self) -> Any:
        """
        Read the latest value from the channel. This call will block until a
        value is available to read.

        Subsequent calls to begin_read() will *block*, until end_read() is
        called and the next value is available to read.

        Returns:
            Any: The deserialized value.
        """
        self._ensure_registered_as_reader()
        exc = None
        try:
            val = ray.get(self._base_refs[self._begin_read_ref_idx])
        except Exception as e:
            exc = e

        self._begin_read_ref_idx += 1
        self._begin_read_ref_idx %= len(self._base_refs)

        if exc is not None:
            raise exc

        return val

    def end_read(self):
        """
        Signal to the writer that the channel is ready to write again.

        If begin_read is not called first, then this call will block until a
        value is written, then drop the value.
        """
        self._ensure_registered_as_reader()
        self._worker.core_worker.experimental_channel_read_release(
            [self._base_refs[self._end_read_ref_idx]]
        )
        self._end_read_ref_idx += 1
        self._end_read_ref_idx %= len(self._base_refs)

    def close(self) -> None:
        """
        Close this channel by setting the error bit on the object.

        Does not block. Any existing values in the channel may be lost after the
        channel is closed.
        """
        logger.debug(f"Setting error bit on channels: {self._base_refs}")
        self._ensure_registered_as_writer()
        for base_ref in self._base_refs:
            self._worker.core_worker.experimental_channel_set_error(base_ref)


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
        self._num_reads_complete = 0

    def get_num_reads_complete(self) -> int:
        return self._num_reads_complete

    def start(self):
        raise NotImplementedError

    def _begin_read_list(self) -> Any:
        raise NotImplementedError

    def begin_read(self) -> Any:
        outputs = self._begin_read_list()
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
        self._num_reads_complete += 1


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
        self._num_reads_complete += 1

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
        self._output_channel._ensure_registered_as_writer()
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
        self._output_channel._ensure_registered_as_writer()
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
