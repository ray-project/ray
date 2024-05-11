import asyncio
import concurrent
import threading
from dataclasses import dataclass
from typing import Any, List, Optional

import ray
from ray.experimental.channel.nccl_group import _NcclGroup
from ray.util.annotations import DeveloperAPI, PublicAPI

# The context singleton on this process.
_default_context: "Optional[ChannelContext]" = None
_context_lock = threading.Lock()


@PublicAPI(stability="alpha")
class ChannelOutputType:
    @staticmethod
    def register_custom_serializer() -> None:
        """
        Register any custom serializers needed to pass data of this type.
        """
        pass

    def create_channel(
        self,
        writer: Optional["ray.actor.ActorHandle"],
        readers: List[Optional["ray.actor.ActorHandle"]],
    ) -> "ChannelInterface":
        """
        Instantiate a ChannelInterface class that can be used
        to pass data of this type.

        Args:
            writer: The actor that may write to the channel. None signifies the driver.
            readers: The actors that may read from the channel. None signifies
                the driver.
        Returns:
            A ChannelInterface that can be used to pass data
                of this type.
        """
        raise NotImplementedError


def _do_register_custom_serializers(
    self: Any, channel_output_types: List[type]
) -> None:
    """
    Register custom serializers for the given channel types. This method should
    be run on the reader(s) and writer of a channel, which are the driver
    and/or Ray actors.

    Args:
        self: This method should be run on the driver or the Ray actor. The Ray
            actor should be passed as `self`.
        channel_output_types: The list of channel output types to register.
    """
    for typ in channel_output_types:
        typ.register_custom_serializer(self)


@DeveloperAPI
@dataclass
class ChannelContext:
    # Used for the torch.Tensor NCCL transport.
    nccl_group: Optional["_NcclGroup"] = None

    @staticmethod
    def get_current() -> "ChannelContext":
        """Get or create a singleton context.

        If the context has not yet been created in this process, it will be
        initialized with default settings.
        """

        global _default_context

        with _context_lock:
            if _default_context is None:
                _default_context = ChannelContext()

            return _default_context


@PublicAPI(stability="alpha")
class ChannelInterface:
    """
    Abstraction for a transport between a writer actor and some number of
    reader actors.
    """

    def __init__(
        self,
        writer: Optional[ray.actor.ActorHandle],
        readers: List[Optional[ray.actor.ActorHandle]],
        typ: Optional["ChannelOutputType"],
    ):
        """
        Create a channel that can be read and written by a Ray driver or actor.

        Args:
            writer: The actor that may write to the channel. None signifies the driver.
            readers: The actors that may read from the channel. None signifies
                the driver.
            typ: Type information about the values passed through the channel.
        """
        pass

    def ensure_registered_as_writer(self):
        raise NotImplementedError

    def ensure_registered_as_reader(self):
        raise NotImplementedError

    def write(self, value: Any) -> None:
        """
        Write a value to the channel.

        Blocks if there are still pending readers for the previous value. The
        writer may not write again until the specified number of readers have
        called ``end_read``.

        Args:
            value: The value to write.
        """
        raise NotImplementedError

    def begin_read(self) -> Any:
        """
        Read the latest value from the channel. This call will block until a
        value is available to read.

        Subsequent calls to begin_read() will *block*, until end_read() is
        called and the next value is available to read.

        Returns:
            Any: The deserialized value.
        """
        raise NotImplementedError

    def end_read(self) -> None:
        """
        Signal to the writer that the channel is ready to write again.

        If begin_read is not called first, then this call will block until a
        value is written, then drop the value.
        """
        raise NotImplementedError

    def close(self) -> None:
        """
        Close this channel. This method must not block. Any existing values in
        the channel may be lost after the channel is closed.
        """
        raise NotImplementedError


# Interfaces for channel I/O.
@DeveloperAPI
class ReaderInterface:
    def __init__(self, input_channels: List[ChannelInterface]):
        if isinstance(input_channels, List):
            for chan in input_channels:
                assert isinstance(chan, ChannelInterface)
            self._has_single_output = False
        else:
            assert isinstance(input_channels, ChannelInterface)
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
    def __init__(self, input_channels: List[ChannelInterface]):
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

    def __init__(
        self, input_channels: List[ChannelInterface], fut_queue: asyncio.Queue
    ):
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
    def __init__(self, output_channel: ChannelInterface):
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
    def __init__(
        self, output_channel: ChannelInterface, max_queue_size: Optional[int] = None
    ):
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
