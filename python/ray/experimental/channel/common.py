import asyncio
import concurrent
import copy
import threading
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Dict, List, Optional

import ray
from ray.experimental.channel.nccl_group import _NcclGroup
from ray.experimental.channel.serialization_context import _SerializationContext
from ray.util.annotations import DeveloperAPI, PublicAPI

# The context singleton on this process.
_default_context: "Optional[ChannelContext]" = None
_context_lock = threading.Lock()

if TYPE_CHECKING:
    import torch


@PublicAPI(stability="alpha")
class ChannelOutputType:
    def __init__(self):
        self._contains_type: Optional["ChannelOutputType"] = None

    def register_custom_serializer(self) -> None:
        """
        Register any custom serializers needed to pass data of this type. This
        method should be run on the reader(s) and writer of a channel, which
        are the driver and/or Ray actors.

        NOTE: When custom serializers are registered with Ray, the registered
        deserializer is shipped with the serialized value and used on the
        receiving end. Therefore, the deserializer function should *not*
        capture state that is meant to be worker-local, such as the worker's
        default device. Instead, these should be extracted from the
        worker-local _SerializationContext.
        """
        if self._contains_type is not None:
            self._contains_type.register_custom_serializer()

    @property
    def is_direct_return(self) -> bool:
        """
        Some channels may contain other values that should be sent via a
        different channel. This returns whether the value is a direct return or
        if it is "nested" inside a different channel.
        """
        return True

    @property
    def contains_type(self) -> "ChannelOutputType":
        """
        Some channel values may contain an object that should be sent through a
        different channel. For example, a Python object containing a GPU tensor
        may be sent over two channels, one to serialize the Python data on CPU
        memory and another to transfer the GPU data over NCCL. This function
        returns the type of this nested value, if any.
        """
        return self._contains_type

    def set_contains_type(self, typ: "ChannelOutputType") -> None:
        """
        Mark that values sent on this channel may contain objects that should
        be sent through a different channel.
        """
        from ray.experimental.channel.torch_tensor_type import TorchTensorType

        if typ is not None:
            assert isinstance(
                typ, TorchTensorType
            ), "Contained type must be of type TorchTensorType"
        self._contains_type = copy.deepcopy(typ)

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

    def requires_nccl(self) -> bool:
        if self._contains_type is not None:
            if self._contains_type.requires_nccl():
                return True

        # By default, channels do not require NCCL.
        return False

    def set_nccl_group_id(self, group_id: str) -> None:
        raise NotImplementedError


@DeveloperAPI
@dataclass
class ChannelContext:
    serialization_context = _SerializationContext()
    _torch_device: Optional["torch.device"] = None

    def __init__(self):
        # Used for the torch.Tensor NCCL transport.
        self.nccl_groups: Dict[str, "_NcclGroup"] = {}

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

    @property
    def torch_device(self) -> "torch.device":
        if self._torch_device is None:

            if not ray.get_gpu_ids():
                import torch

                # torch_utils defaults to returning GPU 0 if no GPU IDs were assigned
                # by Ray. We instead want the default to be CPU.
                self._torch_device = torch.device("cpu")

            from ray.air._internal import torch_utils

            self._torch_device = torch_utils.get_devices()[0]

        return self._torch_device

    def set_torch_device(self, device: "torch.device"):
        self._torch_device = device


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
        """
        Check whether the process is a valid writer. This method must be idempotent.
        """
        raise NotImplementedError

    def ensure_registered_as_reader(self):
        """
        Check whether the process is a valid reader. This method must be idempotent.
        """
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
        Close this channel. This method must not block and it must be made
        idempotent. Any existing values in the channel may be lost after the
        channel is closed.
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
