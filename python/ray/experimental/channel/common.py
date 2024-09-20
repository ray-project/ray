import asyncio
import concurrent
import copy
import threading
import time
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Dict, List, NamedTuple, Optional, Tuple, Union

import ray
from ray.experimental.channel.gpu_communicator import GPUCommunicator
from ray.experimental.channel.serialization_context import _SerializationContext
from ray.util.annotations import DeveloperAPI, PublicAPI

# The context singleton on this process.
_default_context: "Optional[ChannelContext]" = None
_context_lock = threading.Lock()

if TYPE_CHECKING:
    import torch


# Holds the input arguments for an accelerated DAG node.
@PublicAPI(stability="alpha")
class RayDAGArgs(NamedTuple):
    args: Tuple[Any, ...]
    kwargs: Dict[str, Any]


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
        reader_and_node_list: List[Tuple["ray.actor.ActorHandle", str]],
        read_by_adag_driver: bool,
    ) -> "ChannelInterface":
        """
        Instantiate a ChannelInterface class that can be used
        to pass data of this type.

        Args:
            writer: The actor that may write to the channel. None signifies the driver.
            reader_and_node_list: A list of tuples, where each tuple contains a reader
                actor handle and the node ID where the actor is located.
            read_by_adag_driver: True if a channel is read by an aDAG driver (Ray driver
                or an actor that creates the aDAG).
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

    def get_custom_nccl_group(self) -> Optional[GPUCommunicator]:
        """
        Return the custom NCCL group if one is specified.
        """
        if self._contains_type is not None:
            return self._contains_type.get_custom_nccl_group()
        return None

    def set_nccl_group_id(self, group_id: str) -> None:
        raise NotImplementedError


@DeveloperAPI
@dataclass
class ChannelContext:
    serialization_context = _SerializationContext()
    _torch_device: Optional["torch.device"] = None

    def __init__(self):
        # Used for the torch.Tensor NCCL transport.
        self.nccl_groups: Dict[str, "GPUCommunicator"] = {}

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

    def write(self, value: Any, timeout: Optional[float] = None) -> None:
        """
        Write a value to the channel.

        Blocks if there are still pending readers for the previous value. The
        writer may not write again until the specified number of readers have
        read the value.

        Args:
            value: The value to write.
            timeout: The maximum time in seconds to wait to write the value.
                None means using default timeout, 0 means immediate timeout
                (immediate success or timeout without blocking), -1 means
                infinite timeout (block indefinitely).
        """
        raise NotImplementedError

    def read(self, timeout: Optional[float] = None) -> Any:
        """
        Read the latest value from the channel. This call will block until a
        value is available to read.

        Subsequent calls to read() may *block* if the deserialized object is
        zero-copy (e.g., bytes or a numpy array) *and* the object is still in scope.

        Args:
            timeout: The maximum time in seconds to wait to read the value.
                None means using default timeout, 0 means immediate timeout
                (immediate success or timeout without blocking), -1 means
                infinite timeout (block indefinitely).

        Returns:
            Any: The deserialized value. If the deserialized value is an
            Exception, it will be returned directly instead of being raised.
        """

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
    def __init__(
        self,
        input_channels: List[ChannelInterface],
    ):
        assert isinstance(input_channels, list)
        for chan in input_channels:
            assert isinstance(chan, ChannelInterface)

        self._input_channels = input_channels
        self._closed = False
        self._num_reads = 0

    def get_num_reads(self) -> int:
        return self._num_reads

    def start(self):
        raise NotImplementedError

    def _read_list(self, timeout: Optional[float] = None) -> List[Any]:
        """
        Read a list of values from this reader.

        Args:
            timeout: The maximum time in seconds to wait for reading.
                None means using default timeout, 0 means immediate timeout
                (immediate success or timeout without blocking), -1 means
                infinite timeout (block indefinitely).

        """
        raise NotImplementedError

    def read(self, timeout: Optional[float] = None) -> List[Any]:
        """
        Read from this reader.

        Args:
            timeout: The maximum time in seconds to wait for reading.
                None means using default timeout, 0 means immediate timeout
                (immediate success or timeout without blocking), -1 means
                infinite timeout (block indefinitely).
        """
        assert (
            timeout is None or timeout >= 0 or timeout == -1
        ), "Timeout must be non-negative or -1."
        outputs = self._read_list(timeout)
        self._num_reads += 1
        return outputs

    def close(self) -> None:
        self._closed = True
        for channel in self._input_channels:
            channel.close()


@DeveloperAPI
class SynchronousReader(ReaderInterface):
    def __init__(
        self,
        input_channels: List[ChannelInterface],
    ):
        super().__init__(input_channels)

    def start(self):
        pass

    def _read_list(self, timeout: Optional[float] = None) -> List[Any]:
        results = []
        for c in self._input_channels:
            start_time = time.monotonic()
            results.append(c.read(timeout))
            if timeout is not None:
                timeout -= time.monotonic() - start_time
                timeout = max(timeout, 0)
        return results


@DeveloperAPI
class AwaitableBackgroundReader(ReaderInterface):
    """
    Asyncio-compatible channel reader.

    The reader is constructed with an async queue of futures whose values it
    will fulfill. It uses a threadpool to execute the blocking calls to read
    from the input channel(s).
    """

    def __init__(
        self,
        input_channels: List[ChannelInterface],
        fut_queue: asyncio.Queue,
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
        return [c.read() for c in self._input_channels]

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
            # NOTE(swang): If the object is zero-copy deserialized, then it
            # will stay in scope as long as ret and the future are in scope.
            # Therefore, we must delete both here after fulfilling the future.
            del res
            del fut

    def close(self):
        super().close()
        self._background_task_executor.shutdown(cancel_futures=True)


@DeveloperAPI
class WriterInterface:
    def __init__(
        self,
        output_channels: List[ChannelInterface],
        output_idxs: List[Optional[Union[int, str]]],
        is_input=False,
    ):
        """
        Initialize the writer.

        Args:
            output_channels: The output channels to write to.
            output_idxs: The indices of the values to write to each channel.
                This has the same length as `output_channels`. If `is_input` is True,
                the index can be an integer or a string to retrieve the corresponding
                value from `args` or `kwargs` in the DAG's input. If `is_input`
                is False, the entire value is written if the index is None. Otherwise,
                the value at the specified index in the tuple is written.
            is_input: Whether the writer is DAG input writer or not.
        """

        assert len(output_channels) == len(output_idxs)
        self._output_channels = output_channels
        self._output_idxs = output_idxs
        self._closed = False
        self._num_writes = 0
        self._is_input = is_input

    def get_num_writes(self) -> int:
        return self._num_writes

    def start(self):
        raise NotImplementedError()

    def write(self, val: Any, timeout: Optional[float] = None) -> None:
        """
        Write the value.

        Args:
            timeout: The maximum time in seconds to wait for writing.
                None means using default timeout, 0 means immediate timeout
                (immediate success or timeout without blocking), -1 means
                infinite timeout (block indefinitely).
        """
        raise NotImplementedError()

    def close(self) -> None:
        self._closed = True
        for channel in self._output_channels:
            channel.close()


def _adapt(raw_args: Any, key: Optional[Union[int, str]], is_input: bool):
    """
    Adapt the raw arguments to the key. If `is_input` is True, this method will
    retrieve the value from the input data for an InputAttributeNode. Otherwise, it
    will retrieve either a partial value or the entire value from the output of
    a ClassMethodNode.

    Args:
        raw_args: The raw arguments to adapt.
        key: The key to adapt.
        is_input: Whether the writer is DAG input writer or not.
    """
    if is_input:
        if not isinstance(raw_args, RayDAGArgs):
            # Fast path for a single input.
            return raw_args
        else:
            args = raw_args.args
            kwargs = raw_args.kwargs

        if isinstance(key, int):
            return args[key]
        else:
            return kwargs[key]
    else:
        if key is not None:
            return raw_args[key]
        else:
            return raw_args


@DeveloperAPI
class SynchronousWriter(WriterInterface):
    def start(self):
        for channel in self._output_channels:
            channel.ensure_registered_as_writer()

    def write(self, val: Any, timeout: Optional[float] = None) -> None:
        # If it is an exception, there's only 1 return value.
        # We have to send the same data to all channels.
        if isinstance(val, Exception):
            if len(self._output_channels) > 1:
                val = tuple(val for _ in range(len(self._output_channels)))

        if not self._is_input:
            if len(self._output_channels) > 1:
                if not isinstance(val, tuple):
                    raise ValueError(
                        f"Expected a tuple of {len(self._output_channels)} outputs, "
                        f"but got {type(val)}"
                    )
                if len(val) != len(self._output_channels):
                    raise ValueError(
                        f"Expected {len(self._output_channels)} outputs, but got "
                        f"{len(val)} outputs"
                    )

        for i, channel in enumerate(self._output_channels):
            idx = self._output_idxs[i]
            val_i = _adapt(val, idx, self._is_input)
            channel.write(val_i, timeout)
        self._num_writes += 1


@DeveloperAPI
class AwaitableBackgroundWriter(WriterInterface):
    def __init__(
        self,
        output_channels: List[ChannelInterface],
        output_idxs: List[Optional[Union[int, str]]],
        max_queue_size: Optional[int] = None,
        is_input=False,
    ):
        super().__init__(output_channels, output_idxs, is_input=is_input)
        if max_queue_size is None:
            from ray.dag import DAGContext

            ctx = DAGContext.get_current()
            max_queue_size = ctx.asyncio_max_queue_size
        self._queue = asyncio.Queue(max_queue_size)
        self._background_task = None
        self._background_task_executor = concurrent.futures.ThreadPoolExecutor(
            max_workers=1, thread_name_prefix="channel.AwaitableBackgroundWriter"
        )

    def start(self):
        for channel in self._output_channels:
            channel.ensure_registered_as_writer()
        self._background_task = asyncio.ensure_future(self.run())

    def _run(self, res):
        if not self._is_input:
            if len(self._output_channels) > 1:
                if not isinstance(res, tuple):
                    raise ValueError(
                        f"Expected a tuple of {len(self._output_channels)} outputs, "
                        f"but got {type(res)}"
                    )
                if len(res) != len(self._output_channels):
                    raise ValueError(
                        f"Expected {len(self._output_channels)} outputs, but got "
                        f"{len(res)} outputs"
                    )

        for i, channel in enumerate(self._output_channels):
            idx = self._output_idxs[i]
            res_i = _adapt(res, idx, self._is_input)
            channel.write(res_i)

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
