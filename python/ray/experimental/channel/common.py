import asyncio
import concurrent
import sys
import threading
import time
from dataclasses import dataclass
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    List,
    NamedTuple,
    Optional,
    Tuple,
    Union,
)

import ray
import ray.exceptions
from ray.experimental.channel.communicator import Communicator
from ray.experimental.channel.serialization_context import _SerializationContext
from ray.util.annotations import DeveloperAPI, PublicAPI

# The context singleton on this process.
_default_context: "Optional[ChannelContext]" = None
_context_lock = threading.Lock()

if TYPE_CHECKING:
    import torch


def retry_and_check_interpreter_exit(f: Callable[[], None]) -> bool:
    """This function is only useful when f contains channel read/write.

    Keep retrying channel read/write inside `f` and check if interpreter exits.
    It is important in case the read/write happens in a separate thread pool.
    See https://github.com/ray-project/ray/pull/47702

    f should a function that doesn't receive any input and return nothing.
    """
    exiting = False
    while True:
        try:
            f()
            break
        except ray.exceptions.RayChannelTimeoutError:
            if sys.is_finalizing():
                # Interpreter exits. We should ignore the error and
                # stop reading so that the thread can join.
                exiting = True
                break

    return exiting


# Holds the input arguments for Compiled Graph
@PublicAPI(stability="alpha")
class CompiledDAGArgs(NamedTuple):
    args: Tuple[Any, ...]
    kwargs: Dict[str, Any]


@PublicAPI(stability="alpha")
class ChannelOutputType:
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
        pass

    def create_channel(
        self,
        writer: Optional["ray.actor.ActorHandle"],
        reader_and_node_list: List[Tuple["ray.actor.ActorHandle", str]],
        driver_actor_id: Optional[str] = None,
    ) -> "ChannelInterface":
        """
        Instantiate a ChannelInterface class that can be used
        to pass data of this type.

        Args:
            writer: The actor that may write to the channel. None signifies the driver.
            reader_and_node_list: A list of tuples, where each tuple contains a reader
                actor handle and the node ID where the actor is located.
            driver_actor_id: If this is a CompositeChannel that is read by a driver and
                that driver is an actual actor, this will be the actor ID of that
                driver actor.
        Returns:
            A ChannelInterface that can be used to pass data
                of this type.
        """
        raise NotImplementedError

    def requires_nccl(self) -> bool:
        # By default, channels do not require NCCL.
        return False

    def get_custom_communicator(self) -> Optional[Communicator]:
        """
        Return the custom NCCL group if one is specified.
        """
        return None

    def set_communicator_id(self, group_id: str) -> None:
        raise NotImplementedError


@DeveloperAPI
@dataclass
class ChannelContext:
    serialization_context = _SerializationContext()
    _torch_available: Optional[bool] = None
    _torch_device: Optional["torch.device"] = None
    _current_stream: Optional["torch.cuda.Stream"] = None

    def __init__(self):
        # Used for the torch.Tensor NCCL transport.
        self.communicators: Dict[str, "Communicator"] = {}

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
    def torch_available(self) -> bool:
        """
        Check if torch package is available.
        """
        if self._torch_available is not None:
            return self._torch_available

        try:
            import torch  # noqa: F401
        except ImportError:
            self._torch_available = False
            return False
        self._torch_available = True
        return True

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

        # A list of channels that were not read in the last `read` call
        # because the reader returned immediately when a RayTaskError was found.
        # These channels must be consumed before the next read to avoid reading
        # stale data remaining from the last read.
        self._leftover_channels: List[ChannelInterface] = []

    def get_num_reads(self) -> int:
        return self._num_reads

    def start(self):
        raise NotImplementedError

    def _read_list(self, timeout: Optional[float] = None) -> List[Any]:
        """
        Read a list of values from this reader.

        Args:
            timeout: The maximum time in seconds to wait for reading.
                None means using default timeout which is infinite, 0 means immediate
                timeout (immediate success or timeout without blocking), -1 means
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

    def _consume_leftover_channels_if_needed(
        self, timeout: Optional[float] = None
    ) -> None:
        # Consume the channels that were not read in the last `read` call because a
        # RayTaskError was returned from another channel. If we don't do this, the
        # read operation will read stale versions of the object refs.
        #
        # If a RayTaskError is returned from a leftover channel, it will be ignored.
        # If a read operation times out, a RayChannelTimeoutError exception will be
        # raised.
        #
        # TODO(kevin85421): Currently, a DAG with NCCL channels and fast fail enabled
        # may not be reusable. Revisit this in the future.
        for c in self._leftover_channels:
            start_time = time.monotonic()
            c.read(timeout)
            if timeout is not None:
                timeout -= time.monotonic() - start_time
                timeout = max(timeout, 0)
        self._leftover_channels = []


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
        self._consume_leftover_channels_if_needed(timeout)
        # We don't update `remaining_timeout` here because in the worst case,
        # consuming leftover channels requires reading all `_input_channels`,
        # which users expect to complete within the original `timeout`. Updating
        # `remaining_timeout` could cause unexpected timeouts in subsequent read
        # operations.

        # It is a special case that `timeout` is set to 0, which means
        # read once for each channel.
        is_zero_timeout = timeout == 0

        results = [None for _ in range(len(self._input_channels))]
        if timeout is None or timeout == -1:
            timeout = float("inf")
        timeout_point = time.monotonic() + timeout
        remaining_timeout = timeout

        from ray.dag import DAGContext

        ctx = DAGContext.get_current()
        iteration_timeout = ctx.read_iteration_timeout

        # Iterate over the input channels with a shorter timeout for each iteration
        # to detect RayTaskError early and fail fast.
        done_channels = set()
        while len(done_channels) < len(self._input_channels):
            for i, c in enumerate(self._input_channels):
                if c in done_channels:
                    continue
                try:
                    result = c.read(min(remaining_timeout, iteration_timeout))
                    results[i] = result
                    done_channels.add(c)
                    if isinstance(result, ray.exceptions.RayTaskError):
                        # If we raise an exception immediately, it will be considered
                        # as a system error which will cause the execution loop to
                        # exit. Hence, return immediately and let `_process_return_vals`
                        # handle the exception.
                        #
                        # Return a list of RayTaskError so that the caller will not
                        # get an undefined partial result.
                        self._leftover_channels = [
                            c for c in self._input_channels if c not in done_channels
                        ]
                        return [result for _ in range(len(self._input_channels))]
                except ray.exceptions.RayChannelTimeoutError as e:
                    remaining_timeout = max(timeout_point - time.monotonic(), 0)
                    if remaining_timeout == 0:
                        raise e
                    continue

                remaining_timeout = max(timeout_point - time.monotonic(), 0)
                if remaining_timeout == 0 and not is_zero_timeout:
                    raise ray.exceptions.RayChannelTimeoutError(
                        f"Cannot read all channels within {timeout} seconds"
                    )
        return results

    def release_channel_buffers(self, timeout: Optional[float] = None) -> None:
        for c in self._input_channels:
            start_time = time.monotonic()
            assert hasattr(
                c, "release_buffer"
            ), "release_buffer() is only supported for shared memory channel "
            "(e.g., Channel, BufferedSharedMemoryChannel, CompositeChannel) "
            "and used between the last actor and the driver, but got a channel"
            f" of type {type(c)}."
            c.release_buffer(timeout)
            if timeout is not None:
                timeout -= time.monotonic() - start_time
                timeout = max(timeout, 0)


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
        # Give it a default timeout 60 seconds to release the buffers
        # of the channels that were not read in the last `read` call.
        self._consume_leftover_channels_if_needed(60)

        results = [None for _ in range(len(self._input_channels))]

        from ray.dag import DAGContext

        ctx = DAGContext.get_current()
        iteration_timeout = ctx.read_iteration_timeout

        done_channels = set()
        while len(done_channels) < len(self._input_channels):
            for i, c in enumerate(self._input_channels):
                if c in done_channels:
                    continue
                try:
                    result = c.read(iteration_timeout)
                    results[i] = result
                    done_channels.add(c)
                    if isinstance(result, ray.exceptions.RayTaskError):
                        self._leftover_channels = [
                            c for c in self._input_channels if c not in done_channels
                        ]
                        return [result for _ in range(len(self._input_channels))]
                except ray.exceptions.RayChannelTimeoutError:
                    pass
                if sys.is_finalizing():
                    return results
        return results

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
        self._background_task.cancel()


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
            timeout: The maximum time in seconds to wait for writing. 0 means
                immediate timeout (immediate success or timeout without blocking).
                -1 and None mean infinite timeout (blocks indefinitely).
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
        if not isinstance(raw_args, CompiledDAGArgs):
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
        is_input=False,
    ):
        super().__init__(output_channels, output_idxs, is_input=is_input)
        self._queue = asyncio.Queue()
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
            exiting = retry_and_check_interpreter_exit(
                lambda: channel.write(res_i, timeout=1)
            )
            if exiting:
                break

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
