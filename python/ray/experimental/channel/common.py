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
    Set,
    Tuple,
    Union,
)

import ray
import ray.exceptions
from ray import ObjectRef
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


# Holds the input arguments for an accelerated DAG node.
@PublicAPI(stability="alpha")
class RayDAGArgs(NamedTuple):
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
        if self._contains_type is not None:
            return self._contains_type.get_custom_nccl_group()
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

    def get_ray_waitables(self) -> List[Tuple[ObjectRef, bool]]:
        """
        Get a list of tuples containing an ObjectRef and a boolean flag.
        The flag indicates whether the ObjectRef should skip deserialization
        in `experimental_wait_and_get_mutable_objects` and instead be
        deserialized in the channel's `read()` method instead.
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
        # List of object refs that were not completed in the last read.
        # The reader returns immediately when a RayTaskError is found.
        # This list must be consumed before the next read to avoid reading
        # stale data remaining from the last read.
        self._non_complete_object_refs: List[ObjectRef] = []

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

    def _get_all_waitables_to_num_consumers(
        self,
    ) -> Tuple[Dict[ObjectRef, int], Dict[ObjectRef, int]]:
        """
        Returns two lists of tuples, where each tuple contains an ObjectRef and an
        integer. The integer indicates the number of consumers for the ObjectRef.

        The first list contains mutable object refs that need to be deserialized
        when we first retrieve them (i.e., in `_read_list`). The second list
        contains mutable object refs that do not need to be deserialized in `_read_list`
        and instead need to be deserialized in the channel's `read()` method.
        """
        waitable_to_num_consumers = {}
        skip_deserialization_waitables_to_num_consumers = {}
        for c in self._input_channels:
            waitables = c.get_ray_waitables()
            for waitable, skip_deserialization in waitables:
                target_dict = (
                    skip_deserialization_waitables_to_num_consumers
                    if skip_deserialization
                    else waitable_to_num_consumers
                )
                target_dict[waitable] = target_dict.get(waitable, 0) + 1
        return (
            waitable_to_num_consumers,
            skip_deserialization_waitables_to_num_consumers,
        )

    def _consume_non_complete_object_refs_if_needed(
        self, timeout: Optional[float] = None
    ) -> None:
        timeout_point = time.monotonic() + timeout
        worker = ray._private.worker.global_worker
        if len(self._non_complete_object_refs) > 0:
            # If the last read failed early, we need to consume the data from
            # the non-complete object refs before the next read. If we don't do
            # this, the read operation will read different versions of the
            # object refs.
            (
                _,
                non_complete_object_refs_set,
            ) = worker.experimental_wait_and_get_mutable_objects(
                self._non_complete_object_refs,
                num_returns=len(self._non_complete_object_refs),
                timeout_ms=max(0, (timeout_point - time.monotonic()) * 1000),
                return_exceptions=True,
                # Skip deserialization to speed up this step.
                skip_deserialization=True,
                suppress_timeout_errors=False,
            )
            assert len(non_complete_object_refs_set) == 0
            self._non_complete_object_refs = []

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

    def _process_values_and_waitables(
        self,
        values: List[Any],
        target_waitable_group: List[ObjectRef],
        non_complete_object_refs_set: Set[ObjectRef],
        target_waitable_group_num_consumers: Dict[ObjectRef, int],
    ) -> Optional[List[Any]]:
        """Process channel values and handle any task errors.

        Args:
            values: List of values to process
            target_waitable_group: List of object refs being processed
            non_complete_object_refs_set: Set of incomplete object refs
            target_waitable_group_num_consumers: Dict mapping object refs to
                number of consumers

        Returns:
            List[Any]: List of RayTaskError if error encountered, None otherwise
        """
        ctx = ChannelContext.get_current().serialization_context
        for i, value in enumerate(values):
            if target_waitable_group[i] in non_complete_object_refs_set:
                continue
            if isinstance(value, ray.exceptions.RayTaskError):
                self._non_complete_object_refs = list(non_complete_object_refs_set)
                for w in target_waitable_group:
                    ctx.reset_data(w)
                # If we raise an exception immediately, it will be considered
                # as a system error which will cause the execution loop to
                # exit. Hence, return immediately and let `_process_return_vals`
                # handle the exception.
                #
                # Return a list of RayTaskError so that the caller will not
                # get an undefined partial result.
                #
                # TODO(kevin85421): Should we consider reading channels that
                # are ready to be read and returning them as partial results
                # along with the RayTaskError?
                return [value for _ in range(len(self._input_channels))]
            ctx.set_data(
                target_waitable_group[i],
                value,
                target_waitable_group_num_consumers[target_waitable_group[i]],
            )
        return None

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
        timeout = 1e6 if timeout is None or timeout == -1 else timeout
        self._consume_non_complete_object_refs_if_needed(timeout)

        (
            waitables_to_num_consumers,
            skip_deserialization_waitables_to_num_consumers,
        ) = self._get_all_waitables_to_num_consumers()
        normal_waitables = list(waitables_to_num_consumers.keys())
        skip_deserialization_waitables = list(
            skip_deserialization_waitables_to_num_consumers.keys()
        )

        timeout_point = time.monotonic() + timeout
        worker = ray._private.worker.global_worker
        while len(normal_waitables) > 0 or len(skip_deserialization_waitables) > 0:
            # Retrieve at most one object each time.
            use_normal_waitables = len(normal_waitables) > 0
            target_waitable_group = (
                normal_waitables
                if use_normal_waitables
                else skip_deserialization_waitables
            )
            target_waitable_group_num_consumers = (
                waitables_to_num_consumers
                if use_normal_waitables
                else skip_deserialization_waitables_to_num_consumers
            )
            # Retrieve one mutable object reference at a time so that we can fail
            # early if one of the object's data is a `RayTaskError`.
            (
                values,
                non_complete_object_refs_set,
            ) = worker.experimental_wait_and_get_mutable_objects(
                target_waitable_group,
                num_returns=1,
                timeout_ms=max(0, (timeout_point - time.monotonic()) * 1000),
                return_exceptions=True,
                skip_deserialization=not use_normal_waitables,
                suppress_timeout_errors=True,
            )

            ray_task_errors = self._process_values_and_waitables(
                values,
                target_waitable_group,
                non_complete_object_refs_set,
                target_waitable_group_num_consumers,
            )
            if ray_task_errors is not None:
                return ray_task_errors

            target_waitable_group = list(non_complete_object_refs_set)
            if time.monotonic() > timeout_point and len(target_waitable_group) != 0:
                # This ensures that the reader attempts to retrieve
                # data once even when the `timeout` is 0.
                raise ray.exceptions.RayChannelTimeoutError(
                    "Timed out waiting for channel data."
                )
            if use_normal_waitables:
                normal_waitables = target_waitable_group
            else:
                skip_deserialization_waitables = target_waitable_group

        results = []
        for c in self._input_channels:
            start_time = time.monotonic()
            results.append(c.read(timeout))
            if timeout is not None:
                timeout -= time.monotonic() - start_time
                timeout = max(timeout, 0)
        return results

    def release_channel_buffers(self, timeout: Optional[float] = None) -> None:
        for c in self._input_channels:
            start_time = time.monotonic()
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
        # TODO(kevin85421): What's a good timeout for this?
        # Should we make it configurable?
        self._consume_non_complete_object_refs_if_needed(60)
        (
            waitables_to_num_consumers,
            skip_deserialization_waitables_to_num_consumers,
        ) = self._get_all_waitables_to_num_consumers()
        normal_waitables = list(waitables_to_num_consumers.keys())
        skip_deserialization_waitables = list(
            skip_deserialization_waitables_to_num_consumers.keys()
        )

        results = []

        worker = ray._private.worker.global_worker
        while len(normal_waitables) > 0 or len(skip_deserialization_waitables) > 0:
            use_normal_waitables = len(normal_waitables) > 0
            target_waitable_group = (
                normal_waitables
                if use_normal_waitables
                else skip_deserialization_waitables
            )
            target_waitable_group_num_consumers = (
                waitables_to_num_consumers
                if use_normal_waitables
                else skip_deserialization_waitables_to_num_consumers
            )

            (
                values,
                non_complete_object_refs_set,
            ) = worker.experimental_wait_and_get_mutable_objects(
                target_waitable_group,
                num_returns=1,
                timeout_ms=1000,
                return_exceptions=True,
                skip_deserialization=not use_normal_waitables,
                suppress_timeout_errors=True,
            )

            ray_task_errors = self._process_values_and_waitables(
                values,
                target_waitable_group,
                non_complete_object_refs_set,
                target_waitable_group_num_consumers,
            )
            if ray_task_errors is not None:
                return ray_task_errors

            target_waitable_group = list(non_complete_object_refs_set)
            if use_normal_waitables:
                normal_waitables = target_waitable_group
            else:
                skip_deserialization_waitables = target_waitable_group
            if sys.is_finalizing():
                return results

        for c in self._input_channels:
            exiting = retry_and_check_interpreter_exit(
                lambda: results.append(c.read(timeout=1))
            )
            if exiting:
                break

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
