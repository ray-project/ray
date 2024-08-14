import io
import logging
import time
from typing import Any, Dict, List, Optional, Set, Tuple, Union

import ray
import ray.exceptions
from ray._raylet import SerializedObject
from ray.experimental.channel.common import ChannelInterface, ChannelOutputType
from ray.experimental.channel.intra_process_channel import IntraProcessChannel
from ray.experimental.channel.torch_tensor_type import TorchTensorType
from ray.util.annotations import PublicAPI

# Logger for this module. It should be configured at the entry point
# into the program using Ray. Ray provides a default configuration at
# entry/init points.
logger = logging.getLogger(__name__)

DEFAULT_MAX_BUFFER_SIZE = int(100 * 1e6)  # 100 mB
# The min buffer size must be large enough to at least fit an instance of the
# _ResizeChannel class along with any metadata.
MIN_BUFFER_SIZE = int(1000)  # 1000 bytes


def _create_channel_ref(
    self,
    buffer_size_bytes: int,
) -> "ray.ObjectRef":
    """
    Create a channel that can be read and written through Ray's shared-memory
    object store.

    The channel has no buffer, so the writer will block until reader(s) have
    read the previous value.

    A writer and colocated readers can communicate via a shared memory buffer.
    If the readers are remote, then RPC is used to synchronize the writer and
    readers' buffers.

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


def _get_self_actor() -> Optional["ray.actor.ActorHandle"]:
    """
    Get the current actor handle in this worker.
    If this is called in a driver process, it will return None.
    """
    try:
        return ray.get_runtime_context().current_actor
    except RuntimeError:
        return None


class _ResizeChannel:
    """
    When a channel must be resized, the channel backing store must be resized on both
    the writer and the reader nodes. The writer first resizes its own backing store. The
    writer then uses an instance of this class as a sentinel value to tell the reader to
    resize its own backing store. The class instance is sent through the channel.
    """

    def __init__(self, reader_ref: "ray.ObjectRef"):
        self._reader_ref = reader_ref


class SharedMemoryType(ChannelOutputType):
    def __init__(self, buffer_size_bytes: int):
        """
        Args:
            buffer_size_bytes: The number of bytes to allocate for the object data and
                metadata. Writes to the channel must produce serialized data and
                metadata less than or equal to this value.
        """
        super().__init__()
        self.buffer_size_bytes = buffer_size_bytes

    def create_channel(
        self,
        writer: Optional["ray.actor.ActorHandle"],
        reader_and_node_list: List[Tuple["ray.actor.ActorHandle", str]],
    ) -> "Channel":
        """
        Instantiate a ChannelInterface class that can be used
        to pass data of this type.

        Args:
            writer: The actor that may write to the channel. None signifies the driver.
            reader_and_node_list: A list of tuples, where each tuple contains a reader
                actor handle and the node ID where the actor is located.
        Returns:
            A ChannelInterface that can be used to pass data
                of this type.
        """
        if self._contains_type is not None:
            assert isinstance(
                self._contains_type, TorchTensorType
            ), "_contains_type must be of type TorchTensorType"

            from ray.experimental.channel.torch_tensor_nccl_channel import (
                NestedTorchTensorNcclChannel,
            )

            if self._contains_type.requires_nccl():
                cpu_data_typ = SharedMemoryType(
                    buffer_size_bytes=self.buffer_size_bytes
                )
                return NestedTorchTensorNcclChannel(
                    writer,
                    reader_and_node_list,
                    gpu_data_typ=self._contains_type,
                    cpu_data_typ=cpu_data_typ,
                )

        return CompositeChannel(writer, reader_and_node_list)

    def set_nccl_group_id(self, group_id: str) -> None:
        assert self.requires_nccl()

        # Shared memory channels don't need NCCL but they can
        # contain objects that use NCCL.
        self._contains_type.set_nccl_group_id(group_id)


@PublicAPI(stability="alpha")
class Channel(ChannelInterface):
    """
    A wrapper type for ray.ObjectRef. Currently supports ray.get but not
    ray.wait.
    """

    def __init__(
        self,
        writer: Optional[ray.actor.ActorHandle],
        reader_and_node_list: List[Tuple["ray.actor.ActorHandle", str]],
        typ: Optional[Union[int, SharedMemoryType]] = None,
        _writer_node_id: Optional["ray.NodeID"] = None,
        _reader_node_id: Optional["ray.NodeID"] = None,
        _writer_ref: Optional["ray.ObjectRef"] = None,
        _reader_ref: Optional["ray.ObjectRef"] = None,
        _writer_registered: bool = False,
        _reader_registered: bool = False,
    ):
        """
        Create a channel that can be read and written by co-located Ray processes.

        Anyone may write to or read from the channel. The channel has no
        buffer, so the writer will block until reader(s) have read the previous
        value.

        Args:
            writer: The actor that may write to the channel. None signifies the driver.
            reader_and_node_list: A list of tuples, where each tuple contains a reader
                actor handle and the node ID where the actor is located.
            typ: Type information about the values passed through the channel.
                Either an integer representing the max buffer size in bytes
                allowed, or a SharedMemoryType.
        Returns:
            Channel: A wrapper around ray.ObjectRef.
        """
        assert len(reader_and_node_list) > 0
        for reader, _ in reader_and_node_list:
            assert isinstance(reader, ray.actor.ActorHandle)

        if typ is None:
            typ = SharedMemoryType(DEFAULT_MAX_BUFFER_SIZE)
        elif isinstance(typ, int):
            typ = SharedMemoryType(typ)

        if typ.buffer_size_bytes < MIN_BUFFER_SIZE:
            raise ValueError(
                "typ.buffer_size_bytes must be at least MIN_BUFFER_SIZE "
                f"({MIN_BUFFER_SIZE} bytes)"
            )

        self._writer = writer
        self._reader_and_node_list = reader_and_node_list
        self._typ = typ

        self._worker = ray._private.worker.global_worker
        self._worker.check_connected()

        self._writer_registered = _writer_registered
        self._reader_registered = _reader_registered

        if _writer_ref is None:
            # We are the writer. Check that the passed handle matches the
            # current actor (or it is the driver).
            # TODO(swang): Channels must be initially constructed by the writer
            # actor, so we shouldn't need to include `writer` in the
            # constructor args. Either support Channels being constructed by
            # someone other than the writer or remove it from the args.
            self_actor = _get_self_actor()
            assert writer == self_actor

            # For now, all readers must be on the same node. Note that the writer can
            # still be on a different node than the readers though.
            #
            # Note that we only check this when the writer is creating the channel.
            # Ideally, when each reader constructs its own instance of the channel, it
            # would check this as well. However, this could result in deadlock as two
            # readers attempt to execute a remote function on each other to get each
            # other's node ID. We cannot use a separate concurrency group to execute the
            # function because reader actors may not have been declared with an
            # additional concurrency group beyond default.
            #
            # TODO(jhumphri): Allow different readers for the same channel to be on
            # different nodes.
            prev_reader_node = None
            prev_reader = None
            for reader, node in reader_and_node_list:
                if prev_reader_node is None:
                    prev_reader_node = node
                elif prev_reader_node != node:
                    raise ValueError(
                        f"All reader actors must be on the same node. Actor "
                        f"{prev_reader} is on node {prev_reader_node} while actor "
                        f"{reader} is on node {node}."
                    )
                prev_reader = reader

            self._writer_node_id = (
                ray.runtime_context.get_runtime_context().get_node_id()
            )
            self._writer_ref = _create_channel_ref(self, typ.buffer_size_bytes)
            self._reader_node_id = prev_reader_node

            self._create_reader_ref(reader_and_node_list, typ.buffer_size_bytes)

            assert self._reader_ref is not None
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

        self._num_readers = len(self._reader_and_node_list)
        if self.is_remote():
            # Even though there may be multiple readers on a remote node, we set
            # `self._num_readers` to 1 here. On this local node, only the IO thread in
            # the mutable object provider will read the mutable object. The IO thread
            # will then send a gRPC with the mutable object contents to the remote node
            # where the readers are.
            self._num_readers = 1

    def _create_reader_ref(
        self,
        reader_and_node_list: List[Tuple["ray.actor.ActorHandle", str]],
        buffer_size_bytes: int,
    ):
        # TODO(jhumphri): Free the current reader ref once the reference to it is
        # destroyed below.
        reader = reader_and_node_list[0][0]
        if self.is_remote():
            fn = reader.__ray_call__
            self._reader_ref = ray.get(
                fn.remote(_create_channel_ref, buffer_size_bytes)
            )
        else:
            self._reader_ref = self._writer_ref

        # We need to register the new writer_ref.
        self._writer_registered = False
        self.ensure_registered_as_writer()

    @staticmethod
    def is_local_node(node_id):
        return ray.runtime_context.get_runtime_context().get_node_id() == node_id

    def is_remote(self):
        return self._writer_node_id != self._reader_node_id

    def ensure_registered_as_writer(self) -> None:
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
        self._worker.core_worker.experimental_channel_register_writer(
            self._writer_ref,
            self._reader_ref,
            self._writer_node_id,
            self._reader_node_id,
            self._reader_and_node_list[0][0]._actor_id,
            len(self._reader_and_node_list),
        )
        self._writer_registered = True

    def ensure_registered_as_reader(self) -> None:
        if self._reader_registered:
            return

        self._worker.core_worker.experimental_channel_register_reader(
            self._reader_ref,
        )
        self._reader_registered = True

    @staticmethod
    def _deserialize_reader_channel(
        writer: ray.actor.ActorHandle,
        reader_and_node_list: List[Tuple["ray.actor.ActorHandle", str]],
        typ: int,
        writer_node_id,
        reader_node_id,
        writer_ref: "ray.ObjectRef",
        reader_ref: "ray.ObjectRef",
        writer_registered: bool,
        reader_registered: bool,
    ) -> "Channel":
        chan = Channel(
            writer,
            reader_and_node_list,
            typ,
            _writer_node_id=writer_node_id,
            _reader_node_id=reader_node_id,
            _writer_ref=writer_ref,
            _reader_ref=reader_ref,
            _writer_registered=writer_registered,
            _reader_registered=reader_registered,
        )
        return chan

    def __reduce__(self):
        assert self._reader_ref is not None
        return self._deserialize_reader_channel, (
            self._writer,
            self._reader_and_node_list,
            self._typ,
            self._writer_node_id,
            self._reader_node_id,
            self._writer_ref,
            self._reader_ref,
            self._writer_registered,
            self._reader_registered,
        )

    def __str__(self) -> str:
        return (
            f"Channel(_reader_ref={self._reader_ref}, _writer_ref={self._writer_ref})"
        )

    def _resize_channel_if_needed(self, serialized_value: str, timeout_ms: int):
        # serialized_value.total_bytes *only* includes the size of the data. It does not
        # include the size of the metadata, so we must account for the size of the
        # metadata explicitly.
        size = serialized_value.total_bytes + len(serialized_value.metadata)
        if size > self._typ.buffer_size_bytes:
            # Now make the channel backing store larger.
            self._typ.buffer_size_bytes = size
            # TODO(jhumphri): Free the current writer ref once the reference to it is
            # destroyed below.
            prev_writer_ref = self._writer_ref
            self._writer_ref = _create_channel_ref(self, self._typ.buffer_size_bytes)

            self._create_reader_ref(
                self._reader_and_node_list, self._typ.buffer_size_bytes
            )

            # Write a special message to the channel so that the readers know to
            # stop using the current reader_ref.
            special_message = _ResizeChannel(self._reader_ref)
            special_message_serialized = (
                self._worker.get_serialization_context().serialize(special_message)
            )
            self._worker.core_worker.experimental_channel_put_serialized(
                special_message_serialized,
                prev_writer_ref,
                self._num_readers,
                timeout_ms,
            )

    def write(self, value: Any, timeout: Optional[float] = None) -> None:
        self.ensure_registered_as_writer()
        assert (
            timeout is None or timeout >= 0 or timeout == -1
        ), "Timeout must be non-negative or -1."
        # -1 means no timeout (block indefinitely)
        timeout_ms = int(timeout * 1000) if timeout is not None else -1

        if not isinstance(value, SerializedObject):
            try:
                serialized_value = self._worker.get_serialization_context().serialize(
                    value
                )
            except TypeError as e:
                sio = io.StringIO()
                ray.util.inspect_serializability(value, print_file=sio)
                msg = (
                    "Could not serialize the put value "
                    f"{repr(value)}:\n"
                    f"{sio.getvalue()}"
                )
                raise TypeError(msg) from e
        else:
            serialized_value = value

        start_time = time.monotonic()
        self._resize_channel_if_needed(serialized_value, timeout_ms)
        if timeout is not None:
            timeout_ms -= int((time.monotonic() - start_time) * 1000)
            timeout_ms = max(timeout_ms, 0)

        self._worker.core_worker.experimental_channel_put_serialized(
            serialized_value,
            self._writer_ref,
            self._num_readers,
            timeout_ms,
        )

    def read(self, timeout: Optional[float] = None) -> Any:
        assert (
            timeout is None or timeout >= 0 or timeout == -1
        ), "Timeout must be non-negative or -1."
        self.ensure_registered_as_reader()

        start_time = time.monotonic()
        ret = self._worker.get_objects(
            [self._reader_ref], timeout=timeout, return_exceptions=True
        )[0][0]

        if isinstance(ret, _ResizeChannel):
            self._reader_ref = ret._reader_ref
            # We need to register the new reader_ref.
            self._reader_registered = False
            self.ensure_registered_as_reader()
            if timeout is not None:
                timeout -= time.monotonic() - start_time
                timeout = max(timeout, 0)
            ret = self._worker.get_objects(
                [self._reader_ref], timeout=timeout, return_exceptions=True
            )[0][0]

        return ret

    def close(self) -> None:
        """
        Close this channel by setting the error bit on both the writer_ref and the
        reader_ref.
        """
        self._worker.core_worker.experimental_channel_set_error(self._writer_ref)
        if self.is_local_node(self._reader_node_id):
            self.ensure_registered_as_reader()
        self._worker.core_worker.experimental_channel_set_error(self._reader_ref)


@PublicAPI(stability="alpha")
class CompositeChannel(ChannelInterface):
    """
    Can be used to send data to different readers via different channels.
    For example, if the reader is in the same worker process as the writer,
    the data can be sent via IntraProcessChannel. If the reader is in a different
    worker process, the data can be sent via shared memory channel.

    Args:
        writer: The actor that may write to the channel. None signifies the driver.
        reader_and_node_list: A list of tuples, where each tuple contains a reader
            actor handle and the node ID where the actor is located.
    """

    def __init__(
        self,
        writer: Optional[ray.actor.ActorHandle],
        reader_and_node_list: List[Tuple["ray.actor.ActorHandle", str]],
        _channel_dict: Optional[Dict[ray.ActorID, ChannelInterface]] = None,
        _channels: Optional[Set[ChannelInterface]] = None,
        _writer_registered: bool = False,
        _reader_registered: bool = False,
    ):
        self._writer = writer
        self._reader_and_node_list = reader_and_node_list
        self._writer_registered = _writer_registered
        self._reader_registered = _reader_registered
        # A dictionary that maps the actor ID to the channel object.
        self._channel_dict = _channel_dict or {}
        # The set of channels is a deduplicated version of the _channel_dict values.
        self._channels = _channels or set()
        if self._channels:
            # This CompositeChannel object is created by deserialization.
            # We don't need to create channels again.
            return

        remote_reader_and_node_list: List[Tuple["ray.actor.ActorHandle", str]] = []
        for reader, node in self._reader_and_node_list:
            if self._writer != reader:
                remote_reader_and_node_list.append((reader, node))
        # There are some local readers which are the same worker process as the writer.
        # Create a local channel for the writer and the local readers.
        num_local_readers = len(self._reader_and_node_list) - len(
            remote_reader_and_node_list
        )
        if num_local_readers > 0:
            local_channel = IntraProcessChannel(num_local_readers)
            self._channels.add(local_channel)
            actor_id = self._get_actor_id(self._writer)
            self._channel_dict[actor_id] = local_channel
        # There are some remote readers which are not the same Ray actor as the writer.
        # Create a shared memory channel for the writer and the remote readers.
        if len(remote_reader_and_node_list) != 0:
            remote_channel = Channel(self._writer, remote_reader_and_node_list)
            self._channels.add(remote_channel)
            for reader, _ in remote_reader_and_node_list:
                actor_id = self._get_actor_id(reader)
                self._channel_dict[actor_id] = remote_channel

    def _get_actor_id(self, reader: ray.actor.ActorHandle) -> str:
        return reader._actor_id.hex()

    def _get_self_actor_id(self) -> str:
        """
        Get the actor ID of the current process. If the current process is the driver,
        use the actor ID of the DAGDriverProxyActor.
        """
        actor_id = ray.get_runtime_context().get_actor_id()
        if actor_id is None:
            # The reader is the driver process.
            # Use the actor ID of the DAGDriverProxyActor.
            assert len(self._reader_and_node_list) == 1
            driver_actor = self._reader_and_node_list[0][0]
            actor_id = self._get_actor_id(driver_actor)
        return actor_id

    def ensure_registered_as_writer(self) -> None:
        if self._writer_registered:
            return
        for channel in self._channels:
            channel.ensure_registered_as_writer()
        self._writer_registered = True

    def ensure_registered_as_reader(self) -> None:
        if self._reader_registered:
            return
        for channel in self._channels:
            channel.ensure_registered_as_reader()
        self._reader_registered = True

    def __reduce__(self):
        return CompositeChannel, (
            self._writer,
            self._reader_and_node_list,
            self._channel_dict,
            self._channels,
            self._writer_registered,
            self._reader_registered,
        )

    def __str__(self) -> str:
        return (
            "CompositeChannel(_channels="
            f"{[str(channel) for channel in self._channels]})"
        )

    def write(self, value: Any, timeout: Optional[float] = None) -> None:
        self.ensure_registered_as_writer()
        for channel in self._channels:
            channel.write(value, timeout)

    def read(self, timeout: Optional[float] = None) -> Any:
        self.ensure_registered_as_reader()
        actor_id = self._get_self_actor_id()
        return self._channel_dict[actor_id].read(timeout)

    def close(self) -> None:
        for channel in self._channels:
            channel.close()
