import io
import logging
from typing import Any, List, Optional, Union

import ray
from ray.experimental.channel.common import ChannelInterface, ChannelOutputType
from ray.util.annotations import PublicAPI

# Logger for this module. It should be configured at the entry point
# into the program using Ray. Ray provides a default configuration at
# entry/init points.
logger = logging.getLogger(__name__)

DEFAULT_MAX_BUFFER_SIZE = int(100 * 1e6)  # 100MB


def _get_node_id(self) -> "ray.NodeID":
    return ray.get_runtime_context().get_node_id()


def _get_reader_node_id(self, reader_actor: "ray.actor.ActorHandle") -> "ray.NodeID":
    """
    Get the node ID of the reader actor.
    If the reader actor is the same as the current actor, make a local method call
    to get the node ID. Otherwise, make a remote ray.get() call to get the node ID.
    """
    current_actor_id = ray.get_runtime_context().get_actor_id()
    if current_actor_id is None:
        # We are calling from the driver, make a remote call
        fn = reader_actor.__ray_call__
        return ray.get(fn.remote(_get_node_id))

    current_actor = ray.get_runtime_context().current_actor
    if reader_actor == current_actor:
        return _get_node_id(self)
    else:
        fn = reader_actor.__ray_call__
        return ray.get(fn.remote(_get_node_id))


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
        readers: List[Optional["ray.actor.ActorHandle"]],
    ) -> "Channel":
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
        return Channel(writer, readers)


@PublicAPI(stability="alpha")
class Channel(ChannelInterface):
    """
    A wrapper type for ray.ObjectRef. Currently supports ray.get but not
    ray.wait.
    """

    def __init__(
        self,
        writer: Optional[ray.actor.ActorHandle],
        readers: List[Optional[ray.actor.ActorHandle]],
        typ: Optional[Union[int, SharedMemoryType]] = None,
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
            writer: The actor that may write to the channel. None signifies the driver.
            readers: The actors that may read from the channel. None signifies
                the driver.
            typ: Type information about the values passed through the channel.
                Either an integer representing the max buffer size in bytes
                allowed, or a SharedMemoryType.
        Returns:
            Channel: A wrapper around ray.ObjectRef.
        """
        is_creator = False
        assert len(readers) > 0

        if typ is None:
            typ = SharedMemoryType(DEFAULT_MAX_BUFFER_SIZE)
        elif isinstance(typ, int):
            typ = SharedMemoryType(typ)
        elif not isinstance(typ, SharedMemoryType):
            raise ValueError(
                "`typ` must be an `int` representing the max buffer size in "
                "bytes or a SharedMemoryType"
            )

        if _writer_ref is None:
            # We are the writer. Check that the passed handle matches the
            # current actor (or it is the driver).
            # TODO(swang): Channels must be initially constructed by the writer
            # actor, so we shouldn't need to include `writer` in the
            # constructor args. Either support Channels being constructed by
            # someone other than the writer or remove it from the args.
            self_actor = None
            try:
                self_actor = ray.get_runtime_context().current_actor
            except RuntimeError:
                # This is the driver so there is no current actor handle.
                pass
            assert writer == self_actor

            self._writer_node_id = (
                ray.runtime_context.get_runtime_context().get_node_id()
            )
            self._writer_ref = _create_channel_ref(self, typ.buffer_size_bytes)

            if readers[0] is None:
                # Reader is the driver. We assume that the reader and the writer are on
                # the same node.
                self._reader_node_id = self._writer_node_id
                self._reader_ref = self._writer_ref
            else:
                # Reader and writer are on different nodes.
                self._reader_node_id = _get_reader_node_id(self, readers[0])
                for reader in readers:
                    reader_node_id = _get_reader_node_id(self, reader)
                    if reader_node_id != self._reader_node_id:
                        raise NotImplementedError(
                            "All readers must be on the same node for now."
                        )
                if self.is_remote():
                    fn = readers[0].__ray_call__
                    self._reader_ref = ray.get(
                        fn.remote(_create_channel_ref, typ.buffer_size_bytes)
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

        self._writer = writer
        self._readers = readers
        self._typ = typ
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
        readers: List[Optional[ray.actor.ActorHandle]],
        typ: int,
        writer_node_id,
        reader_node_id,
        writer_ref: "ray.ObjectRef",
        reader_ref: "ray.ObjectRef",
    ) -> "Channel":
        chan = Channel(
            writer,
            readers,
            typ,
            _writer_node_id=writer_node_id,
            _reader_node_id=reader_node_id,
            _writer_ref=writer_ref,
            _reader_ref=reader_ref,
        )
        return chan

    def __reduce__(self):
        assert self._reader_ref is not None
        return self._deserialize_reader_channel, (
            self._writer,
            self._readers,
            self._typ,
            self._writer_node_id,
            self._reader_node_id,
            self._writer_ref,
            self._reader_ref,
        )

    def write(self, value: Any):
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
        self.ensure_registered_as_reader()
        return ray.get(self._reader_ref)

    def end_read(self):
        self.ensure_registered_as_reader()
        self._worker.core_worker.experimental_channel_read_release([self._reader_ref])

    def close(self) -> None:
        """
        Close this channel by setting the error bit on both the writer_ref and the
        reader_ref.
        """
        self._worker.core_worker.experimental_channel_set_error(self._writer_ref)
        if self.is_local_node(self._reader_node_id):
            self.ensure_registered_as_reader()
        self._worker.core_worker.experimental_channel_set_error(self._reader_ref)
