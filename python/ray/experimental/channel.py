import io
import logging
from typing import Any, Optional

import ray
from ray.exceptions import RaySystemError
from ray.util.annotations import PublicAPI

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
    read the previous value. Only the channel creator may write to the channel.

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
            value, owner_address=None, _is_experimental_mutable_object=True
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
        _base_ref: Optional["ray.ObjectRef"] = None,
    ):
        """
        Create a channel that can be read and written by co-located Ray processes.

        Only the caller may write to the channel. The channel has no buffer,
        so the writer will block until reader(s) have read the previous value.

        Args:
            buffer_size_bytes: The number of bytes to allocate for the object data and
                metadata. Writes to the channel must produce serialized data and
                metadata less than or equal to this value.
        Returns:
            Channel: A wrapper around ray.ObjectRef.
        """
        if buffer_size_bytes is None:
            if _base_ref is None:
                raise ValueError(
                    "One of `buffer_size_bytes` or `_base_ref` must be provided"
                )
            self._base_ref = _base_ref
        else:
            if not isinstance(buffer_size_bytes, int):
                raise ValueError("buffer_size_bytes must be an integer")
            self._base_ref = _create_channel_ref(buffer_size_bytes)

        if not isinstance(num_readers, int):
            raise ValueError("num_readers must be an integer")

        self._num_readers = num_readers
        self._worker = ray._private.worker.global_worker
        self._worker.check_connected()

    @staticmethod
    def _from_base_ref(base_ref: "ray.ObjectRef", num_readers: int) -> "Channel":
        return Channel(num_readers=num_readers, _base_ref=base_ref)

    def __reduce__(self):
        return self._from_base_ref, (self._base_ref, self._num_readers)

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

        self._worker.core_worker.experimental_mutable_object_put_serialized(
            serialized_value,
            self._base_ref,
            num_readers,
        )

    def begin_read(self) -> Any:
        """
        Read the latest value from the channel. This call will block until a
        value is available to read.

        Subsequent calls to begin_read() will return the same value, until
        end_read() is called. Then, the client must begin_read() again to get
        the next value.

        Returns:
            Any: The deserialized value.
        """
        values, _ = self._worker.get_objects(
            [self._base_ref], _is_experimental_mutable_object=True
        )
        return values[0]

    def end_read(self):
        """
        Signal to the writer that the channel is ready to write again.

        If begin_read is not called first, then this call will block until a
        value is written, then drop the value.
        """
        self._worker.core_worker.experimental_mutable_object_read_release(
            [self._base_ref]
        )

    def unblock_readers_with_error(self, e: Exception) -> None:
        """
        If readers are blocked on the channel, shut it down by writing an error
        object to the channel.

        Does not block.

        Args:
            e: The exception object to write to the channel.
        """
        logger.debug(f"Writing error to channel: {self._base_ref}: {e}")
        serialized_exc = self._worker.get_serialization_context().serialize(e)
        try:
            # Write an error if a reader is blocked. If a value is already available,
            # no need to write anything.
            self._worker.core_worker.experimental_mutable_object_put_serialized(
                serialized_exc,
                self._base_ref,
                num_readers=1,
                try_wait=True,
            )
        except Exception as e:
            # If we get a write acquire failed error, that's expected since it means
            # no reader is currently blocked for this channel.
            # Raise other types of errors encountered.
            if not _is_write_acquire_failed_error(e):
                logger.exception("Error setting error on channel")
                raise


def _is_write_acquire_failed_error(e: Exception) -> bool:
    # TODO(ekl): detect the exception type better
    return isinstance(e, RaySystemError)
