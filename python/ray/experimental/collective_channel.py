"""Experimental code for leveraging gloo as a channel backend.

This currently leverages gloo through torch distributed."""

import logging
import datetime
import pickle
from typing import Any, List, Callable

import numpy as np
from ray.exceptions import RayTaskError

import ray
from ray.experimental.channel import Channel
import ray.util.collective as col

logger = logging.getLogger(__name__)
DATALEN_PREFIX_SIZE = 4


# Precondition: the communication group is setup for the reader and writer actors.
class RayCollectiveChannel(Channel):
    def __init__(
        self,
        buffer_size_bytes: int,
        reader_ranks: List[int],
        writer_rank: int,
        # strategy: str = "isend",
    ):
        self._buffer_size_bytes = buffer_size_bytes
        self._reader_ranks = reader_ranks
        self._writer_rank = writer_rank
        # Reuse array instead of creating a new one
        # to reduce memory allocation overhead.
        self._arr = np.zeros(4 + buffer_size_bytes, dtype=np.uint8)
        self._worker = ray._private.worker.global_worker
        self._worker.check_connected()
        self.closed = False

    def __reduce__(self):
        return RayCollectiveChannel, (
            self._buffer_size_bytes,
            self._reader_ranks,
            self._writer_rank,
        )

    def write(self, value: Any, inform_close: bool = False) -> None:
        serialized_value = self._serialize(value)
        # TODO(sang): can we avoid sending the entire buffer each time?
        # TODO(sang): send/recv in pygloo creates a new buffer every time.

        datalen = len(serialized_value)
        if datalen + DATALEN_PREFIX_SIZE > len(self._arr):
            raise ValueError("Serialized value larger than the channel buffer length")
        arr = np.frombuffer(serialized_value, np.uint8)
        datalen_prefix = np.array([datalen], dtype=np.uint32).view(np.uint8)

        self._arr[0:DATALEN_PREFIX_SIZE] = datalen_prefix
        # TODO(sang): Can we avoid copy?
        self._arr[DATALEN_PREFIX_SIZE : DATALEN_PREFIX_SIZE + datalen] = np.copy(arr)

        # TODO(sang): Enable async send
        for rank in self._reader_ranks:
            logger.debug(f"send data to {rank}")
            # TODO(sang): If Connection closed by
            # error occurs here, it is highly likely an actor
            # is dead. Ping an actor and check if it is alive
            # when this exception is raised.
            col.send(self._arr, rank)

    def begin_read(self) -> Any:
        # TODO(sang): I think we can make sending an error bit
        # to a downstream channel as the default implementation
        # of all channels.

        logger.debug(f"recv data from {self._writer_rank}")

        data_received = False

        # Repetitively call recv API unless it is canceled.
        while not data_received:
            try:
                # TODO(sang): To support multiple DAG in the same
                # actor, we should specify a tag.
                col.recv(
                    self._arr,
                    self._writer_rank,
                    timeout_ms=5000
                )
            except RuntimeError as e:
                if self.closed and "Timed out waiting" in str(e):
                    raise RuntimeError("DAG execution cancelled") from None
            data_received = True

        datalen = self._arr[0:DATALEN_PREFIX_SIZE].view(np.uint32)[0]
        val = self._deserialize(
            self._arr[DATALEN_PREFIX_SIZE : DATALEN_PREFIX_SIZE + datalen].tobytes()) # noqa

        if isinstance(val, RayTaskError):
            raise val.as_instanceof_cause()
        else:
            return val

    def end_read(self):
        # Nothing to do for end_read.
        pass

    def close(self) -> None:
        self.closed = True

    def _serialize(self, value: Any, serialization_callback: Callable[[Any], bytes] = None) -> bytes:
        if serialization_callback is None:
            serialization_callback = pickle.dumps
        # TODO(sang): Support zero-copy.
        return serialization_callback(value)

    def _deserialize(self, serialized_value: bytes, deser_callback: Callable[[bytes], Any] = None) -> Any:
        if deser_callback is None:
            deser_callback = pickle.loads
        return pickle.loads(serialized_value)
