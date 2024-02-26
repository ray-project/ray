"""Experimental code for leveraging gloo as a channel backend.

This currently leverages gloo through torch distributed."""

import datetime
import logging
import pickle
from typing import Any, Callable, List

import numpy as np

import ray
import ray.util.collective as col
from ray.exceptions import RayTaskError
from ray.experimental.channel import Channel

logger = logging.getLogger(__name__)
DATALEN_PREFIX_SIZE = 4


# Precondition: the communication group is setup for the reader and writer actors.
class RayCollectiveChannel(Channel):
    def __init__(
        self,
        buffer_size_bytes: int,
        reader_ranks: List[int],
        writer_rank: int,
        strategy: str,
    ):
        self._buffer_size_bytes = buffer_size_bytes
        self._reader_ranks = reader_ranks
        self._writer_rank = writer_rank
        self._arr = np.zeros(4 + buffer_size_bytes, dtype=np.uint8)
        self._worker = ray._private.worker.global_worker
        self._worker.check_connected()
        self.closed = False
        self._strategy = strategy

    def __reduce__(self):
        return RayCollectiveChannel, (
            self._buffer_size_bytes,
            self._reader_ranks,
            self._writer_rank,
            self._strategy,
        )

    def write(self, value: Any) -> None:
        serialized_value = self._serialize(value)
        datalen = len(serialized_value)
        if datalen + DATALEN_PREFIX_SIZE > self._buffer_size_bytes:
            raise ValueError("Serialized value larger than the channel buffer length")

        arr = np.frombuffer(serialized_value, np.uint8)
        datalen_prefix = np.array([datalen], dtype=np.uint32).view(np.uint8)
        send_arr = np.zeros(4 + datalen, dtype=np.uint8).view()

        send_arr[0:DATALEN_PREFIX_SIZE] = datalen_prefix
        # TODO(sang): Can we avoid copy?
        send_arr[DATALEN_PREFIX_SIZE : DATALEN_PREFIX_SIZE + datalen] = arr

        futures = []
        for rank in self._reader_ranks:
            logger.debug(f"send data to {rank}")
            # TODO(sang): If Connection closed by
            # error occurs here, it is highly likely an actor
            # is dead. Ping an actor and check if it is alive
            # when this exception is raised.
            if self._strategy == "isend":
                futures.append(col.send(send_arr, rank, async_op=True))
            else:
                col.send(send_arr, rank)

        if self._strategy == "isend":
            for fut in futures:
                fut.wait()

    def begin_read(self) -> Any:
        logger.debug(f"recv data from {self._writer_rank}")

        # Repetitively call recv API until it is canceled.
        data_received = False
        while not data_received:
            try:
                # TODO(sang): To support multiple DAG in the same
                # actor, we should specify a tag.
                col.recv(self._arr, self._writer_rank, timeout_ms=5000)
            except RuntimeError as e:
                if self.closed and "Timed out waiting" in str(e):
                    raise RuntimeError("DAG execution cancelled") from None
            data_received = True

        datalen = self._arr[0:DATALEN_PREFIX_SIZE].view(np.uint32)[0]
        val = self._deserialize(
            self._arr[DATALEN_PREFIX_SIZE : DATALEN_PREFIX_SIZE + datalen].data
        )  # noqa

        if isinstance(val, RayTaskError):
            raise val.as_instanceof_cause()
        else:
            return val

    def begin_read_async(self):
        return col.recv(self._arr, self._writer_rank, async_op=True)

    def end_read(self):
        # Nothing to do for end_read.
        pass

    def close(self) -> None:
        self.closed = True

    def _serialize(
        self, value: Any, serialization_callback: Callable[[Any], bytes] = None
    ) -> bytes:
        if serialization_callback is None:
            # NOTE: clodpickle supports more complex types but is slower.
            serialization_callback = pickle.dumps
        return serialization_callback(value)

    def _deserialize(
        self, serialized_value: bytes, deser_callback: Callable[[bytes], Any] = None
    ) -> Any:
        if deser_callback is None:
            deser_callback = pickle.loads
        return pickle.loads(serialized_value)


def batch_read(channels):
    result = []
    futures = []
    for channel in channels:
        futures.append(channel.begin_read_async())

    for fut, chan in zip(futures, channels):
        data_received = False
        while not data_received:
            try:
                fut = fut.wait(timeout=datetime.timedelta(seconds=5))
            except RuntimeError as e:
                if chan.closed and "Timed out waiting" in str(e):
                    raise RuntimeError("DAG execution cancelled") from None
            data_received = True

        datalen = chan._arr[0:DATALEN_PREFIX_SIZE].view(np.uint32)[0]
        val = chan._deserialize(
            chan._arr[DATALEN_PREFIX_SIZE : DATALEN_PREFIX_SIZE + datalen].data
        )  # noqa

        if isinstance(val, RayTaskError):
            raise val.as_instanceof_cause()
        else:
            result.append(val)
    return result
