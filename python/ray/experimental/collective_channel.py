"""Experimental code for leveraging gloo as a channel backend.

This currently leverages gloo through torch distributed."""

import logging
import sys
from typing import Any, List
import time

import numpy as np

import ray
from ray.experimental.channel import Channel
import ray.util.collective as col

logger = logging.getLogger(__name__)


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
        # self._strategy = strategy
        # assert strategy in ["broadcast", "isend"]
        logger.info(
            f"Created Collective channel with buffer_size_bytes={buffer_size_bytes}, "
            f"reader_ranks={reader_ranks}, writer_rank={writer_rank}, "
            # f"strategy={strategy}"
        )

    def __reduce__(self):
        return RayCollectiveChannel, (
            self._buffer_size_bytes,
            self._reader_ranks,
            self._writer_rank,
        )

    def write(self, value: Any) -> None:
        serialized_value = self._serialize(value)
        # TODO(sang): can we avoid sending the entire buffer each time?
        # TODO(sang): send/recv in pygloo creates a new buffer every time.
        datalen = len(serialized_value)
        if datalen + 4 > len(self._arr):
            raise ValueError("Serialized value larger than the channel buffer length")
        arr = np.frombuffer(serialized_value, np.uint8)
        # Prefix contains the length of the buffer.
        prefix = np.array([datalen], dtype=np.uint32).view(np.uint8)
        self._arr[:4] = prefix
        self._arr[4 : 4 + datalen] = np.copy(arr)
        # TODO(sang): Enable async send
        # TODO(sang): Probably we should use broadcast as the short term solution.
        for rank in self._reader_ranks:
            logger.debug(f"SANG-TODO send it to {rank}")
            col.send(self._arr, rank)

    def begin_read(self) -> Any:
        logger.debug(f"SANG-TODO recv from {self._writer_rank}")
        col.recv(self._arr, self._writer_rank)
        datalen = self._arr[:4].view(np.uint32)[0]
        return self._deserialize(self._arr[4 : 4 + datalen].tobytes())

    def end_read(self):
        pass

    def close(self) -> None:
        # TODO(sang): support proper shutdown using an error bit.
        sys.exit(1)

    def _serialize(self, value: Any) -> bytes:
        if not isinstance(value, bytes):
            raise NotImplementedError("only supports bytes types only for now")
        return value

    def _deserialize(self, serialized_value: bytes) -> Any:
        return serialized_value
