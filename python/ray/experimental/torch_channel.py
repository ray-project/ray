"""Experimental code for leveraging gloo as a channel backend.

This currently leverages gloo through torch distributed."""

import logging
import sys
import time
from typing import Any, List

import numpy as np

import ray
from ray.experimental.channel import Channel

logger = logging.getLogger(__name__)

_group = None


# Precondition: the communication group is setup for the reader and writer actors.
class TorchChannel(Channel):
    def __init__(
        self,
        buffer_size_bytes: int,
        reader_ranks: List[int],
        writer_rank: int,
        strategy: str = "isend",
    ):
        # Delayed import to avoid hard torch dependency.
        import torch

        self._torch = torch

        self._buffer_size_bytes = buffer_size_bytes
        self._reader_ranks = reader_ranks
        self._writer_rank = writer_rank
        # Reuse array instead of creating a new one
        # to reduce memory allocation overhead.
        self._arr = self._torch.from_numpy(
            np.zeros(4 + buffer_size_bytes, dtype=np.uint8)
        )
        self._worker = ray._private.worker.global_worker
        self._worker.check_connected()
        self._strategy = strategy
        assert strategy in ["broadcast", "isend"]
        logger.info(
            f"Created Torch channel with buffer_size_bytes={buffer_size_bytes}, "
            f"reader_ranks={reader_ranks}, writer_rank={writer_rank}, "
            f"strategy={strategy}"
        )

    def __reduce__(self):
        return TorchChannel, (
            self._buffer_size_bytes,
            self._reader_ranks,
            self._writer_rank,
            self._strategy,
        )

    @property
    def group(self):
        global _group
        if _group is None:
            assert self._torch.distributed.is_initialized()
            _group = self._torch.distributed.new_group(backend="gloo")
        return _group

    def write(self, value: Any) -> None:
        serialized_value = self._serialize(value)
        # TODO(ekl): can we avoid sending the entire buffer each time?
        datalen = len(serialized_value)
        if datalen + 4 > len(self._arr):
            raise ValueError("Serialized value larger than the channel buffer length")
        arr = np.frombuffer(serialized_value, np.uint8)
        # Prefix contains the length of the buffer.
        prefix = np.array([datalen], dtype=np.uint32).view(np.uint8)
        self._arr[:4] = self._torch.from_numpy(prefix)
        self._arr[4 : 4 + datalen] = self._torch.from_numpy(np.copy(arr))
        work = []
        if self._strategy == "broadcast":
            work.append(
                self._torch.distributed.broadcast(
                    self._arr, src=self._writer_rank, async_op=True, group=self.group
                )
            )
        else:
            for rank in self._reader_ranks:
                work.append(
                    self._torch.distributed.isend(self._arr, rank, group=self.group)
                )
        [w.wait() for w in work]

    def begin_read(self) -> Any:
        if self._strategy == "broadcast":
            self._torch.distributed.broadcast(
                self._arr, src=self._writer_rank, group=self.group
            )
        else:
            self._torch.distributed.irecv(
                self._arr, self._writer_rank, group=self.group
            ).wait()
        datalen = self._arr[:4].numpy().view(np.uint32)[0]
        return self._deserialize(self._arr[4 : 4 + datalen].numpy().tobytes())

    def begin_read_async(self) -> Any:
        if self._strategy == "broadcast":
            future = self._torch.distributed.broadcast(
                self._arr, src=self._writer_rank, group=self.group, async_op=True
            )
        else:
            future = self._torch.distributed.irecv(
                self._arr, self._writer_rank, group=self.group
            )
        return future

    def end_read(self):
        pass

    def close(self) -> None:
        # TODO(ekl): support proper shutdown.
        sys.exit(1)

    def _serialize(self, value: Any) -> bytes:
        if not isinstance(value, bytes):
            raise NotImplementedError("only supports bytes types only for now")
        return value

    def _deserialize(self, serialized_value: bytes) -> Any:
        return serialized_value


def batch_wait(channels):
    futures = []
    for chan in channels:
        # s = time.time()
        futures.append(chan.begin_read_async())
        # print("futures generatoion takes,", (time.time() - s) * 1000 * 1000)
    # s = time.time()
    for future in futures:
        future.wait()
    # print("wait takes,", (time.time() - s) * 1000 * 1000)
    # s = time.time()
    for chan in channels:
        datalen = chan._arr[:4].numpy().view(np.uint32)[0]
        return chan._deserialize(chan._arr[4 : 4 + datalen].numpy().tobytes())
    # print("deser takes,", (time.time() - s) * 1000 * 1000)
