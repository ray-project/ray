import logging
from typing import Any, List

import numpy as np
import torch

import ray
from ray.experimental.channel import Channel

logger = logging.getLogger(__name__)


# Precondition: the communication group is setup for the reader and writer actors.
class TorchChannel(Channel):
    def __init__(
        self,
        buffer_size_bytes: int,
        reader_ranks: List[int],
        writer_rank: int,
        strategy: str = "isend",
    ):
        self._buffer_size_bytes = buffer_size_bytes
        self._reader_ranks = reader_ranks
        self._writer_rank = writer_rank
        self._arr = torch.from_numpy(np.zeros(4 + buffer_size_bytes, dtype=np.uint8))
        self._worker = ray._private.worker.global_worker
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

    def write(self, value: Any) -> None:
        serialized_value = self._serialize(value)
        # TODO(ekl): can we avoid sending the entire buffer each time?
        datalen = len(serialized_value)
        if datalen + 4 > len(self._arr):
            raise ValueError("Serialized value larger than the channel buffer length")
        arr = np.frombuffer(serialized_value, np.uint8)
        prefix = np.array([datalen], dtype=np.uint32).view(np.uint8)
        self._arr[:4] = torch.from_numpy(prefix)
        self._arr[4 : 4 + datalen] = torch.from_numpy(np.copy(arr))
        work = []
        if self._strategy == "broadcast":
            work.append(
                torch.distributed.broadcast(
                    self._arr, src=self._writer_rank, async_op=True
                )
            )
        else:
            for rank in self._reader_ranks:
                work.append(torch.distributed.isend(self._arr, rank))
        [w.wait() for w in work]

    def begin_read(self) -> Any:
        if self._strategy == "broadcast":
            torch.distributed.broadcast(self._arr, src=self._writer_rank)
        else:
            torch.distributed.irecv(self._arr, self._writer_rank).wait()
        datalen = self._arr[:4].numpy().view(np.uint32)[0]
        return self._deserialize(self._arr[4 : 4 + datalen].numpy().tobytes())

    def end_read(self):
        pass

    def close(self) -> None:
        pass

    def _serialize(self, value: Any) -> bytes:
        if not isinstance(value, bytes):
            raise NotImplementedError("only supports bytes types only for now")
        return value

    def _deserialize(self, serialized_value: bytes) -> Any:
        return serialized_value
