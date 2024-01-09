from typing import Any

import numpy as np
import torch

import ray
from ray.experimental.channel import Channel


# Precondition: the communication group is setup for the reader and writer actors.
class TorchChannel(Channel):
    def __init__(self, buffer_size_bytes: int, reader_rank: int, writer_rank: int):
        self._arr = np.zeros(4 + buffer_size_bytes, dtype=np.uint8)
        self._reader_rank = reader_rank
        self._writer_rank = writer_rank
        self._worker = ray._private.worker.global_worker

    @staticmethod
    def _from_arr(arr: np.ndarray, reader_rank: int, writer_rank: int) -> "Channel":
        channel = Channel(1, reader_rank, writer_rank)
        channel._arr = arr
        return channel

    def __reduce__(self):
        return self._from_arr, (self._arr, self._reader_rank, self._writer_rank)

    def write(self, value: Any) -> None:
        serialized_value = self._serialize(value)
        datalen = len(serialized_value)
        if datalen + 4 > len(self._arr):
            raise ValueError("Serialized value larger than the channel buffer length")
        arr = np.frombuffer(serialized_value, np.uint8)
        prefix = np.array([datalen], dtype=np.uint32).view(np.uint8)
        self._arr[:4] = prefix
        self._arr[4 : 4 + datalen] = arr
        torch.distributed.isend(self._arr, self._reader_rank).wait()

    def begin_read(self) -> Any:
        torch.distributed.irecv(self._arr, self._writer_rank).wait()
        datalen = self._arr[:4].view(np.uint32)[0]
        return self._deserialize(self._arr[4 : 4 + datalen]).tobytes()

    def end_read(self):
        pass

    def close(self) -> None:
        pass

    def _serialize(self, value: Any) -> bytes:
        ctx = self._worker.get_serialization_context()
        raise NotImplementedError

    def _deserialize(self, serialized_value: bytes) -> Any:
        ctx = self._worker.get_serialization_context()
        raise NotImplementedError
