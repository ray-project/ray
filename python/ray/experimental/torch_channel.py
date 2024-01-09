from typing import Any

import torch
import numpy as np

from ray.experimental.channel import Channel


# Precondition: the communication group is setup for the reader and writer actors.
class TorchChannel(Channel):
    def __init__(self, buffer_size_bytes: int, reader_rank: int, writer_rank: int):
        self._arr = np.zeros(4 + buffer_size_bytes, dtype=np.uint8)
        self._reader_rank = reader_rank
        self._writer_rank = writer_rank

    @staticmethod
    def _from_arr(arr: np.ndarray, reader_rank: int, writer_rank: int) -> "Channel":
        channel = Channel(1, reader_tank, writer_rank)
        channel._arr = arr
        return channel

    def __reduce__(self):
        return self._from_arr, (self._arr, self._reader_rank, self._writer_rank)

    def write(self, value: Any) -> None:
        serialized_value = self._serialize(value)
        datalen = len(serialized_value)
        if datalen + 4 > len(arr):
            raise ValueError("Serialized value larger than the channel buffer length")
        arr = np.frombuffer(serialized_value, np.uint8)
        prefix = np.array([datalen], dtype=np.uint32).view(np.uint8)
        self._arr[:4] = prefix
        self._arr[4 : 4 + datalen] = arr
        torch.distributed.isend(self._arr, self._reader_rank).wait()

    def begin_read(self) -> Any:
        torch.distributed.irecv(self._arr, self._writer_rank).wait()
        prefix = np.array([len(arr)], dtype=np.uint32).view(np.uint8)
        datalen = self._arr[:4].view(np.uint32)[0]
        return self._deserialize(self._arr[4 : 4 + datalen]).tobytes()

    def end_read(self):
        pass

    def close(self) -> None:
        pass

    def _deserialize(self, serialized_value: bytes) -> Any:
        raise NotImplementedError
