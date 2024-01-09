from typing import Any

import numpy as np
import torch

import ray
from ray.experimental.channel import Channel


# Precondition: the communication group is setup for the reader and writer actors.
class TorchChannel(Channel):
    def __init__(self, buffer_size_bytes: int, reader_rank: int, writer_rank: int):
        self._buffer_size_bytes = buffer_size_bytes
        self._reader_rank = reader_rank
        self._writer_rank = writer_rank
        self._arr = torch.from_numpy(np.zeros(4 + buffer_size_bytes, dtype=np.uint8))
        self._worker = ray._private.worker.global_worker

    def __reduce__(self):
        return TorchChannel, (self._buffer_size_bytes, self._reader_rank, self._writer_rank)

    def write(self, value: Any) -> None:
        serialized_value = self._serialize(value)
        datalen = len(serialized_value)
        if datalen + 4 > len(self._arr):
            raise ValueError("Serialized value larger than the channel buffer length")
        arr = np.frombuffer(serialized_value, np.uint8)
        prefix = np.array([datalen], dtype=np.uint32).view(np.uint8)
        self._arr[:4] = torch.from_numpy(prefix)
        self._arr[4 : 4 + datalen] = torch.from_numpy(np.copy(arr))
        print("SEND", len(self._arr))
        torch.distributed.isend(self._arr, self._reader_rank).wait()

    def begin_read(self) -> Any:
        print("RECV", len(self._arr))
        torch.distributed.irecv(self._arr, self._writer_rank).wait()
        datalen = self._arr[:4].numpy().view(np.uint32)[0]
        return self._deserialize(self._arr[4 : 4 + datalen].numpy().tobytes())

    def end_read(self):
        pass

    def close(self) -> None:
        pass

    def _serialize(self, value: Any) -> bytes:
        ctx = self._worker.get_serialization_context()
        if not isinstance(value, bytes):
            raise NotImplementedError("only support bytes types only for now")
        return value

    def _deserialize(self, serialized_value: bytes) -> Any:
        return serialized_value
