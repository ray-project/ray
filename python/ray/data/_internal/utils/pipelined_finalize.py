import threading
from typing import Any, Optional

import torch

from ray.data.collate_fn import is_tensor_batch_type


class PipelinedFinalizeFn:
    def __init__(self, device: str):
        self._device_str = device

        # State to lazily init. Transfer may not be necessary in some cases.
        self._device: Optional[torch.device] = None
        self._copy_stream: Optional[torch.cuda.Stream] = None
        self._init_lock = threading.Lock()

    def __call__(self, batch: Any) -> Any:
        if not is_tensor_batch_type(batch):
            return batch

        self._lazy_init()
        if not self._is_cuda():
            return batch.to(self._device)

        compute_stream = torch.cuda.current_stream(self.device)
        with torch.cuda.stream(self._copy_stream):
            # arbitrary use of .to(). Normally we will need modifications
            # because a batch may be a list of tensors, etc. See next
            # section for more details.
            moved = batch.to(self.device, non_blocking=True)
            moved.record_stream(compute_stream)

        copy_done = self._copy_stream.record_event()
        compute_stream.wait_event(copy_done)
        return moved

    def _is_cuda(self) -> None:
        assert self._device is not None
        return self._device.type == "cuda"

    def _lazy_init(self) -> None:
        if self._device is not None:
            return
        with self._init_lock:
            if self._device is None:
                self._device = torch.device(self._device_str)
                if self._is_cuda:
                    self._copy_stream = torch.cuda.Stream(self._device)
