import threading
from typing import Any, Optional, Union

import torch

from ray.data.collate_fn import is_tensor_batch_type
from ray.data.util.torch_utils import move_tensors_to_device


class PipelinedFinalizeFn:
    """finalize_fn that overlaps the host->device transfer with downstream GPU compute.

    The transfer reuses ``move_tensors_to_device`` (which handles the
    ``TensorBatchType`` shapes and the per-column chunk concatenation), but runs it
    inside a dedicated copy-stream context. Setting the current stream routes all of
    ``move_tensors_to_device``'s internal CUDA ops (``torch.empty``, ``.to``,
    ``copy_``) onto the copy stream, so the H2D copy overlaps the consumer's compute
    on the default stream. A CUDA event then orders the default stream after the
    copy, and ``record_stream`` keeps the caching allocator from recycling the output
    tensors while the compute stream is still reading them.

    Preconditions for real overlap (not enforced here):
    - The collated source tensors are pinned (e.g. ``DefaultCollateFn(pin_memory=True)``);
      otherwise the non-blocking H2D copy from pageable memory is effectively
      synchronous and nothing overlaps.
    - The consumer computes on the default stream (legacy global default stream), so
      the ``wait_event`` issued here actually applies to it.
    """

    def __init__(self, device: Union[str, "torch.device"]):
        self._device_arg = device

        # Lazily initialized: the transfer may not be needed (e.g. CPU device), and
        # the copy stream must be created on the finalize thread.
        self._device: Optional[torch.device] = None
        self._copy_stream: Optional[torch.cuda.Stream] = None
        self._init_lock = threading.Lock()

    @torch.no_grad()
    def __call__(self, batch: Any) -> Any:
        if not is_tensor_batch_type(batch):
            return batch

        self._lazy_init()

        # CPU target: no stream/event coordination needed. move_tensors_to_device
        # still handles the chunk concatenation and the shape dispatch.
        if not self._is_cuda():
            return move_tensors_to_device(batch, device=self._device)

        compute_stream = torch.cuda.current_stream(self._device)
        with torch.cuda.stream(self._copy_stream):
            moved = move_tensors_to_device(batch, device=self._device)

        # The outputs were allocated on the copy stream but will be read on the
        # compute stream; tell the allocator so it doesn't recycle them early.
        _record_stream(moved, compute_stream)

        # Order the compute stream after copy is fully complete.
        copy_done = self._copy_stream.record_event()
        compute_stream.wait_event(copy_done)
        return moved

    def _is_cuda(self) -> bool:
        assert self._device is not None
        return self._device.type == "cuda"

    def _lazy_init(self) -> None:
        if self._device is not None:
            return
        with self._init_lock:
            if self._device is None:
                device = torch.device(self._device_arg)
                if device.type == "cuda":
                    self._copy_stream = torch.cuda.Stream(device)
                self._device = device


def _record_stream(batch: Any, stream: "torch.cuda.Stream") -> None:
    """Recursively call ``record_stream(stream)`` on every tensor in ``batch``.

    ``move_tensors_to_device`` returns a tensor, a (list/tuple of) tensor(s), or a
    dict of tensors, so we recurse to cover all of them.
    """
    if isinstance(batch, torch.Tensor):
        batch.record_stream(stream)
    elif isinstance(batch, dict):
        for value in batch.values():
            _record_stream(value, stream)
    elif isinstance(batch, (list, tuple)):
        for value in batch:
            _record_stream(value, stream)
