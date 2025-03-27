import threading
import time
from collections import deque
from contextlib import contextmanager
from typing import Any, Dict, Iterator, List, Optional, Tuple

from ray.anyscale.safetensors._private.base_sink import BaseProcessingUnitSink
from ray.anyscale.safetensors._private.env import (
    GPU_SINK_NUM_PINNED_BUFFERS,
    GPU_SINK_NUM_THREADS,
    PROCESS_SHUTDOWN_TIMEOUT_S,
)
from ray.anyscale.safetensors._private.lazy_torch import torch
from ray.anyscale.safetensors._private.logging_utils import logger
from ray.anyscale.safetensors._private.shm_download import ReadRangeOutput
from ray.anyscale.safetensors._private.util import (
    TensorRanges,
    preprocess_safetensor_tensor_key,
)


class PinnedBuffer:
    """Wrapper around a pinned memory buffer used to copy from CPU->GPU memory."""

    def __init__(self, buffer_size: int, *, unique_id: int):
        # NOTE(edoakes): unique_id must uniquely identify this buffer.
        self._unique_id = unique_id
        self._buffer = torch.empty(
            buffer_size, dtype=torch.uint8, device="cpu", pin_memory=True
        )
        self._event = torch.cuda.Event()
        self._stream = torch.cuda.Stream()

    def __eq__(self, other: Any) -> bool:
        return isinstance(other, PinnedBuffer) and self._unique_id == other._unique_id

    def query(self) -> bool:
        """Check if pending transfers are complete."""
        return self._event.query()

    def synchronize(self):
        """Wait until pending transfers are complete."""
        return self._event.synchronize()

    def copy_tensor_to_gpu_async(
        self,
        source_tensor: "torch.Tensor",
        tensor_ranges_map: Dict[str, TensorRanges],
        state_dict_with_dtype: Dict[str, Tuple["torch.Tensor", "torch.dtype"]],
    ):
        """Initiates an asynchronous copy of the source tensor into GPU memory.

        First synchronously copies the data into the pinned memory buffer, then
        initiates an asynchronous transfer to the GPU.

        The `query()` method can be used to determine when the transfer is complete.
        """
        # Copy the data from the source tensor to the pinned memory buffer.
        size = source_tensor.numel()
        cpu_buffer = self._buffer[:size]
        cpu_buffer.copy_(source_tensor)
        with torch.cuda.stream(self._stream):
            for tensor_name, r in tensor_ranges_map.items():
                gpu_tensor, dtype = state_dict_with_dtype.get(
                    preprocess_safetensor_tensor_key(tensor_name), (None, None)
                )
                if gpu_tensor is None:
                    continue

                tensor_range = r.range_in_tensor
                buffer_range = r.range_in_buffer
                assert dtype == r.dtype, f"Expected {dtype}, got {r.dtype}"

                # Source range to copy from the pinned memory buffer.
                cpu_buffer_slice = cpu_buffer[buffer_range[0] : buffer_range[1]]
                # Destination range to copy to GPU memory.
                gpu_tensor_slice = gpu_tensor[tensor_range[0] : tensor_range[1]]
                gpu_tensor_slice.copy_(cpu_buffer_slice, non_blocking=True)

        self._event.record(self._stream)


class PinnedBufferQueue:
    """Thread-safe queue of pinned memory buffers for CPU->GPU transfers.

    Memory must be pinned (un-pageable) in order for the GPU to access it.
    However, allocating pinned memory is significantly slower than regular memory,
    so we use a small set of pinned memory buffers and reuse them after transferring
    each chunk is complete.
    """

    WAIT_FOR_FREE_BUFFER_SLEEP_PERIOD_S = 0.0001
    WAIT_FOR_FREE_BUFFER_WARNING_THRESHOLD_S = 1

    def __init__(self, *, num_buffers: int, buffer_size: int):
        self._lock = threading.Lock()
        self._buffers = deque(
            PinnedBuffer(buffer_size, unique_id=i) for i in range(num_buffers)
        )

    def _pop_free_buffer_locked(self) -> Optional[PinnedBuffer]:
        """Pops a buffer from the queue if available and returns it.

        If all buffers have ongoing transfers, returns None.

        This method expects that the caller holds the lock.
        """
        free_buffer = None
        for buffer in self._buffers:
            if buffer.query():
                free_buffer = buffer

        if free_buffer is not None:
            self._buffers.remove(free_buffer)

        return free_buffer

    @contextmanager
    def pop_free_buffer(self) -> Iterator[PinnedBuffer]:
        """Pop a free buffer from the queue that can be used to copy from CPU->GPU.

        When the context manager is entered, the caller will have exclusive access to
        the buffer. The caller must use the CUDA event associated with the buffer to
        synchronize transfers.

        When the context manager is exited, the buffer will be returned to the queue.
        A transfer may still be in progress; the buffer won't be reused until the
        CUDA event indicates that the transfer is complete.

        TODO(edoakes): consider adding a timeout to this function.
        """
        free_buffer: Optional[PinnedBuffer] = None

        logged_warning = False
        start_time_s = time.time()
        sleep_period_s = self.WAIT_FOR_FREE_BUFFER_SLEEP_PERIOD_S
        warning_threshold_s = self.WAIT_FOR_FREE_BUFFER_WARNING_THRESHOLD_S
        with self._lock:
            while free_buffer is None:
                free_buffer = self._pop_free_buffer_locked()
                if free_buffer is None:
                    if (
                        not logged_warning
                        and time.time() - start_time_s > warning_threshold_s
                    ):
                        logger.warning(
                            f"Waited more than {warning_threshold_s}s "
                            "for a free pinned buffer."
                        )
                        logged_warning = True

                    time.sleep(sleep_period_s)

        assert free_buffer is not None

        try:
            yield free_buffer
        finally:
            # Push the buffer back onto the queue even in the case of error.
            # The buffer is placed at the end as it's least likely to be free
            # since a transfer was just initiated.
            with self._lock:
                self._buffers.append(free_buffer)

    def synchronize(self):
        """Returns once all ongoing transfers for buffers in the queue are complete."""
        with self._lock:
            for buffer in self._buffers:
                buffer.synchronize()


def create_gpu_copy_tensor_func(buffer_queue: PinnedBufferQueue):
    def copy_tensor_to_gpu(
        source_tensor: "torch.Tensor",
        tensor_ranges_map: Dict[str, TensorRanges],
        state_dict_with_dtype: Dict[str, Tuple["torch.Tensor", "torch.dtype"]],
    ):
        with buffer_queue.pop_free_buffer() as pinned_buffer:
            pinned_buffer.copy_tensor_to_gpu_async(
                source_tensor,
                tensor_ranges_map,
                state_dict_with_dtype,
            )

    return copy_tensor_to_gpu


class GPUSink(BaseProcessingUnitSink):
    """Copies ReadRangeOutputs to GPU memory.

    The copy operations are performed by a thread pool, so calls to
    `process_output` are fully asynchronous. `shutdown` will wait for
    the threads to finish all copies.

    To perform the copies, we use a set of small pinned memory buffers.
    Pinned memory is required to copy to the GPU, and if we naiively copy
    from shared memory, a new buffer will be allocated for each copy.
    Instead, we cache the buffers and reuse them.

    We use small buffers instead of a large chunk of pinned memory because
    allocating pinned memory is much slower than pageable memory.

    NOTE(edoakes): errors encountered in the GPU copy threads will be raised
    when `shutdown` is called.
    """

    # Number of threads that copy chunks from CPU -> GPU memory.
    NUM_THREADS = GPU_SINK_NUM_THREADS

    # Number of shared pinned buffers used for copies.
    # All buffers will be the size of the largest chunk.
    NUM_PINNED_BUFFERS = GPU_SINK_NUM_PINNED_BUFFERS

    def __init__(
        self,
        source_buffer: memoryview,
        state_dict: Dict[str, "torch.Tensor"],
        urls_to_ranges_to_tensors: Dict[
            str, Dict[Tuple[int, int], Dict[str, TensorRanges]]
        ],
        *,
        max_read_range_size: int,
    ):
        super().__init__(
            source_buffer,
            state_dict,
            urls_to_ranges_to_tensors,
        )
        self._buffer_queue = PinnedBufferQueue(
            num_buffers=self.NUM_PINNED_BUFFERS,
            buffer_size=max_read_range_size,
        )

        self._threads: List[threading.Thread] = self._create_writer_threads(
            self._work_queue,
            self._exception_queue,
            self._source_tensor,
            self._flat_state_dict_with_dtype,
            urls_to_ranges_to_tensors,
            create_gpu_copy_tensor_func(self._buffer_queue),
            "GPU",
            self.NUM_THREADS,
        )

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.shutdown(timeout_s=PROCESS_SHUTDOWN_TIMEOUT_S, success=exc_type is None)

    def start(self):
        """Start GPU copy threads.

        Should only be called once.
        """
        for t in self._threads:
            t.start()

    def process_output(self, output: ReadRangeOutput):
        """Enqueues the output to be copied to the GPU asynchronously."""
        self._work_queue.put_nowait(output)

    def shutdown(self, *, timeout_s: float, success: bool = True):
        """Wait for all work to be processed and worker threads to exit.

        If any worker threads failed, the underlying exception will be raised here.
        """

        gpu_flush_start_s = time.time()
        # TODO(edoakes): calculate a relative timeout here.
        for _ in range(self.NUM_THREADS):
            self._work_queue.put_nowait(None)

        start_time_s = time.time()
        for t in self._threads:
            elapsed_time_s = time.time() - start_time_s
            remaining_timeout_s = max(0, timeout_s - elapsed_time_s)
            t.join(remaining_timeout_s)
            # NOTE(edoakes): join() does not raise on timeout, so check `is_alive`.
            if t.is_alive():
                raise TimeoutError(
                    f"Timed out after {timeout_s}s waiting "
                    "for GPU copy worker threads to exit."
                )

        # Handle errors from the GPU writer threads.
        if not self._exception_queue.empty():
            e = self._exception_queue.get_nowait()
            raise RuntimeError("Failed to copy to GPU memory") from e

        # TODO(edoakes): torch.cuda.Event.synchronize() does not support a timeout,
        # so we need to wrap this in an executor thread.
        self._buffer_queue.synchronize()

        gpu_flush_time_s = time.time() - gpu_flush_start_s
        logger.debug(
            "Waiting for CPU->GPU copies to complete took %.4fs", gpu_flush_time_s
        )
        if gpu_flush_time_s > 0.1:
            logger.warning(
                f"Waiting for GPU copies to complete took {gpu_flush_time_s:.3f}s. "
                "You may need to increase ANYTENSOR_GPU_SINK_NUM_THREADS and/or "
                "ANYTENSOR_GPU_SINK_NUM_PINNED_BUFFERS."
            )
