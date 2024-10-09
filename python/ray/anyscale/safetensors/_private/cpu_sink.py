import time
from typing import Dict, Tuple

from ray.anyscale.safetensors._private.base_sink import BaseProcessingUnitSink
from ray.anyscale.safetensors._private.env import (
    CPU_SINK_NUM_THREADS,
    PROCESS_SHUTDOWN_TIMEOUT_S,
)
from ray.anyscale.safetensors._private.lazy_torch import torch
from ray.anyscale.safetensors._private.shm_download import ReadRangeOutput
from ray.anyscale.safetensors._private.util import (
    TensorRanges,
    preprocess_safetensor_tensor_key,
)


def copy_tensor_to_cpu(
    source_tensor: "torch.Tensor",
    tensor_ranges_map: Dict[str, TensorRanges],
    state_dict_with_dtype: Dict[str, Tuple["torch.Tensor", "torch.dtype"]],
):
    for tensor_name, r in tensor_ranges_map.items():
        cpu_tensor, dtype = state_dict_with_dtype.get(
            preprocess_safetensor_tensor_key(tensor_name), (None, None)
        )

        if cpu_tensor is None:
            continue

        tensor_range = r.range_in_tensor
        buffer_range = r.range_in_buffer
        assert dtype == r.dtype, f"Expected {dtype} got {r.dtype}"

        cpu_tensor_slice = cpu_tensor[tensor_range[0] : tensor_range[1]]
        cpu_tensor_slice.copy_(source_tensor[buffer_range[0] : buffer_range[1]])


class CPUSink(BaseProcessingUnitSink):
    NUM_THREADS = CPU_SINK_NUM_THREADS

    def __init__(
        self,
        source_buffer: memoryview,
        state_dict: Dict[str, "torch.Tensor"],
        urls_to_ranges_to_tensors: Dict[
            str, Dict[Tuple[int, int], Dict[str, TensorRanges]]
        ],
    ):
        super().__init__(
            source_buffer,
            state_dict,
            urls_to_ranges_to_tensors,
        )

        self.threads = self._create_writer_threads(
            self._work_queue,
            self._exception_queue,
            self._source_tensor,
            self._flat_state_dict_with_dtype,
            self._urls_to_ranges_to_tensors,
            copy_tensor_to_cpu,
            "CPU",
            self.NUM_THREADS,
        )

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.shutdown(timeout_s=PROCESS_SHUTDOWN_TIMEOUT_S, success=exc_type is None)

    def start(self):
        for t in self.threads:
            t.start()

    def process_output(self, output: ReadRangeOutput):
        self._work_queue.put_nowait(output)

    def shutdown(self, *, timeout_s: float, success: bool = True):
        for _ in range(self.NUM_THREADS):
            self._work_queue.put_nowait(None)
        for t in self.threads:
            t.join()
        start_time_s = time.time()
        elapsed_time_s = time.time() - start_time_s
        remaining_timeout_s = max(0, timeout_s - elapsed_time_s)
        for t in self.threads:
            t.join(remaining_timeout_s)
            if t.is_alive():
                raise TimeoutError(
                    f"Timed out after {timeout_s}s waiting "
                    "for CPU copy worker threads to exit."
                )

        # Handle errors from the CPU writer threads.
        if not self._exception_queue.empty():
            e = self._exception_queue.get_nowait()
            raise RuntimeError("Failed to copy to CPU memory") from e
