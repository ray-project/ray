import concurrent.futures
import queue
import threading
from abc import ABC, abstractmethod
from typing import Callable, Dict, Optional, Tuple

from ray.anyscale.safetensors._private.lazy_torch import torch
from ray.anyscale.safetensors._private.logging_utils import logger
from ray.anyscale.safetensors._private.shm_download import ReadRangeOutput
from ray.anyscale.safetensors._private.util import TensorRanges


class BaseSink(ABC):
    """Writes data to a device asynchronously.


    This is a base class for writing from any source
    (network/disk) to any device (GPU/CPU/Disk).
    """

    @abstractmethod
    def start(self):
        ...

    @abstractmethod
    def process_output(self, item: ReadRangeOutput):
        ...

    @abstractmethod
    def shutdown(
        self, *, timeout_s: float, success: bool
    ) -> Optional[concurrent.futures.Future]:
        ...

    @abstractmethod
    def __enter__(self) -> "BaseSink":
        ...

    @abstractmethod
    def __exit__(self, exc_type, exc_value, traceback):
        ...


class BaseProcessingUnitSink(BaseSink):
    def __init__(
        self,
        source_buffer: memoryview,
        state_dict: Dict[str, "torch.Tensor"],
        urls_to_ranges_to_tensors: Dict[
            str, Dict[Tuple[int, int], Dict[str, TensorRanges]]
        ],
    ):
        self._source_tensor = torch.frombuffer(source_buffer, dtype=torch.uint8)

        # Flatten the state dict and view the tensors as uint8 (raw bytes).
        self._flat_state_dict_with_dtype = {
            k: (v.flatten().view(dtype=torch.uint8), v.dtype)
            for k, v in state_dict.items()
        }

        self._urls_to_ranges_to_tensors = urls_to_ranges_to_tensors

        self._work_queue: queue.SimpleQueue[
            Optional[ReadRangeOutput]
        ] = queue.SimpleQueue()
        self._exception_queue: queue.SimpleQueue[Exception] = queue.SimpleQueue()

    @staticmethod
    def _writer_thread(
        work_queue: queue.SimpleQueue[Optional[ReadRangeOutput]],
        exception_queue: queue.SimpleQueue[Exception],
        source_tensor: "torch.Tensor",
        state_dict_with_dtype: Dict[str, Tuple["torch.Tensor", "torch.dtype"]],
        urls_to_ranges_to_tensors: Dict[
            str, Dict[Tuple[int, int], Dict[str, TensorRanges]]
        ],
        copy_tensor_func: Callable,
        device_name: str,
    ):
        try:
            while True:
                item = work_queue.get(block=True)
                if item is None:
                    # Done, exit.
                    return

                range_to_tensors_map: Dict[
                    Tuple[int, int], Dict[str, TensorRanges]
                ] = urls_to_ranges_to_tensors[item.url]
                tensor_ranges_map: Dict[str, TensorRanges] = range_to_tensors_map[
                    (item.logical_start, item.logical_end)
                ]

                copy_tensor_func(
                    source_tensor[item.data_start : item.data_end],
                    tensor_ranges_map,
                    state_dict_with_dtype,
                )
        except Exception as e:
            logger.exception(f"Unexpected error when copying to {device_name} memory")
            exception_queue.put_nowait(e)

    @staticmethod
    def _create_writer_threads(
        work_queue: queue.SimpleQueue[Optional[ReadRangeOutput]],
        exception_queue: queue.SimpleQueue[Exception],
        source_tensor: "torch.Tensor",
        state_dict_with_dtype: Dict[str, Tuple["torch.Tensor", "torch.dtype"]],
        urls_to_ranges_to_tensors: Dict[
            str, Dict[Tuple[int, int], Dict[str, TensorRanges]]
        ],
        copy_tensor_func: Callable,
        device_name: str,
        num_threads: int,
    ):
        threads = []
        for _ in range(num_threads):
            threads.append(
                threading.Thread(
                    target=BaseProcessingUnitSink._writer_thread,
                    args=(
                        work_queue,
                        exception_queue,
                        source_tensor,
                        state_dict_with_dtype,
                        urls_to_ranges_to_tensors,
                        copy_tensor_func,
                        device_name,
                    ),
                )
            )
        return threads
