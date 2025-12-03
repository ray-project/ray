from typing import TYPE_CHECKING

from ray.data._internal.execution.execution_callback import (
    ExecutionCallback,
)

if TYPE_CHECKING:
    from ray.data._internal.execution.streaming_executor import StreamingExecutor


class EpochIdxUpdateCallback(ExecutionCallback):
    def after_execution_succeeds(self, executor: "StreamingExecutor"):
        dataset_context = executor._data_context
        dataset_context._epoch_idx += 1
