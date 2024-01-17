from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Dict, Optional

import ray._private.worker
from ray.data._internal.dataset_logger import DatasetLogger
from ray.data._internal.progress_bar import ProgressBar
from ray.data._internal.util import convert_bytes_to_human_readable_str

if TYPE_CHECKING:
    from ray.data._internal.execution.operators.map_transformer import MapTransformer

logger = DatasetLogger(__name__)


@dataclass
class TaskContext:
    """This describes the information of a task running block transform."""

    # The index of task. Each task has a unique task index within the same
    # operator.
    task_idx: int

    # If driver memory exceeds this threshold, warn the user. For now, this
    # only applies to shuffle ops because most other ops are unlikely to use as
    # much driver memory.
    warn_on_driver_memory_usage_bytes: Optional[int] = None

    # The dictionary of sub progress bar to update. The key is name of sub progress
    # bar. Note this is only used on driver side.
    # TODO(chengsu): clean it up from TaskContext with new optimizer framework.
    sub_progress_bar_dict: Optional[Dict[str, ProgressBar]] = None

    # NOTE(hchen): `upstream_map_transformer` and `upstream_map_ray_remote_args`
    # are only used for `RandomShuffle`. DO NOT use them for other operators.
    # Ideally, they should be handled by the optimizer, and should be transparent
    # to the specific operators.
    # But for `RandomShuffle`, the AllToAllOperator doesn't do the shuffle itself.
    # It uses `ExchangeTaskScheduler` to launch new tasks to do the shuffle.
    # That's why we need to pass them to `ExchangeTaskScheduler`.
    # TODO(hchen): Use a physical operator to do the shuffle directly.

    # The underlying function called in a MapOperator; this is used when fusing
    # an AllToAllOperator with an upstream MapOperator.
    upstream_map_transformer: Optional["MapTransformer"] = None

    # The Ray remote arguments of the fused upstream MapOperator.
    # This should be set if upstream_map_transformer is set.
    upstream_map_ray_remote_args: Optional[Dict[str, Any]] = None

    # The target maximum number of bytes to include in the task's output block.
    target_max_block_size: Optional[int] = None

    def warn_on_high_local_memory_store_usage(self) -> None:
        ray_core_worker = ray._private.worker.global_worker.core_worker
        local_memory_store_bytes_used = (
            ray_core_worker.get_local_memory_store_bytes_used()
        )
        self.warn_on_driver_memory_usage(
            local_memory_store_bytes_used,
            "More than "
            f"{convert_bytes_to_human_readable_str(local_memory_store_bytes_used)} "
            "of driver memory used to store Ray Data block data and metadata. "
            "This job may exit if driver memory is insufficient.\n\n"
            "This can happen when many tiny blocks are created. "
            "Check the block size using Dataset.stats() and see "
            "https://docs.ray.io/en/latest/data/performance-tips.html"
            "#TODO for mitigation.",
        )

    def warn_on_driver_memory_usage(
        self, memory_usage_bytes: int, log_str: str
    ) -> None:
        if self.warn_on_driver_memory_usage_bytes is None:
            return

        if memory_usage_bytes > self.warn_on_driver_memory_usage_bytes:
            logger.get_logger().warn(log_str)
            # Double the threshold to avoid verbose warnings.
            self.warn_on_driver_memory_usage_bytes = memory_usage_bytes * 2
