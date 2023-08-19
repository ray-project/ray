from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Dict, Optional

from ray.data._internal.progress_bar import ProgressBar

if TYPE_CHECKING:
    from ray.data._internal.execution.operators.map_transformer import MapTransformer


@dataclass
class TaskContext:
    """This describes the information of a task running block transform."""

    # The index of task. Each task has a unique task index within the same
    # operator.
    task_idx: int

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
