from typing import TYPE_CHECKING, Callable, List, Tuple

from .ref_bundle import RefBundle
from .task_context import TaskContext
from ray.data._internal.stats import StatsDict

if TYPE_CHECKING:
    import pyarrow as pa

# Block transform function applied in AllToAllOperator.
AllToAllTransformFn = Callable[
    [List[RefBundle], TaskContext], Tuple[List[RefBundle], StatsDict, "pa.lib.Schema"]
]
