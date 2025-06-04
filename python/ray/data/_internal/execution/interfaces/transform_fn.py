from typing import TYPE_CHECKING, Callable, List, Optional, Tuple

from .ref_bundle import RefBundle
from .task_context import TaskContext
from ray.data._internal.stats import StatsDict

if TYPE_CHECKING:

    from ray.data.block import Schema

# Block transform function applied in AllToAllOperator.
AllToAllTransformFn = Callable[
    [List[RefBundle], TaskContext],
    Tuple[List[RefBundle], StatsDict, "Schema"],
]

# Result type of AllToAllTransformFn.
AllToAllTransformFnResult = Tuple[List[RefBundle], StatsDict, Optional["Schema"]]
