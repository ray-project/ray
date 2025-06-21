from typing import Callable, List, Tuple

from .ref_bundle import RefBundle
from .task_context import TaskContext
from ray.data._internal.stats import StatsDict

# Result type of AllToAllTransformFn.
AllToAllTransformFnResult = Tuple[List[RefBundle], StatsDict]

# Block transform function applied in AllToAllOperator.
AllToAllTransformFn = Callable[
    [List[RefBundle], TaskContext],
    AllToAllTransformFnResult,
]
