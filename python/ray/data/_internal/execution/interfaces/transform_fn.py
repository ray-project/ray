from typing import Callable, List, Tuple

from ray.data._internal.stats import StatsDict

from .ref_bundle import RefBundle
from .task_context import TaskContext

# Block transform function applied in AllToAllOperator.
AllToAllTransformFn = Callable[
    [List[RefBundle], TaskContext], Tuple[List[RefBundle], StatsDict]
]
