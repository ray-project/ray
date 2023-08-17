from typing import Callable, Iterable, List, Tuple

from .ref_bundle import RefBundle
from .task_context import TaskContext
from ray.data._internal.stats import StatsDict
from ray.data.block import Block

# Block transform function applied in AllToAllOperator.
AllToAllTransformFn = Callable[
    [List[RefBundle], TaskContext], Tuple[List[RefBundle], StatsDict]
]
