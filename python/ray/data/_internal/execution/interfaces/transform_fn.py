from typing import Callable, Iterable, List, Tuple

from ray.data._internal.execution.interfaces.ref_bundles import RefBundle
from ray.data._internal.execution.interfaces.task_context import TaskContext
from ray.data._internal.stats import StatsDict
from ray.data.block import Block

# Block transform function applied by task and actor pools in MapOperator.
MapTransformFn = Callable[[Iterable[Block], TaskContext], Iterable[Block]]

# Block transform function applied in AllToAllOperator.
AllToAllTransformFn = Callable[
    [List[RefBundle], TaskContext], Tuple[List[RefBundle], StatsDict]
]
