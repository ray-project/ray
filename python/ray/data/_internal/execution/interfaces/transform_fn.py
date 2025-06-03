from typing import TYPE_CHECKING, Callable, List, Tuple, Union

from .ref_bundle import RefBundle
from .task_context import TaskContext
from ray.data._internal.stats import StatsDict

if TYPE_CHECKING:
    import pyarrow as pa

    from ray.data.block import PandasBlockSchema

# Block transform function applied in AllToAllOperator.
AllToAllTransformFn = Callable[
    [List[RefBundle], TaskContext],
    Tuple[
        List[RefBundle], StatsDict, Union[type, "PandasBlockSchema", "pa.lib.Schema"]
    ],
]
