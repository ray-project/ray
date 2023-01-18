import sys
from typing import Any, Dict, Iterable, Optional, Union
from ray.data._internal.compute import ComputeStrategy
from ray.data._internal.lazy_block_list import LazyBlockList

from ray.data._internal.logical.interfaces import LogicalOperator
from ray.data.block import BatchUDF
from ray.data.datasource.datasource import Datasource

if sys.version_info >= (3, 8):
    from typing import Literal
else:
    from typing_extensions import Literal


class Read(LogicalOperator):
    """Logical operator for read."""

    def __init__(
        self,
        blocks: LazyBlockList,
        datasource: Datasource,
        parallelism: int = -1,
        ray_remote_args: Dict[str, Any] = None,
        read_args: Dict[str, Any] = None,
    ):
        super().__init__("Read", [])
        self._blocks = blocks
        self._datasource = datasource
        self._parallelism = parallelism
        self._ray_remote_args = ray_remote_args
        self._read_args = read_args


class MapBatches(LogicalOperator):
    """Logical operator for map_batches."""

    def __init__(
        self,
        fn: BatchUDF,
        batch_size: Optional[Union[int, Literal["default"]]] = "default",
        compute: Optional[Union[str, ComputeStrategy]] = None,
        batch_format: Literal["default", "pandas", "pyarrow", "numpy"] = "default",
        zero_copy_batch: bool = False,
        fn_args: Optional[Iterable[Any]] = None,
        fn_kwargs: Optional[Dict[str, Any]] = None,
        fn_constructor_args: Optional[Iterable[Any]] = None,
        fn_constructor_kwargs: Optional[Dict[str, Any]] = None,
        ray_remote_args: Optional[Dict[str, Any]] = None,
    ):
        super().__init__("MapBatches", [])
        self._fn = fn
        self._batch_size = batch_size
        self._compute = compute
        self._batch_format = batch_format
        self._zero_copy_batch = zero_copy_batch
        self._fn_args = fn_args
        self._fn_kwargs = fn_kwargs
        self._fn_constructor_args = fn_constructor_args
        self._fn_constructor_kwargs = fn_constructor_kwargs
        self._ray_remote_args = ray_remote_args
