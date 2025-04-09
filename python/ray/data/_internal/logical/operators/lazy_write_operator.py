from typing import Any, Optional, Union, Callable

from ray.data._internal.logical.interfaces import LogicalOperator
from ray.data._internal.logical.operators.map_operator import AbstractMap
from ray.data.block import Block
from ray.data.datasource.datasink import Datasink
from ray.data.datasource.datasource import Datasource


class LazyWrite(AbstractMap):
    """Logical operator for write."""

    def __init__(
        self,
        input_op: LogicalOperator,
        prefilter_fn: Callable[[Block], Block] | None,
        datasink_or_legacy_datasource: Union[Datasink, Datasource],
        ray_remote_args: Optional[dict[str, Any]] = None,
        concurrency: Optional[int] = None,
        **write_args,
    ):
        if isinstance(datasink_or_legacy_datasource, Datasink):
            min_rows_per_bundled_input = (
                datasink_or_legacy_datasource.min_rows_per_write
            )
        else:
            min_rows_per_bundled_input = None

        super().__init__(
            "LazyWrite",
            input_op,
            min_rows_per_bundled_input=min_rows_per_bundled_input,
            ray_remote_args=ray_remote_args,
        )
        self._datasink_or_legacy_datasource = datasink_or_legacy_datasource
        self._write_args = write_args
        self._concurrency = concurrency
        self._prefilter_fn = prefilter_fn
