from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Union

from ray.data._internal.compute import ComputeStrategy
from ray.data._internal.logical.interfaces import LogicalOperator
from ray.data._internal.logical.operators.map_operator import AbstractMap
from ray.data.datasource.datasink import Datasink
from ray.data.datasource.datasource import Datasource

__all__ = [
    "Write",
]


@dataclass(frozen=True, init=False, repr=False)
class Write(AbstractMap):
    """Logical operator for write."""

    datasink_or_legacy_datasource: Union[Datasink, Datasource]
    write_args: Dict[str, Any]

    def __init__(
        self,
        input_op: Optional[LogicalOperator] = None,
        datasink_or_legacy_datasource: Optional[Union[Datasink, Datasource]] = None,
        input_dependencies: Optional[List[LogicalOperator]] = None,
        num_outputs: Optional[int] = None,
        ray_remote_args: Optional[Dict[str, Any]] = None,
        compute: Optional[ComputeStrategy] = None,
        name: Optional[str] = None,
        per_block_limit: Optional[int] = None,
        write_args: Optional[Dict[str, Any]] = None,
        **extra_write_args,
    ):
        assert datasink_or_legacy_datasource is not None
        if name is None:
            name = "Write"
        if write_args is None:
            write_args = extra_write_args
        elif extra_write_args:
            write_args = {**write_args, **extra_write_args}
        if isinstance(datasink_or_legacy_datasource, Datasink):
            min_rows_per_bundled_input = (
                datasink_or_legacy_datasource.min_rows_per_write
            )
        else:
            min_rows_per_bundled_input = None

        if input_dependencies is None:
            assert input_op is not None
            input_dependencies = [input_op]
        if input_op is None:
            assert len(input_dependencies) == 1
            input_op = input_dependencies[0]
        super().__init__(
            name=name,
            input_op=input_op,
            can_modify_num_rows=True,
            num_outputs=num_outputs,
            min_rows_per_bundled_input=min_rows_per_bundled_input,
            ray_remote_args=ray_remote_args,
            compute=compute,
            per_block_limit=per_block_limit,
        )
        object.__setattr__(
            self, "datasink_or_legacy_datasource", datasink_or_legacy_datasource
        )
        object.__setattr__(self, "write_args", write_args)
