from dataclasses import dataclass, field
from typing import Any, Dict, Optional, Union

from ray.data._internal.compute import ComputeStrategy, TaskPoolStrategy
from ray.data._internal.logical.interfaces import (
    LogicalOperator,
    LogicalOperatorPreservesSchema,
)
from ray.data._internal.logical.operators.map_operator import AbstractMap
from ray.data.datasource.datasink import Datasink
from ray.data.datasource.datasource import Datasource

__all__ = [
    "Write",
]


@dataclass(frozen=True, repr=False, eq=False)
class Write(AbstractMap, LogicalOperatorPreservesSchema):
    """Logical operator for write."""

    datasink_or_legacy_datasource: Union[Datasink, Datasource]
    input_dependencies: list[LogicalOperator] = field(repr=False, kw_only=True)
    ray_remote_args: Dict[str, Any] = field(default_factory=dict)
    compute: ComputeStrategy = field(default_factory=TaskPoolStrategy)
    write_args: Dict[str, Any] = field(default_factory=dict)
    can_modify_num_rows: bool = field(init=False, default=True)
    ray_remote_args_fn: None = field(init=False, default=None)
    per_block_limit: Optional[int] = None

    def __post_init__(self):
        assert len(self.input_dependencies) == 1, len(self.input_dependencies)
        datasink = self.datasink_or_legacy_datasource
        if (
            isinstance(datasink, Datasink)
            and datasink.min_rows_per_write is not None
            and datasink.min_bytes_per_write is not None
        ):
            raise ValueError(
                "Datasink cannot specify both min_rows_per_write and "
                "min_bytes_per_write. Set only one write bundling target."
            )

    @property
    def min_rows_per_bundled_input(self) -> Optional[int]:
        if isinstance(self.datasink_or_legacy_datasource, Datasink):
            return self.datasink_or_legacy_datasource.min_rows_per_write
        return None
