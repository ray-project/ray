from dataclasses import dataclass, field
from typing import Any, Dict, Optional, Union

from ray.data._internal.compute import ComputeStrategy
from ray.data._internal.logical.interfaces import LogicalOperator
from ray.data._internal.logical.operators.map_operator import AbstractMap
from ray.data.datasource.datasink import Datasink
from ray.data.datasource.datasource import Datasource

__all__ = [
    "Write",
]


@dataclass(frozen=True, repr=False, eq=False)
class Write(AbstractMap):
    """Logical operator for write."""

    datasink_or_legacy_datasource: Union[Datasink, Datasource]
    input_dependencies: list[LogicalOperator] = field(repr=False, kw_only=True)
    ray_remote_args: Dict[str, Any] = field(default_factory=dict)
    compute: Optional[ComputeStrategy] = None
    write_args: Dict[str, Any] = field(default_factory=dict)
    can_modify_num_rows: bool = field(init=False, default=True)
    min_rows_per_bundled_input: Optional[int] = field(init=False)
    ray_remote_args_fn: None = field(init=False, default=None)
    per_block_limit: Optional[int] = None
    _num_outputs: Optional[int] = field(init=False, default=None, repr=False)

    def __post_init__(self):
        assert len(self.input_dependencies) == 1, len(self.input_dependencies)
        if isinstance(self.datasink_or_legacy_datasource, Datasink):
            min_rows_per_bundled_input = (
                self.datasink_or_legacy_datasource.min_rows_per_write
            )
        else:
            min_rows_per_bundled_input = None
        if self.compute is None:
            from ray.data._internal.compute import TaskPoolStrategy

            object.__setattr__(self, "compute", TaskPoolStrategy())
        object.__setattr__(
            self, "min_rows_per_bundled_input", min_rows_per_bundled_input
        )
        object.__setattr__(self, "_num_outputs", None)
