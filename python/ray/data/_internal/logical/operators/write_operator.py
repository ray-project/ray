from dataclasses import InitVar, dataclass, field, replace
from typing import Any, Callable, Dict, Optional, Union

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

    input_op: InitVar[LogicalOperator]
    datasink_or_legacy_datasource: Union[Datasink, Datasource]
    ray_remote_args: Dict[str, Any] = field(default_factory=dict)
    compute: Optional[ComputeStrategy] = None
    write_args: Dict[str, Any] = field(default_factory=dict)
    can_modify_num_rows: bool = field(init=False, default=True)
    min_rows_per_bundled_input: Optional[int] = field(init=False)
    ray_remote_args_fn: None = field(init=False, default=None)
    per_block_limit: Optional[int] = None
    _name: str = field(init=False, repr=False)
    _input_dependencies: list[LogicalOperator] = field(init=False, repr=False)
    _num_outputs: Optional[int] = field(init=False, default=None, repr=False)

    def __post_init__(self, input_op: LogicalOperator):
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
        object.__setattr__(self, "_input_dependencies", [input_op])
        object.__setattr__(self, "_num_outputs", None)

    def _apply_transform(
        self, transform: Callable[[LogicalOperator], LogicalOperator]
    ) -> LogicalOperator:
        input_op = self.input_dependencies[0]
        transformed_input = input_op._apply_transform(transform)
        target: LogicalOperator
        if transformed_input is input_op:
            target = self
        else:
            target = replace(self, input_op=transformed_input)
        return transform(target)
