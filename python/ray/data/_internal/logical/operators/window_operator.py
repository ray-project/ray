"""
Logical operator for window operations.
"""

from typing import List, Optional, TYPE_CHECKING, Dict, Any

from ray.data._internal.logical.interfaces import LogicalOperator
from ray.data._internal.logical.operators.all_to_all_operator import AbstractAllToAll
from ray.data.window import WindowSpec

if TYPE_CHECKING:
    from ray.data.block import BlockMetadata
    from ray.data import Schema
    from ray.data.aggregate import AggregateFn


class Window(AbstractAllToAll):
    """Logical operator for window operations."""

    def __init__(
        self,
        input_op: LogicalOperator,
        window_spec: WindowSpec,
        aggs: List["AggregateFn"],
        num_partitions: Optional[int] = None,
        batch_format: Optional[str] = "default",
        compute_strategy: Optional["ComputeStrategy"] = None,
        ray_remote_args: Optional[Dict[str, Any]] = None,
    ):
        super().__init__(
            "Window",
            input_op,
            sub_progress_bar_names=[
                "WindowSample",
                "WindowShuffle",
                "WindowAggregate",
            ],
        )
        self._window_spec = window_spec
        self._aggs = aggs
        self._num_partitions = num_partitions
        self._batch_format = batch_format
        self._compute_strategy = compute_strategy
        self._ray_remote_args = ray_remote_args or {}

    @property
    def window_spec(self) -> WindowSpec:
        """Get the window specification."""
        return self._window_spec

    @property
    def aggs(self) -> List["AggregateFn"]:
        """Get the aggregation functions."""
        return self._aggs

    @property
    def num_partitions(self) -> Optional[int]:
        """Get the number of partitions."""
        return self._num_partitions

    @property
    def batch_format(self) -> Optional[str]:
        """Get the batch format."""
        return self._batch_format

    @property
    def compute_strategy(self) -> Optional["ComputeStrategy"]:
        """Get the compute strategy."""
        return self._compute_strategy

    @property
    def ray_remote_args(self) -> Dict[str, Any]:
        """Get the Ray remote arguments."""
        return self._ray_remote_args

    @property
    def compute_strategy(self) -> Optional["ComputeStrategy"]:
        """Get the compute strategy."""
        return self._compute_strategy

    @property
    def ray_remote_args(self) -> Dict[str, Any]:
        """Get the Ray remote arguments."""
        return self._ray_remote_args

    def infer_metadata(self) -> "BlockMetadata":
        assert len(self._input_dependencies) == 1, len(self._input_dependencies)
        assert isinstance(self._input_dependencies[0], LogicalOperator)
        return self._input_dependencies[0].infer_metadata()

    def infer_schema(
        self,
    ) -> Optional["Schema"]:
        assert len(self._input_dependencies) == 1, len(self._input_dependencies)
        return self._input_dependencies[0].infer_schema()
