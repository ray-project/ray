from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Dict, List, Optional

from ray.data._internal.logical.interfaces import (
    LogicalOperator,
    LogicalOperatorSupportsPredicatePassThrough,
    PredicatePassThroughBehavior,
)
from ray.data.block import BlockMetadata

if TYPE_CHECKING:

    from ray.data.block import Schema

__all__ = [
    "AbstractOneToOne",
    "Download",
    "Limit",
]


@dataclass(frozen=True, repr=False)
class AbstractOneToOne(LogicalOperator):
    """Abstract class for one-to-one logical operators, which
    have one input and one output dependency.
    """

    input_op: Optional[LogicalOperator] = None
    can_modify_num_rows: bool = False

    def __post_init__(self) -> None:
        if not self.input_dependencies and self.input_op is not None:
            object.__setattr__(self, "input_dependencies", [self.input_op])
        super().__post_init__()

    @property
    def input_dependency(self) -> LogicalOperator:
        return self.input_dependencies[0]


@dataclass(frozen=True, repr=False)
class Limit(AbstractOneToOne, LogicalOperatorSupportsPredicatePassThrough):
    """Logical operator for limit."""

    limit: int = 0

    def __post_init__(self) -> None:
        if self.name is None:
            object.__setattr__(self, "name", f"limit={self.limit}")
        object.__setattr__(self, "can_modify_num_rows", True)
        super().__post_init__()

    def infer_metadata(self) -> BlockMetadata:
        return BlockMetadata(
            num_rows=self._num_rows(),
            size_bytes=None,
            input_files=self._input_files(),
            exec_stats=None,
        )

    def infer_schema(
        self,
    ) -> Optional["Schema"]:
        assert len(self.input_dependencies) == 1, len(self.input_dependencies)
        assert isinstance(self.input_dependencies[0], LogicalOperator)
        return self.input_dependencies[0].infer_schema()

    def _num_rows(self):
        assert len(self.input_dependencies) == 1, len(self.input_dependencies)
        assert isinstance(self.input_dependencies[0], LogicalOperator)
        input_rows = self.input_dependencies[0].infer_metadata().num_rows
        if input_rows is not None:
            return min(input_rows, self.limit)
        else:
            return None

    def _input_files(self):
        assert len(self.input_dependencies) == 1, len(self.input_dependencies)
        assert isinstance(self.input_dependencies[0], LogicalOperator)
        return self.input_dependencies[0].infer_metadata().input_files

    def predicate_passthrough_behavior(self) -> PredicatePassThroughBehavior:
        # Pushing filter through limit is safe: Filter(Limit(data, n), pred)
        # becomes Limit(Filter(data, pred), n), which filters earlier
        return PredicatePassThroughBehavior.PASSTHROUGH


@dataclass(frozen=True, repr=False)
class Download(AbstractOneToOne):
    """Logical operator for download operation.

    Supports downloading from multiple URI columns in a single operation.
    """

    uri_column_names: List[str] = None  # type: ignore[assignment]
    output_bytes_column_names: List[str] = None  # type: ignore[assignment]
    ray_remote_args: Optional[Dict[str, Any]] = None

    def __post_init__(self) -> None:
        if self.name is None:
            object.__setattr__(self, "name", "Download")
        if len(self.uri_column_names) != len(self.output_bytes_column_names):
            raise ValueError(
                f"Number of URI columns ({len(self.uri_column_names)}) must match "
                f"number of output columns ({len(self.output_bytes_column_names)})"
            )
        if self.ray_remote_args is None:
            object.__setattr__(self, "ray_remote_args", {})
        object.__setattr__(self, "can_modify_num_rows", False)
        super().__post_init__()
