from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Dict, List, Optional

from ray.data._internal.logical.interfaces import (
    LogicalOperator,
    LogicalOperatorSupportsPredicatePassThrough,
    PredicatePassThroughBehavior,
)
from ray.data.block import BlockMetadata

if TYPE_CHECKING:
    import pyarrow

    from ray.data.block import Schema

__all__ = [
    "AbstractOneToOne",
    "Download",
    "Limit",
]


@dataclass(frozen=True, repr=False, eq=False, init=False)
class AbstractOneToOne(LogicalOperator):
    """Abstract class for one-to-one logical operators, which
    have one input and one output dependency.
    """

    def __init__(
        self,
        input_op: Optional[LogicalOperator],
        can_modify_num_rows: bool,
        num_outputs: Optional[int] = None,
        *,
        name: Optional[str] = None,
    ):
        """Initialize an AbstractOneToOne operator.

        Args:
            input_op: The operator preceding this operator in the plan DAG. The outputs
                of `input_op` will be the inputs to this operator.
            can_modify_num_rows: Whether the UDF can change the row count. False if
                # of input rows = # of output rows. True otherwise.
            num_outputs: If known, the number of blocks produced by this operator.
            name: Name for this operator. This is the name that will appear when
                inspecting the logical plan of a Dataset.
        """
        super().__init__(
            input_dependencies=[input_op] if input_op else [],
        )
        object.__setattr__(self, "num_outputs", num_outputs)
        object.__setattr__(self, "name", name or self.__class__.__name__)
        object.__setattr__(self, "can_modify_num_rows", can_modify_num_rows)


@dataclass(frozen=True, repr=False, eq=False)
class Limit(AbstractOneToOne, LogicalOperatorSupportsPredicatePassThrough):
    """Logical operator for limit."""

    limit: int
    input_dependencies: List[LogicalOperator] = field(repr=False, kw_only=True)
    can_modify_num_rows: bool = field(init=False, default=True)

    def __post_init__(self):
        super().__post_init__()
        assert len(self.input_dependencies) == 1, len(self.input_dependencies)
        object.__setattr__(self, "name", f"limit={self.limit}")
        object.__setattr__(self, "num_outputs", None)

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


@dataclass(frozen=True, repr=False, eq=False)
class Download(AbstractOneToOne):
    """Logical operator for download operation.

    Supports downloading from multiple URI columns in a single operation.
    """

    uri_column_names: List[str]
    output_bytes_column_names: List[str]
    ray_remote_args: Dict[str, Any] = field(default_factory=dict)
    filesystem: Optional["pyarrow.fs.FileSystem"] = None
    input_dependencies: List[LogicalOperator] = field(repr=False, kw_only=True)
    can_modify_num_rows: bool = field(init=False, default=False)

    def __post_init__(self):
        super().__post_init__()
        assert len(self.input_dependencies) == 1, len(self.input_dependencies)
        if len(self.uri_column_names) != len(self.output_bytes_column_names):
            raise ValueError(
                f"Number of URI columns ({len(self.uri_column_names)}) must match "
                f"number of output columns ({len(self.output_bytes_column_names)})"
            )
        object.__setattr__(self, "name", self.__class__.__name__)
        object.__setattr__(self, "num_outputs", None)
