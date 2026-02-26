from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Callable, Dict, List, Optional

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
            num_outputs=num_outputs,
            name=name,
        )
        self.can_modify_num_rows = can_modify_num_rows

    @property
    def num_outputs(self) -> Optional[int]:
        return self._num_outputs

    @property
    def input_dependency(self) -> LogicalOperator:
        return self.input_dependencies[0]


@dataclass(frozen=True, repr=False, init=False)
class Limit(AbstractOneToOne, LogicalOperatorSupportsPredicatePassThrough):
    """Logical operator for limit."""

    limit: int
    can_modify_num_rows: bool = field(init=False, default=True)
    _name: str = field(init=False, repr=False)
    _input_dependencies: List[LogicalOperator] = field(init=False, repr=False)
    _num_outputs: Optional[int] = field(init=False, default=None, repr=False)

    def __init__(
        self,
        input_op: LogicalOperator,
        limit: int,
    ):
        assert isinstance(input_op, LogicalOperator), input_op
        object.__setattr__(self, "_name", f"limit={limit}")
        object.__setattr__(self, "_input_dependencies", [input_op])
        object.__setattr__(self, "_num_outputs", None)
        object.__setattr__(self, "can_modify_num_rows", True)
        object.__setattr__(self, "limit", limit)

    def _apply_transform(
        self, transform: Callable[[LogicalOperator], LogicalOperator]
    ) -> LogicalOperator:
        transformed_input = self.input_dependency._apply_transform(transform)
        target: LogicalOperator
        if transformed_input is self.input_dependency:
            target = self
        else:
            target = Limit(transformed_input, self.limit)
        return transform(target)

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


@dataclass(frozen=True, repr=False, init=False)
class Download(AbstractOneToOne):
    """Logical operator for download operation.

    Supports downloading from multiple URI columns in a single operation.
    """

    uri_column_names: List[str]
    output_bytes_column_names: List[str]
    ray_remote_args: Dict[str, Any]
    filesystem: Optional["pyarrow.fs.FileSystem"]
    can_modify_num_rows: bool = field(init=False, default=False)
    _name: str = field(init=False, repr=False)
    _input_dependencies: List[LogicalOperator] = field(init=False, repr=False)
    _num_outputs: Optional[int] = field(init=False, default=None, repr=False)

    def __init__(
        self,
        input_op: LogicalOperator,
        uri_column_names: List[str],
        output_bytes_column_names: List[str],
        ray_remote_args: Optional[Dict[str, Any]] = None,
        filesystem: Optional["pyarrow.fs.FileSystem"] = None,
    ):
        assert isinstance(input_op, LogicalOperator), input_op
        if len(uri_column_names) != len(output_bytes_column_names):
            raise ValueError(
                f"Number of URI columns ({len(uri_column_names)}) must match "
                f"number of output columns ({len(output_bytes_column_names)})"
            )
        object.__setattr__(self, "_name", "Download")
        object.__setattr__(self, "_input_dependencies", [input_op])
        object.__setattr__(self, "_num_outputs", None)
        object.__setattr__(self, "can_modify_num_rows", False)
        object.__setattr__(self, "uri_column_names", uri_column_names)
        object.__setattr__(self, "output_bytes_column_names", output_bytes_column_names)
        object.__setattr__(self, "ray_remote_args", ray_remote_args or {})
        object.__setattr__(self, "filesystem", filesystem)

    def _apply_transform(
        self, transform: Callable[[LogicalOperator], LogicalOperator]
    ) -> LogicalOperator:
        transformed_input = self.input_dependency._apply_transform(transform)
        target: LogicalOperator
        if transformed_input is self.input_dependency:
            target = self
        else:
            target = Download(
                transformed_input,
                uri_column_names=self.uri_column_names,
                output_bytes_column_names=self.output_bytes_column_names,
                ray_remote_args=self.ray_remote_args,
                filesystem=self.filesystem,
            )
        return transform(target)
