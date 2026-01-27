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


class AbstractOneToOne(LogicalOperator):
    """Abstract class for one-to-one logical operators, which
    have one input and one output dependency.
    """

    def __init__(
        self,
        name: str,
        input_op: Optional[LogicalOperator],
        can_modify_num_rows: bool,
        num_outputs: Optional[int] = None,
    ):
        """Initialize an AbstractOneToOne operator.

        Args:
            name: Name for this operator. This is the name that will appear when
                inspecting the logical plan of a Dataset.
            input_op: The operator preceding this operator in the plan DAG. The outputs
                of `input_op` will be the inputs to this operator.
            can_modify_num_rows: Whether the UDF can change the row count. False if
                # of input rows = # of output rows. True otherwise.
            num_outputs: If known, the number of blocks produced by this operator.
        """
        super().__init__(
            name=name,
            input_dependencies=[input_op] if input_op else [],
            num_outputs=num_outputs,
        )
        self._can_modify_num_rows = can_modify_num_rows

    @property
    def input_dependency(self) -> LogicalOperator:
        return self._input_dependencies[0]

    def can_modify_num_rows(self) -> bool:
        """Whether this operator can modify the number of rows,
        i.e. number of input rows != number of output rows."""
        return self._can_modify_num_rows


class Limit(AbstractOneToOne, LogicalOperatorSupportsPredicatePassThrough):
    """Logical operator for limit."""

    def __init__(
        self,
        input_op: LogicalOperator,
        limit: int,
    ):
        super().__init__(
            f"limit={limit}",
            input_op=input_op,
            can_modify_num_rows=True,
        )
        self._limit = limit

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
        assert len(self._input_dependencies) == 1, len(self._input_dependencies)
        assert isinstance(self._input_dependencies[0], LogicalOperator)
        return self._input_dependencies[0].infer_schema()

    def _num_rows(self):
        assert len(self._input_dependencies) == 1, len(self._input_dependencies)
        assert isinstance(self._input_dependencies[0], LogicalOperator)
        input_rows = self._input_dependencies[0].infer_metadata().num_rows
        if input_rows is not None:
            return min(input_rows, self._limit)
        else:
            return None

    def _input_files(self):
        assert len(self._input_dependencies) == 1, len(self._input_dependencies)
        assert isinstance(self._input_dependencies[0], LogicalOperator)
        return self._input_dependencies[0].infer_metadata().input_files

    def predicate_passthrough_behavior(self) -> PredicatePassThroughBehavior:
        # Pushing filter through limit is safe: Filter(Limit(data, n), pred)
        # becomes Limit(Filter(data, pred), n), which filters earlier
        return PredicatePassThroughBehavior.PASSTHROUGH


class Download(AbstractOneToOne):
    """Logical operator for download operation.

    Supports downloading from multiple URI columns in a single operation.
    """

    def __init__(
        self,
        input_op: LogicalOperator,
        uri_column_names: List[str],
        output_bytes_column_names: List[str],
        ray_remote_args: Optional[Dict[str, Any]] = None,
    ):
        super().__init__("Download", input_op, can_modify_num_rows=False)
        if len(uri_column_names) != len(output_bytes_column_names):
            raise ValueError(
                f"Number of URI columns ({len(uri_column_names)}) must match "
                f"number of output columns ({len(output_bytes_column_names)})"
            )
        self._uri_column_names = uri_column_names
        self._output_bytes_column_names = output_bytes_column_names
        self._ray_remote_args = ray_remote_args or {}

    @property
    def uri_column_names(self) -> List[str]:
        return self._uri_column_names

    @property
    def output_bytes_column_names(self) -> List[str]:
        return self._output_bytes_column_names

    @property
    def ray_remote_args(self) -> Dict[str, Any]:
        return self._ray_remote_args
