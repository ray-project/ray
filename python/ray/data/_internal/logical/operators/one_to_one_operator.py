from typing import TYPE_CHECKING, Any, Dict, List, Optional

from ray.data._internal.logical.interfaces import (
    LogicalOperator,
    LogicalOperatorSupportsPredicatePassThrough,
    PredicatePassThroughBehavior,
)
from ray.data.block import BlockMetadata

if TYPE_CHECKING:

    from ray.data.block import Schema


class AbstractOneToOne(LogicalOperator):
    """Abstract class for one-to-one logical operators, which
    have one input and one output dependency.
    """

    def __init__(
        self,
        name: str,
        input_op: Optional[LogicalOperator],
        num_outputs: Optional[int] = None,
    ):
        """
        Args:
            name: Name for this operator. This is the name that will appear when
                inspecting the logical plan of a Dataset.
            input_op: The operator preceding this operator in the plan DAG. The outputs
                of `input_op` will be the inputs to this operator.
        """
        super().__init__(name, [input_op] if input_op else [], num_outputs)

    @property
    def input_dependency(self) -> LogicalOperator:
        return self._input_dependencies[0]

    def can_modify_num_rows(self) -> bool:
        """Whether this operator can modify the number of rows,
        i.e. number of input rows != number of output rows."""
        ...


class Limit(AbstractOneToOne, LogicalOperatorSupportsPredicatePassThrough):
    """Logical operator for limit."""

    def __init__(
        self,
        input_op: LogicalOperator,
        limit: int,
    ):
        super().__init__(
            f"limit={limit}",
            input_op,
        )
        self._limit = limit

    def can_modify_num_rows(self) -> bool:
        return True

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
        super().__init__("Download", input_op)
        if len(uri_column_names) != len(output_bytes_column_names):
            raise ValueError(
                f"Number of URI columns ({len(uri_column_names)}) must match "
                f"number of output columns ({len(output_bytes_column_names)})"
            )
        self._uri_column_names = uri_column_names
        self._output_bytes_column_names = output_bytes_column_names
        self._ray_remote_args = ray_remote_args or {}

    def can_modify_num_rows(self) -> bool:
        return False

    @property
    def uri_column_names(self) -> List[str]:
        return self._uri_column_names

    @property
    def output_bytes_column_names(self) -> List[str]:
        return self._output_bytes_column_names

    @property
    def ray_remote_args(self) -> Dict[str, Any]:
        return self._ray_remote_args
