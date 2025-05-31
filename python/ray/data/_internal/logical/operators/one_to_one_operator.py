from typing import TYPE_CHECKING, Optional

from ray.data._internal.logical.interfaces import GuessMetadataMixin, LogicalOperator
from ray.data.block import BlockMetadata

if TYPE_CHECKING:
    import pyarrow as pa


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


class Limit(AbstractOneToOne, GuessMetadataMixin):
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

    def guess_metadata(self) -> BlockMetadata:
        return BlockMetadata(
            num_rows=self._num_rows(),
            size_bytes=None,
            input_files=self._input_files(),
            exec_stats=None,
        )

    def guess_schema(self) -> Optional["pa.lib.Schema"]:
        assert len(self._input_dependencies) == 1, len(self._input_dependencies)
        inp = self.input_dependencies[0]
        if isinstance(inp, GuessMetadataMixin):
            return inp.guess_schema()
        return None

    def _num_rows(self):
        assert len(self._input_dependencies) == 1, len(self._input_dependencies)
        input_rows = None
        if isinstance(self._input_dependencies[0], GuessMetadataMixin):
            input_rows = self._input_dependencies[0].guess_metadata().num_rows
        if input_rows is not None:
            return min(input_rows, self._limit)
        else:
            return None

    def _input_files(self):
        assert len(self._input_dependencies) == 1, len(self._input_dependencies)
        inp = self._input_dependencies[0]
        if isinstance(inp, GuessMetadataMixin):
            return inp.guess_metadata().input_files
        return None
