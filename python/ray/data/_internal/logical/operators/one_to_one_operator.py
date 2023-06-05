from typing import Optional

from ray.data._internal.dataset_logger import DatasetLogger
from ray.data._internal.logical.interfaces import LogicalOperator

logger = DatasetLogger(__name__)


class AbstractOneToOne(LogicalOperator):
    """Abstract class for one-to-one logical operators should
    be converted to their corresponding physical operators.
    """

    def __init__(
        self,
        name: str,
        input_op: Optional[LogicalOperator],
    ):
        """
        Args:
            name: Name for this operator. This is the name that will appear when
                inspecting the logical plan of a Dataset.
            input_op: The operator preceding this operator in the plan DAG. The outputs
                of `input_op` will be the inputs to this operator.
            num_outputs: The number of expected output bundles outputted by this
                operator.
            ray_remote_args: Args to provide to ray.remote.
        """
        super().__init__(name, [input_op] if input_op else [])

    @property
    def input_dependency(self) -> LogicalOperator:
        return self._input_dependencies[0]

    @property
    def can_modify_num_rows(self) -> bool:
        """Whether this operator can modify the number of rows,
        i.e. number of input rows != number of output rows."""
        raise AttributeError


class Limit(AbstractOneToOne):
    """Logical operator for limit."""

    def __init__(
        self,
        input_op: LogicalOperator,
        limit: int,
    ):
        super().__init__(
            "Limit",
            input_op,
        )
        self._limit = limit

    @property
    def can_modify_num_rows(self) -> bool:
        return True
