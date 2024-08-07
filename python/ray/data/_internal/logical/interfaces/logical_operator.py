from typing import TYPE_CHECKING, Iterator, List, Optional, Union

from .operator import Operator

if TYPE_CHECKING:
    import pyarrow

    from ray.data._internal.execution.interfaces import RefBundle


class LogicalOperator(Operator):
    """Abstract class for logical operators.

    A logical operator describes transformation, and later is converted into
    physical operator.
    """

    def __init__(
        self,
        name: str,
        input_dependencies: List["LogicalOperator"],
        num_outputs: Optional[int] = None,
    ):
        super().__init__(
            name,
            input_dependencies,
        )
        for x in input_dependencies:
            assert isinstance(x, LogicalOperator), x
        self._num_outputs = num_outputs

    def estimated_num_outputs(self) -> Optional[int]:
        """Returns the estimated number of blocks that
        would be outputted by this logical operator.

        This method does not execute the plan, so it does not take into consideration
        block splitting. This method only considers high-level block constraints like
        `Dataset.repartition(num_blocks=X)`. A more accurate estimation can be given by
        `PhysicalOperator.num_outputs_total()` during execution.
        """
        if self._num_outputs is not None:
            return self._num_outputs
        elif len(self._input_dependencies) == 1:
            return self._input_dependencies[0].estimated_num_outputs()
        return None

    # Override the following 3 methods to correct type hints.

    @property
    def input_dependencies(self) -> List["LogicalOperator"]:
        return super().input_dependencies  # type: ignore

    @property
    def output_dependencies(self) -> List["LogicalOperator"]:
        return super().output_dependencies  # type: ignore

    def post_order_iter(self) -> Iterator["LogicalOperator"]:
        return super().post_order_iter()  # type: ignore

    def schema(self) -> Optional[Union[type, "pyarrow.lib.Schema"]]:
        """The schema of operator outputs, or ``None`` if not known.

        This method is used to get the dataset schema without performing actual
        computation.
        """
        return None

    def num_rows(self) -> Optional[int]:
        """The number of rows outputted by this operator, or ``None`` if not known.

        This method is used to count the number of rows in a dataset without performing
        actual computation.
        """
        return None

    def input_files(self) -> Optional[List[str]]:
        """The input files of this operator, or ``None`` if not known."""
        return None

    def output_data(self) -> Optional[List["RefBundle"]]:
        """The output data of this operator, or ``None`` if not known."""
        return None

    def is_lineage_serializable(self) -> bool:
        """Returns whether the lineage of this operator can be serialized.

        An operator is lineage serializable if you can serialize it on one machine and
        deserialize it on another without losing information. Operators that store
        object references (e.g., ``InputData``) aren't lineage serializable because the
        objects aren't available on the deserialized machine.
        """
        return True
