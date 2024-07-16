from typing import Callable, List, Optional

from ray.data._internal.execution.interfaces import RefBundle
from ray.data._internal.logical.interfaces import LogicalOperator
from ray.data._internal.util import unify_block_metadata_schema


class InputData(LogicalOperator):
    """Logical operator for input data.

    This may hold cached blocks from a previous Dataset execution, or
    the arguments for read tasks.
    """

    def __init__(
        self,
        input_data: Optional[List[RefBundle]] = None,
        input_data_factory: Optional[Callable[[int], List[RefBundle]]] = None,
    ):
        assert (input_data is None) != (
            input_data_factory is None
        ), "Only one of input_data and input_data_factory should be set."
        super().__init__(
            "InputData", [], len(input_data) if input_data is not None else None
        )
        self.input_data = input_data
        self.input_data_factory = input_data_factory

    def schema(self):
        if self.input_data is None:
            return None

        metadata = [m for bundle in self.input_data for m in bundle.metadata]
        return unify_block_metadata_schema(metadata)

    def num_rows(self):
        if self.input_data is None:
            return None
        elif all(bundle.num_rows() is not None for bundle in self.input_data):
            return sum(bundle.num_rows() for bundle in self.input_data)
        else:
            return None

    def output_data(self) -> Optional[List[RefBundle]]:
        if self.input_data is None:
            return None
        return self.input_data

    def is_lineage_serializable(self) -> bool:
        # This operator isn't serializable because it contains ObjectRefs.
        return False
