from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Dict

if TYPE_CHECKING:
    from ray.data._internal.execution.interfaces.physical_operator import (
        PhysicalOperator,
    )
    from ray.data._internal.execution.streaming_executor_state import OpState, Topology


class BackpressurePolicy(ABC):
    """Interface for back pressure policies."""

    @abstractmethod
    def __init__(self, topology: "Topology"):
        ...

    def calculate_max_blocks_to_read_per_op(
        self, topology: "Topology"
    ) -> Dict["OpState", int]:
        """Determine how many blocks of data we can read from each operator.
        The `DataOpTask`s of the operators will stop reading blocks when the limit is
        reached. Then the execution of these tasks will be paused when the streaming
        generator backpressure threshold is reached.
        Used in `streaming_executor_state.py::process_completed_tasks()`.

        Returns: A dict mapping from each operator's OpState to the desired number of
            blocks to read. For operators that are not in the dict, all available blocks
            will be read.

        Note: Only one backpressure policy that implements this method can be enabled
            at a time.
        """
        return {}

    def can_add_input(self, op: "PhysicalOperator") -> bool:
        """Determine if we can add a new input to the operator. If returns False, the
        operator will be backpressured and will not be able to run new tasks.
        Used in `streaming_executor_state.py::select_operator_to_run()`.

        Returns: True if we can add a new input to the operator, False otherwise.

        Note, if multiple backpressure policies are enabled, the operator will be
        backpressured if any of the policies returns False.
        """
        return True
