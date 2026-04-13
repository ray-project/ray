from abc import ABC, abstractmethod
from typing import Generic, List, Optional

import pyarrow as pa

from ray.data._internal.io.datasource_v2 import InputSplit
from ray.data._internal.io.datasource_v2.readers.base_reader import Reader
from ray.data.context import DataContext
from ray.util.annotations import DeveloperAPI


@DeveloperAPI
class Scanner(ABC, Generic[InputSplit]):
    """Abstract base class for configured scanners.

    A Scanner represents the logical result of reading data, including applied
    filters, projections, limits, and other pushdown operations. It is an
    immutable abstraction: each push operation returns a new Scanner instance
    via cloning rather than mutation.

    The Scanner is responsible for:
    1. Determining the output schema after all projections
    2. Planning input partitions for parallel execution
    3. Creating Reader instances configured with all pushdowns
    """

    @abstractmethod
    def read_schema(self) -> pa.Schema:
        """Return the schema that will be produced by this scanner.

        This reflects the schema after all column pruning has been applied.

        Returns:
            PyArrow Schema describing the output data.
        """
        ...

    @abstractmethod
    def plan(
        self,
        manifest: InputSplit,
        parallelism: int,
        data_context: Optional["DataContext"] = None,
    ) -> List[InputSplit]:
        """Split the input into parallel work units.

        This method determines how to divide the work for parallel execution.
        The resulting partitions should be roughly balanced in terms of work.

        Args:
            manifest: The full input to partition.
            parallelism: Target number of parallel tasks.
            data_context: Optional data context for configuration.

        Returns:
            List of InputSplit objects, one per parallel task.
        """
        ...

    @abstractmethod
    def create_reader(self) -> Reader[InputSplit]:
        """Create a Reader configured for this scanner.

        The returned Reader will have all pushdowns (columns, predicates, limits)
        applied and is ready to execute on workers.

        Returns:
            Configured Reader instance.
        """
        ...
