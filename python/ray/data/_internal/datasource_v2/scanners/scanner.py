from abc import ABC, abstractmethod
from typing import Generic

import pyarrow as pa

from ray.data._internal.datasource_v2 import InputSplit
from ray.data._internal.datasource_v2.readers.base_reader import Reader
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
    2. Creating Reader instances configured with all pushdowns

    Splitting the input into parallel work units used to live here as a
    ``plan()`` method. That responsibility now belongs to the listing-side
    pipeline (``ListFiles`` + ``FilePartitioner``); scanners only
    need to answer "what schema?" and "give me a reader."
    """

    @abstractmethod
    def read_schema(self) -> pa.Schema:
        """Return the schema that will be produced by this scanner.

        This reflects the schema after all column pruning has been applied.

        Returns:
            PyArrow Schema describing the output data.
        """
        ...

    def metadata_row_count_is_exact(self) -> bool:
        """Whether this scan's row count may come from file metadata.

        Asked by ``pushdown_count_files`` before it answers ``count()`` without
        reading data. It is about the rows this scan *returns*, so a source
        whose metadata says per file whether a filter is fully satisfied may
        answer ``True`` under that filter; for most sources it collapses to "no
        row-reducing pushdown is set".

        Default ``False``: over-claiming makes ``count()`` return a wrong number
        silently, while declining only costs a real read. Account for every
        row-reducing knob, including ones set at construction
        (``read_iceberg(row_filter=...)``) rather than pushed down. The reader
        answers the same question in ``SupportsMetadata.available_metadata``;
        both must agree.
        """
        return False

    @abstractmethod
    def create_reader(self) -> Reader[InputSplit]:
        """Create a Reader configured for this scanner.

        The returned Reader will have all pushdowns (columns, predicates, limits)
        applied and is ready to execute on workers.

        Returns:
            Configured Reader instance.
        """
        ...
