from typing import Iterator, List, Optional

import pyarrow as pa
from pyarrow import compute as pc
from pyarrow.fs import FileSystem

from ray.data._internal.datasource_v2.listing.file_manifest import FileManifest
from ray.data._internal.datasource_v2.readers.base_reader import Reader


class FileReader(Reader[FileManifest]):
    """Reader for file-based sources.

    This reader uses PyArrow's Dataset API which automatically handles:
    - Column pruning
    - Filter pushdown (row group pruning)
    - Batch-level filtering
    """

    def __init__(
        self,
        columns: Optional[List[str]] = None,
        predicate: Optional[pc.Expression] = None,
        batch_size: int = 65536,
        limit: Optional[int] = None,
        filesystem: Optional[FileSystem] = None,
    ):
        """Initialize the reader.

        Args:
            columns: Columns to read. None means all columns.
            predicate: PyArrow compute expression for filtering.
            batch_size: Number of rows per batch.
            limit: Maximum number of rows to read.
            filesystem: Filesystem for reading files.
        """
        self._columns = columns
        self._predicate = predicate
        self._batch_size = batch_size
        self._limit = limit
        self._filesystem = filesystem

    def read(self, input_split: FileManifest) -> Iterator[pa.Table]:
        """Read data from the input bucket and yield Arrow tables.

        This method is called on workers to perform the actual read operation.
        It should respect all pushdowns configured on this reader.

        Args:
            input_split: Work unit describing what data to read.

        Returns:
            Iterator[pa.Table]: Iterator of PyArrow Tables containing the read data.
        """
        ...
