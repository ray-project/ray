from abc import ABC, abstractmethod
from typing import Generic, Iterator

import pyarrow as pa

from ray.data._internal.datasource_v2 import InputSplit
from ray.util.annotations import DeveloperAPI


@DeveloperAPI
class Reader(ABC, Generic[InputSplit]):
    """Abstract base class for reading data from input buckets.

    Readers execute on workers to actually read data. They receive an InputSplit
    (e.g., FileManifest for file-based sources) and yield Arrow tables. A reader
    for a format that pyarrow can't type, such as ``read_json(lines=True)``, may
    yield pandas DataFrames instead; a task's blocks must all be one type.

    The Reader is created by Scanner.create_reader() and is configured with all
    pushdown optimizations (columns, predicates, limits) that were applied.
    """

    @abstractmethod
    def read(self, input_split: InputSplit) -> Iterator[pa.Table]:
        """Read data from the input bucket and yield Arrow tables.

        A reader may yield pandas DataFrames instead, which are never converted
        to Arrow and so need no ``raise_on_pickle_object_columns`` call.

        This method is called on workers to perform the actual read operation.
        It should respect all pushdowns configured on this reader.

        Implement this directly when ``FileReader`` does not decode your
        format or the source is not files at all. Your implementation should:

            1. Open each entry of ``input_split`` and decode it.
            2. Apply every pushdown this reader was configured with.
            3. Call ``raise_on_pickle_object_columns`` on each Arrow table built
               from external input, then yield it.

        Args:
            input_split: Work unit describing what data to read.

        Returns:
            Iterator[pa.Table]: Iterator of PyArrow Tables containing the read data.
        """
        ...
