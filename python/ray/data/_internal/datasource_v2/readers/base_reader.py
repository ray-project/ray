from abc import ABC, abstractmethod
from typing import Generic, Iterator

import pyarrow as pa

from ray.data._internal.datasource_v2 import InputBucket
from ray.util.annotations import DeveloperAPI


@DeveloperAPI
class Reader(ABC, Generic[InputBucket]):
    """Abstract base class for reading data from input buckets.

    Readers execute on workers to actually read data. They receive an InputBucket
    (e.g., FileManifest for file-based sources) and yield Arrow tables.

    The Reader is created by Scanner.create_reader() and is configured with all
    pushdown optimizations (columns, predicates, limits) that were applied.
    """

    @abstractmethod
    def read(self, input_bucket: InputBucket) -> Iterator[pa.Table]:
        """Read data from the input bucket and yield Arrow tables.

        This method is called on workers to perform the actual read operation.
        It should respect all pushdowns configured on this reader.

        Args:
            input_bucket: Work unit describing what data to read.

        Returns:
            Iterator[pa.Table]: Iterator of PyArrow Tables containing the read data.
        """
        ...
