from abc import ABC, abstractmethod
from typing import Generic, Iterator, Mapping, Optional

import numpy as np
import pyarrow as pa

from ray.data._internal.datasource_v2 import InputSplit
from ray.util.annotations import DeveloperAPI


@DeveloperAPI
class Reader(ABC, Generic[InputSplit]):
    """Abstract base class for reading data from input buckets.

    Readers execute on workers to actually read data. They receive an InputSplit
    (e.g., FileManifest for file-based sources) and yield Arrow tables.

    The Reader is created by Scanner.create_reader() and is configured with all
    pushdown optimizations (columns, predicates, limits) that were applied.
    """

    @abstractmethod
    def read(
        self,
        input_split: InputSplit,
        excluded_rows: Optional[Mapping[str, np.ndarray]] = None,
    ) -> Iterator[pa.Table]:
        """Read data from the input bucket and yield Arrow tables.

        This method is called on workers to perform the actual read operation.
        It should respect all pushdowns configured on this reader.

        Args:
            input_split: Work unit describing what data to read.
            excluded_rows: Rows to leave out, keyed by
                :attr:`~ray.data._internal.datasource_v2.interfaces.read_units.ReadUnit.id`:
                a boolean mask over the rows the reader produces from that
                unit, in order, where ``True`` drops the row. Rows past the
                end of a mask are kept. Used by resumed jobs to skip rows a
                checkpoint already finished.

        Returns:
            Iterator[pa.Table]: Iterator of PyArrow Tables containing the read data.
        """
        ...
