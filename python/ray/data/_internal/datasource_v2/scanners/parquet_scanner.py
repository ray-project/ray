from dataclasses import dataclass
from typing import Optional

import pyarrow as pa

from ray.data._internal.datasource_v2.readers.parquet_file_reader import (
    ParquetFileReader,
)
from ray.data._internal.datasource_v2.scanners.arrow_file_scanner import (
    ArrowFileScanner,
)
from ray.util.annotations import DeveloperAPI


@DeveloperAPI
@dataclass(frozen=True)
class ParquetScanner(ArrowFileScanner):
    """Parquet-specific scanner implementation.

    Inherits filter pushdown, column pruning, limit pushdown, and partition
    pruning from ArrowFileScanner. Adds Parquet-specific reader creation
    with adaptive batch sizing.
    """

    target_block_size: Optional[int] = None
    include_paths: bool = False

    def read_schema(self) -> pa.Schema:
        """Return schema after column pruning, with path column if requested."""
        schema = super().read_schema()
        if self.include_paths and schema.get_field_index("path") == -1:
            schema = schema.append(pa.field("path", pa.string()))
        return schema

    def create_reader(self) -> ParquetFileReader:
        """Create a ParquetFileReader configured for this scanner.

        Returns:
            ParquetFileReader with all pushdowns and adaptive batch sizing.
        """
        return ParquetFileReader(
            batch_size=self.batch_size,
            columns=list(self.columns) if self.columns is not None else None,
            predicate=self.predicate,
            limit=self.limit,
            filesystem=self.filesystem,
            partitioning=self.partitioning.to_pyarrow() if self.partitioning else None,
            ignore_prefixes=self.ignore_prefixes,
            target_block_size=self.target_block_size,
            include_paths=self.include_paths,
        )
