from dataclasses import dataclass
from typing import Optional

import pyarrow as pa

from ray.data._internal.datasource.parquet_datasource import (
    check_for_legacy_tensor_type,
)
from ray.data._internal.datasource_v2.readers.file_reader import (
    INCLUDE_PATHS_COLUMN_NAME,
    ROW_HASH_COLUMN_NAME,
)
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

    Inherits filter pushdown, column pruning, limit pushdown, partition
    pruning, and file shuffle from ArrowFileScanner. Adds Parquet-specific
    reader creation with adaptive batch sizing and the Parquet-only
    legacy-tensor-type schema check.
    """

    target_block_size: Optional[int] = None
    include_paths: bool = False
    include_row_hash: bool = False

    def read_schema(self) -> pa.Schema:
        """Return schema after column pruning and tensor check.

        ``path`` and ``row_hash`` are synthesized post-read by the file
        reader, but only for columns listed in ``self.columns`` (see
        ``file_reader.read``'s ``columns_to_synthesize`` filter). When a
        projection has pruned a synthesized column away, advertising it
        here would put the schema out of sync with the actual blocks — so
        only append when no projection is active or when it survives.
        """
        schema = super().read_schema()
        synthesized = (
            (self.include_paths, INCLUDE_PATHS_COLUMN_NAME, pa.string()),
            (self.include_row_hash, ROW_HASH_COLUMN_NAME, pa.uint64()),
        )
        for enabled, name, dtype in synthesized:
            if not enabled:
                continue
            if self.columns is not None and name not in self.columns:
                continue
            if schema.get_field_index(name) != -1:
                continue
            schema = schema.append(pa.field(name, dtype))

        check_for_legacy_tensor_type(schema)
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
            partitioning=self.partitioning,
            ignore_prefixes=self.ignore_prefixes,
            target_block_size=self.target_block_size,
            include_paths=self.include_paths,
            include_row_hash=self.include_row_hash,
            schema=self.schema,
        )
