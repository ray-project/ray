from dataclasses import dataclass
from typing import Optional

import pyarrow as pa

from ray.data._internal.datasource.parquet_datasource import (
    check_for_legacy_tensor_type,
)
from ray.data._internal.datasource_v2.readers.file_reader import (
    INCLUDE_PATHS_COLUMN_NAME,
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

    def read_schema(self) -> pa.Schema:
        """Return schema after column pruning and tensor check.

        The path column is synthesized post-read by the file reader, but
        only for columns listed in ``self.columns`` (see
        ``file_reader.read``'s ``columns_to_synthesize`` filter). When a
        projection has pruned the path column away, advertising it here
        would put the schema out of sync with the actual blocks — so only
        append it when no projection is active or when it survives the
        projection.
        """
        schema = super().read_schema()
        path_kept_by_projection = (
            self.columns is None or INCLUDE_PATHS_COLUMN_NAME in self.columns
        )
        if (
            self.include_paths
            and path_kept_by_projection
            and schema.get_field_index(INCLUDE_PATHS_COLUMN_NAME) == -1
        ):
            schema = schema.append(pa.field(INCLUDE_PATHS_COLUMN_NAME, pa.string()))

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
            schema=self.schema,
        )
