from dataclasses import dataclass

import pyarrow as pa

from ray.data._internal.datasource_v2.readers.file_reader import (
    INCLUDE_PATHS_COLUMN_NAME,
    ROW_HASH_COLUMN_NAME,
)
from ray.data._internal.datasource_v2.readers.orc_file_reader import OrcFileReader
from ray.data._internal.datasource_v2.scanners.arrow_file_scanner import (
    ArrowFileScanner,
)
from ray.util.annotations import DeveloperAPI


@DeveloperAPI
@dataclass(frozen=True)
class OrcScanner(ArrowFileScanner):
    """ORC scanner that reads manifest rows as stripe-based chunks."""

    include_paths: bool = False
    include_row_hash: bool = False

    def read_schema(self) -> pa.Schema:
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
            if schema.get_field_index(name) == -1:
                schema = schema.append(pa.field(name, dtype))
        return schema

    def create_reader(self) -> OrcFileReader:
        return OrcFileReader(
            batch_size=self.batch_size,
            columns=list(self.columns) if self.columns is not None else None,
            predicate=self.predicate,
            limit=self.limit,
            filesystem=self.filesystem,
            partitioning=self.partitioning,
            include_paths=self.include_paths,
            include_row_hash=self.include_row_hash,
            schema=self.schema,
        )
