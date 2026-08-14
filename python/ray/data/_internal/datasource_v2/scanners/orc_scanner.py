from dataclasses import dataclass

import pyarrow as pa

from ray.data._internal.datasource_v2.readers.file_reader import (
    INCLUDE_PATHS_COLUMN_NAME,
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

    def read_schema(self) -> pa.Schema:
        schema = super().read_schema()
        if (
            self.include_paths
            and (self.columns is None or INCLUDE_PATHS_COLUMN_NAME in self.columns)
            and schema.get_field_index(INCLUDE_PATHS_COLUMN_NAME) == -1
        ):
            schema = schema.append(pa.field(INCLUDE_PATHS_COLUMN_NAME, pa.string()))
        return schema

    def create_reader(self) -> OrcFileReader:
        return OrcFileReader(
            columns=list(self.columns) if self.columns is not None else None,
            predicate=self.predicate,
            limit=self.limit,
            filesystem=self.filesystem,
            partitioning=self.partitioning,
            include_paths=self.include_paths,
            schema=self.schema,
        )
