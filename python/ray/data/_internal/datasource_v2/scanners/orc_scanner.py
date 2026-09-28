from dataclasses import dataclass
from typing import Tuple

import pyarrow as pa

from ray.data._internal.datasource_v2.readers.file_reader import (
    _ARROW_DEFAULT_BATCH_SIZE,
)
from ray.data._internal.datasource_v2.readers.orc_file_reader import OrcFileReader
from ray.data._internal.datasource_v2.readers.synthesized_columns import (
    SynthesizedColumn,
)
from ray.data._internal.datasource_v2.scanners.arrow_file_scanner import (
    ArrowFileScanner,
)
from ray.util.annotations import DeveloperAPI


@DeveloperAPI
@dataclass(frozen=True)
class OrcScanner(ArrowFileScanner):
    """Configure a file-level ORC scan through PyArrow Dataset."""

    synthesized_columns: Tuple[SynthesizedColumn, ...] = ()

    def read_schema(self) -> pa.Schema:
        """Return the projected schema including synthesized columns."""
        schema = super().read_schema()
        for column in self.synthesized_columns:
            if self.columns is not None and column.name not in self.columns:
                continue
            if schema.get_field_index(column.name) == -1:
                schema = schema.append(pa.field(column.name, column.type))
        return schema

    def create_reader(self) -> OrcFileReader:
        """Create a reader with this scanner's projection and filters."""
        batch_size = (
            self.batch_size
            if self.batch_size is not None
            else _ARROW_DEFAULT_BATCH_SIZE
        )
        return OrcFileReader(
            batch_size=batch_size,
            columns=list(self.columns) if self.columns is not None else None,
            predicate=self.predicate,
            limit=self.limit,
            filesystem=self.filesystem,
            partitioning=self.partitioning,
            ignore_prefixes=self.ignore_prefixes,
            synthesized_columns=self.synthesized_columns,
            schema=self.schema,
        )
