from dataclasses import dataclass
from typing import Tuple

import pyarrow as pa

from ray.data._internal.datasource_v2.common.arrow_file_scanner import (
    ArrowFileScanner,
)
from ray.data._internal.datasource_v2.common.file_reader import (
    _ARROW_DEFAULT_BATCH_SIZE,
    FileFormat,
)
from ray.data._internal.datasource_v2.formats.orc.orc_file_reader import OrcFileReader
from ray.data._internal.datasource_v2.interfaces.synthesized_columns import (
    SynthesizedColumn,
)
from ray.util.annotations import DeveloperAPI


@DeveloperAPI
@dataclass(frozen=True)
class OrcScanner(ArrowFileScanner):
    """Configure a file-level ORC scan through PyArrow Dataset."""

    synthesized_columns: Tuple[SynthesizedColumn, ...] = ()

    def read_schema(self) -> pa.Schema:
        """Return the projected schema including synthesized columns."""
        synthesized_by_name = {
            column.name: column for column in self.synthesized_columns
        }

        if self.columns is None:
            schema = self.schema
            for column in self.synthesized_columns:
                field = pa.field(column.name, column.type)
                index = schema.get_field_index(column.name)
                if index == -1:
                    schema = schema.append(field)
                elif schema.field(index).type != column.type:
                    schema = schema.set(index, field)
            return schema

        fields = []
        for name in self.columns:
            column = synthesized_by_name.get(name)
            if column is not None:
                # The reader replaces any same-named on-disk field, so the
                # logical schema must advertise the synthesized type too.
                fields.append(pa.field(name, column.type))
                continue

            index = self.schema.get_field_index(name)
            assert index >= 0, f"Column {name} not found in schema"
            fields.append(self.schema.field(index))
        return pa.schema(fields)

    def create_reader(self) -> OrcFileReader:
        """Create a reader with this scanner's projection and filters."""
        batch_size = (
            self.batch_size
            if self.batch_size is not None
            else _ARROW_DEFAULT_BATCH_SIZE
        )
        # FileReader appends synthesized columns after scanning, then uses
        # ``columns`` to restore the logical schema order. Keep that ordering
        # when the caller has not pushed down a projection.
        columns = (
            list(self.columns)
            if self.columns is not None
            else list(self.read_schema().names)
        )
        return OrcFileReader(
            format=FileFormat.ORC,
            batch_size=batch_size,
            columns=columns,
            predicate=self.predicate,
            limit=self.limit,
            filesystem=self.filesystem,
            partitioning=self.partitioning,
            ignore_prefixes=self.ignore_prefixes,
            synthesized_columns=self.synthesized_columns,
            schema=self.schema,
        )
