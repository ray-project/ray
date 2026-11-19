"""``Scanner`` for Iceberg tables: the read state every read task gets.

``IcebergScanner`` is a frozen dataclass that travels to the read tasks. It
holds what PyIceberg's ``ArrowScan`` needs (table metadata, ``FileIO``,
projected schema, row filter) and creates one ``IcebergFileReader`` per task.

It takes column pruning and the limit: pruned columns are reprojected in the
order asked for, and the limit is handed to the reader. It takes no filters: a
Ray filter runs above the read, and filter pushdown is left to a later stack.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

import pyarrow as pa

from ray.data._internal.datasource_v2.formats.iceberg.iceberg_file_reader import (
    IcebergFileReader,
)
from ray.data._internal.datasource_v2.formats.iceberg.iceberg_utils import (
    _get_empty_projection_schema,
)
from ray.data._internal.datasource_v2.interfaces.file_manifest import FileManifest
from ray.data._internal.datasource_v2.interfaces.pushdown import (
    SupportsColumnPruning,
    SupportsLimitPushdown,
)
from ray.data._internal.datasource_v2.interfaces.scanner import Scanner
from ray.util.annotations import DeveloperAPI

if TYPE_CHECKING:
    from pyiceberg.expressions import BooleanExpression
    from pyiceberg.io import FileIO
    from pyiceberg.schema import Schema
    from pyiceberg.table.metadata import TableMetadata


@DeveloperAPI
@dataclass(frozen=True)
class IcebergScanner(
    Scanner[FileManifest],
    SupportsColumnPruning,
    SupportsLimitPushdown,
):
    """What every read task needs to decode an Iceberg table's files.

    ``columns`` and ``limit`` come from the pushdown mixins.
    """

    # Arrow form of ``projected_schema``, before any column pruning.
    schema: pa.Schema
    projected_schema: "Schema"
    table_metadata: "TableMetadata"
    io: "FileIO"
    # ``read_iceberg``'s parsed ``row_filter`` argument.
    row_filter: "BooleanExpression"
    case_sensitive: bool = True

    def _pruned_schema(self) -> "Schema":
        from pyiceberg.schema import Schema

        if self.columns is None:
            return self.projected_schema
        pruned = self.projected_schema.select(
            *self.columns, case_sensitive=self.case_sensitive
        )
        # Keep the requested order; ``Schema.select`` uses schema order.
        return Schema(
            *(
                pruned.find_field(name, case_sensitive=self.case_sensitive)
                for name in self.columns
            ),
            schema_id=pruned.schema_id,
        )

    def read_schema(self) -> pa.Schema:
        from pyiceberg.io.pyarrow import schema_to_pyarrow

        if self.columns is None:
            return self.schema
        # The schema the reader projects to, so the column order matches.
        return schema_to_pyarrow(self._pruned_schema(), include_field_ids=False)

    def create_reader(self) -> IcebergFileReader:
        pruned = self._pruned_schema()
        # PyIceberg returns zero rows for an empty projection (``count()``), so
        # project V1's stub field: no file has it, so it reads as one null per row.
        row_count_only = len(pruned.fields) == 0
        if row_count_only:
            pruned = _get_empty_projection_schema()
        return IcebergFileReader(
            table_metadata=self.table_metadata,
            io=self.io,
            projected_schema=pruned,
            row_filter=self.row_filter,
            case_sensitive=self.case_sensitive,
            limit=self.limit,
            drop_all_columns=row_count_only,
        )
