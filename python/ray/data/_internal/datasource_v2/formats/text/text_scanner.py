"""The text ``Scanner``: the read's options plus the optimizer's pushdowns."""

from dataclasses import dataclass
from typing import Optional, Tuple

import pyarrow as pa
from pyarrow.fs import FileSystem
from typing_extensions import override

from ray.data._internal.datasource_v2.common.file_pruners import (
    PartitionPredicatePruner,
)
from ray.data._internal.datasource_v2.common.file_scanner import FileScanner
from ray.data._internal.datasource_v2.formats.text.text_reader import TextReader
from ray.data._internal.datasource_v2.interfaces.file_pruner import FilePruner
from ray.data._internal.datasource_v2.interfaces.pushdown import (
    SupportsColumnPruning,
    SupportsLimitPushdown,
)
from ray.data._internal.datasource_v2.interfaces.synthesized_columns import (
    SynthesizedColumn,
)
from ray.util.annotations import DeveloperAPI


@DeveloperAPI
@dataclass(frozen=True)
class TextScanner(FileScanner, SupportsColumnPruning, SupportsLimitPushdown):
    """Scanner for plain-text files on DataSource V2.

    Supports column pruning, a per-task row limit and partition pruning. Filter
    pushdown is not claimed: the reader decodes lines in Python, not through
    ``pyarrow.dataset``, so this scanner must not extend ``ArrowFileScanner``.
    """

    schema: pa.Schema
    encoding: str = "utf-8"
    drop_empty_lines: bool = True
    filesystem: Optional[FileSystem] = None
    synthesized_columns: Tuple[SynthesizedColumn, ...] = ()

    @override
    def _create_partition_pruner(self) -> Optional[FilePruner]:
        """Prune by path values; a file whose path lacks a key is left to the reader."""
        if self.partitioning is None or self.partition_predicate is None:
            return None
        return PartitionPredicatePruner(
            self.partitioning,
            self.partition_predicate,
            schema=self.schema,
            keep_unknown=True,
        )

    @override
    def read_schema(self) -> pa.Schema:
        """Return the dataset schema after column pruning."""
        if self.columns is None:
            return self.schema
        fields = []
        for name in self.columns:
            idx = self.schema.get_field_index(name)
            assert idx >= 0, f"Column {name} not found in schema"
            fields.append(self.schema.field(idx))
        return pa.schema(fields)

    @override
    def create_reader(self) -> TextReader:
        """Create a reader with this scanner's projection, limit and partition filter."""
        return TextReader(
            encoding=self.encoding,
            drop_empty_lines=self.drop_empty_lines,
            columns=list(self.columns) if self.columns is not None else None,
            limit=self.limit,
            filesystem=self.filesystem,
            partitioning=self.partitioning,
            partition_predicate=self.partition_predicate,
            synthesized_columns=self.synthesized_columns,
            schema=self.schema,
        )
