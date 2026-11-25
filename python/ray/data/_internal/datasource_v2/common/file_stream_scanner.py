"""``FileStreamScanner``: the shared scanner for :class:`FileStreamReader` formats.

It carries the read's options and the optimizer's pushdowns to every read
task. A format subclasses it, adds its own option fields and implements
:meth:`create_reader` with :meth:`_reader_kwargs`.
"""

from dataclasses import dataclass, field
from typing import Any, Dict, Optional, Tuple

import pyarrow as pa
from pyarrow.fs import FileSystem
from typing_extensions import override

from ray.data._internal.datasource_v2.common.file_pruners import (
    PartitionPredicatePruner,
)
from ray.data._internal.datasource_v2.common.file_scanner import FileScanner
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
class FileStreamScanner(FileScanner, SupportsColumnPruning, SupportsLimitPushdown):
    """Scanner for formats read whole from a byte stream.

    Supports column pruning, a per-task row limit and partition pruning.
    Filter pushdown is not claimed: the reader decodes files in Python, not
    through ``pyarrow.dataset``, so this scanner must not extend
    ``ArrowFileScanner``. A file whose path lacks a partition key is kept at
    listing and the reader evaluates the predicate on nulls before opening it.
    """

    schema: pa.Schema
    filesystem: Optional[FileSystem] = None
    open_stream_args: Dict[str, Any] = field(default_factory=dict)
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

    def _reader_kwargs(self) -> Dict[str, Any]:
        """The ``FileStreamReader`` arguments this scanner's pushdowns and options
        resolve to; a subclass's :meth:`create_reader` adds its own."""
        return dict(
            filesystem=self.filesystem,
            open_stream_args=dict(self.open_stream_args),
            columns=list(self.columns) if self.columns is not None else None,
            limit=self.limit,
            partitioning=self.partitioning,
            partition_predicate=self.partition_predicate,
            synthesized_columns=self.synthesized_columns,
            schema=self.schema,
        )
