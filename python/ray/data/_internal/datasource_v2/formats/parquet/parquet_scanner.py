import os
from dataclasses import dataclass, field
from typing import Any, Dict, Optional, Tuple

import pyarrow as pa
from pyarrow.fs import LocalFileSystem

from ray.data._internal.datasource_v2.common.arrow_file_scanner import ArrowFileScanner
from ray.data._internal.datasource_v2.common.file_reader import (
    _resolve_file_dataset_schema,
)
from ray.data._internal.datasource_v2.formats.parquet.listing_pruning import (
    HivePartitioningSnapshot,
    ListingPruningContext,
    logical_schema_signature,
)
from ray.data._internal.datasource_v2.formats.parquet.parquet_file_reader import (
    ParquetFileReader,
)
from ray.data._internal.datasource_v2.formats.parquet.parquet_utils import (
    check_for_legacy_tensor_type,
)
from ray.data._internal.datasource_v2.interfaces.synthesized_columns import (
    SynthesizedColumn,
)
from ray.data._internal.util import RetryingPyFileSystem, RetryingPyFileSystemHandler
from ray.data.datasource.partitioning import Partitioning, PartitionStyle
from ray.util.annotations import DeveloperAPI


def _is_builtin_local_filesystem(filesystem) -> bool:
    """Recognize the ordinary public-API retry wrapper without removing it."""
    if filesystem is None or type(filesystem) is LocalFileSystem:
        return True
    if type(filesystem) is not RetryingPyFileSystem:
        return False
    handler = filesystem.handler
    return (
        type(handler) is RetryingPyFileSystemHandler
        and type(handler.unwrap()) is LocalFileSystem
    )


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
    # Columns the reader appends to every batch instead of reading them
    # (``PathColumn`` for ``include_paths``, ``RowHashColumn`` for
    # ``include_row_hash``).
    synthesized_columns: Tuple[SynthesizedColumn, ...] = ()
    # Extra kwargs forwarded to ``pds.ParquetFileFormat(**kwargs)`` inside
    # the per-task ``ParquetFileReader`` (e.g. ``coerce_int96_timestamp_unit``,
    # ``pre_buffer``, ``dictionary_columns``). Carries the deprecated
    # ``dataset_kwargs`` payload from ``read_parquet`` to the worker.
    parquet_format_kwargs: Dict[str, Any] = field(default_factory=dict)

    def listing_pruning_context(self) -> Optional[ListingPruningContext]:
        # A derived scanner (e.g. a catalog source) is not a Hive-path source
        # merely because its reader also happens to read Parquet.
        if type(self) is not ParquetScanner:
            return None
        if type(self.synthesized_columns) is not tuple or self.synthesized_columns:
            return None
        if type(self.parquet_format_kwargs) is not dict or self.parquet_format_kwargs:
            return None
        if not _is_builtin_local_filesystem(self.filesystem):
            return None
        scheme = self.partitioning
        if type(scheme) is not Partitioning:
            return None
        if type(scheme.style) not in (str, PartitionStyle):
            return None
        if scheme.style != PartitionStyle.HIVE:
            return None
        if not _is_builtin_local_filesystem(scheme.filesystem):
            return None
        if type(scheme.base_dir) is not str or (
            scheme.base_dir and not os.path.isabs(scheme.base_dir)
        ):
            return None
        if type(scheme.field_names) not in (list, tuple) or not scheme.field_names:
            return None
        if any(type(name) is not str for name in scheme.field_names):
            return None
        if type(scheme.field_types) is not dict:
            return None
        field_types = []
        for name, kind in scheme.field_types.items():
            if type(name) is not str or name not in scheme.field_names:
                return None
            if kind is int:
                tag = "int64"
            elif kind is bool:
                tag = "bool"
            elif kind is str:
                tag = "string"
            else:
                return None
            field_types.append((name, tag))
        signature = logical_schema_signature(self.schema)
        if signature is None:
            return None
        if len(set(scheme.field_names)) != len(scheme.field_names):
            return None
        if not set(scheme.field_names).issubset(dict(signature)):
            return None
        if any(dict(signature)[name] == "float64" for name in scheme.field_names):
            return None
        primitive_tags = {
            pa.float64(): "float64",
            pa.int8(): "int8",
            pa.int16(): "int16",
            pa.int32(): "int32",
            pa.int64(): "int64",
            pa.bool_(): "bool",
            pa.string(): "string",
        }
        if self.columns is not None and (
            type(self.columns) not in (list, tuple)
            or any(type(name) is not str for name in self.columns)
        ):
            return None
        pin = _resolve_file_dataset_schema(self.schema, scheme.field_names, ())
        if pin is None:
            return None
        return ListingPruningContext(
            schema=self.schema,
            file_schema=pin,
            file_schema_signature=tuple((f.name, primitive_tags[f.type]) for f in pin),
            partitioning=HivePartitioningSnapshot(
                base_dir=scheme.base_dir,
                field_names=tuple(scheme.field_names),
                field_types=tuple(field_types),
            ),
        )

    def read_schema(self) -> pa.Schema:
        """Return schema after column pruning and tensor check.

        Synthesized columns (``path``, ``row_hash``) are appended post-read by
        the file reader, but only for columns listed in ``self.columns`` (see
        ``file_reader.read``'s ``columns_to_synthesize`` filter). When a
        projection has pruned a synthesized column away, advertising it
        here would put the schema out of sync with the actual blocks — so
        only append when no projection is active or when it survives.
        """
        schema = super().read_schema()
        for column in self.synthesized_columns:
            if self.columns is not None and column.name not in self.columns:
                continue
            if schema.get_field_index(column.name) != -1:
                continue
            schema = schema.append(pa.field(column.name, column.type))

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
            synthesized_columns=self.synthesized_columns,
            schema=self.schema,
            parquet_format_kwargs=dict(self.parquet_format_kwargs),
        )
