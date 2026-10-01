from dataclasses import dataclass, field
from typing import Any, Dict, Optional, Tuple

import pyarrow as pa

from ray.data._internal.datasource_v2.common.arrow_file_scanner import ArrowFileScanner
from ray.data._internal.datasource_v2.formats.parquet.parquet_file_reader import (
    ParquetFileReader,
)
from ray.data._internal.datasource_v2.formats.parquet.parquet_utils import (
    check_for_legacy_tensor_type,
)
from ray.data._internal.datasource_v2.interfaces.synthesized_columns import (
    SynthesizedColumn,
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
    # Columns the reader appends to every batch instead of reading them
    # (``PathColumn`` for ``include_paths``, ``RowHashColumn`` for
    # ``include_row_hash``).
    synthesized_columns: Tuple[SynthesizedColumn, ...] = ()
    # Extra kwargs forwarded to ``pds.ParquetFileFormat(**kwargs)`` inside
    # the per-task ``ParquetFileReader`` (e.g. ``coerce_int96_timestamp_unit``,
    # ``pre_buffer``, ``dictionary_columns``). Carries the deprecated
    # ``dataset_kwargs`` payload from ``read_parquet`` to the worker.
    parquet_format_kwargs: Dict[str, Any] = field(default_factory=dict)

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
