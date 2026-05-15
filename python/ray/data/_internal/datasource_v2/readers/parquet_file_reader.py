import logging
import math
from typing import TYPE_CHECKING, Iterator, List, Optional

import pyarrow as pa
import pyarrow.dataset as pds
import pyarrow.parquet as pq
from pyarrow.fs import FileSystem
from typing_extensions import override

if TYPE_CHECKING:
    from ray.data.datasource.partitioning import Partitioning

from ray.data._internal.datasource_v2.readers.file_reader import (
    _ARROW_DEFAULT_BATCH_SIZE,
    FileFormat,
    FileReader,
)
from ray.data._internal.datasource_v2.readers.in_memory_size_estimator import (
    PARQUET_ENCODING_RATIO_ESTIMATE_DEFAULT,
)
from ray.data.expressions import Expr
from ray.util.annotations import DeveloperAPI
from ray.util.debug import log_once

logger = logging.getLogger(__name__)

_UNSET = object()


def _estimate_batch_size_from_metadata(
    fragment: pds.ParquetFileFragment,
    columns: Optional[List[str]],
    target_block_size: int,
) -> Optional[int]:
    """Estimate batch size from Parquet row group metadata without reading data.

    Uses uncompressed column sizes from row group metadata and the encoding
    ratio to estimate in-memory row size, then computes how many rows fit
    in ``target_block_size``.

    Args:
        fragment: A PyArrow Parquet fragment with accessible metadata.
        columns: Columns being read, or None for all columns.
        target_block_size: Target in-memory size per batch in bytes.

    Returns:
        Estimated batch size in rows, or None if metadata is unavailable.
    """
    try:
        # Accessing `metadata` triggers I/O via `EnsureCompleteMetadata()` to
        # read the Parquet footer. `check_status` maps C++ Status codes to
        # Python exceptions: IOError (OSError) for I/O failures,
        # ArrowInvalid for corrupt footers.
        # https://github.com/apache/arrow/blob/apache-arrow-23.0.0/python/pyarrow/error.pxi#L110-L126
        metadata: pq.FileMetaData = fragment.metadata
    except (OSError, pa.ArrowInvalid) as e:
        logger.debug("Failed to read Parquet metadata for batch size estimation: %s", e)
        return None

    if metadata is None or metadata.num_row_groups == 0:
        return None

    row_group_idx: int = (
        fragment.row_groups[0].id if fragment.row_groups is not None else 0
    )
    row_group_meta: pq.RowGroupMetaData = metadata.row_group(row_group_idx)
    row_group_num_rows: int = row_group_meta.num_rows

    if row_group_num_rows == 0:
        return None

    if columns is not None:
        projected_columns = tuple(columns)
        target_column_indices = []
        for col_idx in range(row_group_meta.num_columns):
            leaf_path = row_group_meta.column(col_idx).path_in_schema
            # Account for nested columns
            if any(
                leaf_path == col_name or leaf_path.startswith(f"{col_name}.")
                for col_name in projected_columns
            ):
                target_column_indices.append(col_idx)
        row_group_uncompressed_size = sum(
            row_group_meta.column(col_idx).total_uncompressed_size
            for col_idx in target_column_indices
        )
    else:
        # Sum per-column uncompressed sizes instead of using
        # row_group_meta.total_byte_size, which can return the *compressed* size
        # for some files (apache/arrow#48138).
        row_group_uncompressed_size = sum(
            row_group_meta.column(col_idx).total_uncompressed_size
            for col_idx in range(row_group_meta.num_columns)
        )

    # Estimate the in-memory size of the row group
    estimated_in_mem_row_group_size = (
        row_group_uncompressed_size * PARQUET_ENCODING_RATIO_ESTIMATE_DEFAULT
    )

    estimated_in_mem_row_size = estimated_in_mem_row_group_size / row_group_num_rows
    if estimated_in_mem_row_size == 0:
        return None

    # Never request more rows than the row group actually contains.
    target_batch_size = min(
        math.ceil(target_block_size / estimated_in_mem_row_size),
        row_group_num_rows,
    )

    logger.debug(
        f"Estimated target batch size to be: {target_batch_size} (with "
        f"{target_block_size=} bytes and {estimated_in_mem_row_size=} bytes)"
    )

    return target_batch_size


@DeveloperAPI
class ParquetFileReader(FileReader):
    """Parquet-specific file reader with adaptive batch sizing.

    Extends :class:`FileReader` with:

    - **Metadata-based batch size estimation**: Uses Parquet row group metadata
      (uncompressed column sizes) to estimate an optimal batch size before
      reading any data.
    - **Adaptive refinement**: After reading each batch, refines the batch size
      estimate from actual in-memory sizes for subsequent reads.

    For non-Parquet formats, use :class:`FileReader` directly.
    """

    def __init__(
        self,
        batch_size: Optional[int] = None,
        columns: Optional[List[str]] = None,
        predicate: Optional[Expr] = None,
        limit: Optional[int] = None,
        filesystem: Optional[FileSystem] = None,
        partitioning: "Optional[Partitioning]" = None,
        ignore_prefixes: Optional[List[str]] = None,
        target_block_size: Optional[int] = None,
        include_paths: bool = False,
        include_row_hash: bool = False,
        schema: Optional[pa.Schema] = None,
    ):
        """Initialize the Parquet reader.

        Args:
            batch_size: Explicit batch size override. If provided, disables
                adaptive batch sizing.
            columns: Columns to read. None means all columns.
            predicate: Ray Data expression for filtering.
            limit: Maximum number of rows to read.
            filesystem: Filesystem for reading files.
            partitioning: Ray ``Partitioning`` for synthesizing partition
                columns from file paths.
            ignore_prefixes: Prefixes to ignore when reading files.
            target_block_size: Target in-memory size per batch in bytes.
                Used for adaptive batch sizing when ``batch_size`` is not set.
            include_paths: If True, include the source file path in a
                ``'path'`` column for each row.
            include_row_hash: If True, include a deterministic uint64 hash
                per row in a ``'row_hash'`` column.
            schema: Caller-supplied unified schema forwarded to the base
                :class:`FileReader` for per-fragment inference override
                and partition-column type casting.
        """
        super().__init__(
            format=FileFormat.PARQUET,
            batch_size=batch_size or _ARROW_DEFAULT_BATCH_SIZE,
            columns=columns,
            predicate=predicate,
            limit=limit,
            filesystem=filesystem,
            partitioning=partitioning,
            ignore_prefixes=ignore_prefixes,
            include_paths=include_paths,
            include_row_hash=include_row_hash,
            schema=schema,
        )
        self._explicit_batch_size = batch_size
        self._target_block_size = target_block_size
        self._sampled_batch_size: int | object = (
            _UNSET  # pyrefly: ignore[bad-assignment]
        )

    @override
    def _resolve_batch_size(self, dataset: pds.Dataset) -> int:
        """Determine batch size from explicit setting, metadata, or default.

        Priority: explicit batch_size > sampled estimate > metadata estimate > default.

        On the first call ``_sampled_batch_size`` is ``_UNSET``, so we fall
        through to the metadata estimate and seed ``_sampled_batch_size`` with
        the result.  ``_on_batch_read`` later refines it from actual data, and
        subsequent ``read()`` calls on the same instance use the refined value.
        """
        if self._explicit_batch_size is not None:
            return self._explicit_batch_size

        if self._sampled_batch_size is not _UNSET:
            return self._sampled_batch_size  # pyrefly: ignore[bad-return]

        batch_size = _ARROW_DEFAULT_BATCH_SIZE
        if self._target_block_size is not None:
            first_fragment = next(dataset.get_fragments(), None)
            if first_fragment is not None:
                estimated = _estimate_batch_size_from_metadata(
                    first_fragment, self._columns, self._target_block_size
                )
                if estimated is not None:
                    logger.debug(
                        "Estimated Parquet batch size: %d rows (target_block_size=%d)",
                        estimated,
                        self._target_block_size,
                    )
                    batch_size = estimated

        self._sampled_batch_size = batch_size
        return batch_size

    @override
    def _on_batch_read(self, table: pa.Table) -> None:
        """Refine batch size estimate from actual in-memory data."""
        if self._target_block_size is None or table.nbytes == 0 or table.num_rows == 0:
            return
        row_size = table.nbytes / table.num_rows
        self._sampled_batch_size = max(math.ceil(self._target_block_size / row_size), 1)

    @override
    def _iter_fragment_tables(
        self,
        fragment: pds.Fragment,
        scanner_kwargs: dict,
    ) -> "Iterator[pa.Table]":
        """Use V1's nested-type fallback path when the fragment has nested
        columns whose row-group size exceeds Arrow's ~2GB chunking limit
        (ARROW-5030).
        """
        import pyarrow.compute as pc

        from ray.data._internal.arrow_ops.transform_pyarrow import (
            _align_struct_fields,
        )
        from ray.data._internal.datasource.parquet_datasource import (
            _get_safe_batch_size_for_nested_types,
            _needs_nested_type_fallback,
            _resolve_leaf_column_indices,
            _resolve_read_columns,
        )
        from ray.data._internal.planner.plan_expression.expression_visitors import (
            get_column_references,
        )

        columns = scanner_kwargs.get("columns")
        filter_expr: pc.Expression = scanner_kwargs.get("filter")
        # Include filter-referenced columns in the fallback check: a filter
        # that touches a large nested column outside the projection still
        # forces row-level decoding of that column, which would otherwise
        # hit ARROW-5030 in the normal scanner path.
        filter_columns = (
            get_column_references(self._predicate)
            if self._predicate is not None
            else None
        )
        read_columns = _resolve_read_columns(columns, filter_expr, filter_columns)
        if not _needs_nested_type_fallback(fragment, read_columns):
            yield from super()._iter_fragment_tables(fragment, scanner_kwargs)
            return

        if log_once(f"parquet_nested_fallback_v2:{fragment.path}"):
            logger.warning(
                "Using pyarrow.parquet row-level batched reader for '%s' due "
                "to Arrow nested type chunking limitation (ARROW-5030). "
                "Consider writing Parquet files with smaller row group sizes "
                "to avoid this.",
                fragment.path,
            )

        batch_size = scanner_kwargs.get("batch_size")

        pf = pq.ParquetFile(
            fragment.path,
            filesystem=fragment.filesystem,  # pyrefly: ignore[unexpected-keyword]
        )

        # Scope the safe batch-size calculation to the columns actually being
        # decoded so we don't shrink batches based on columns we won't read.
        leaf_indices = (
            _resolve_leaf_column_indices(pf.metadata, read_columns)
            if read_columns is not None and pf.metadata.num_row_groups > 0
            else None
        )
        safe_batch_size = _get_safe_batch_size_for_nested_types(pf, leaf_indices)
        fallback_batch_size = (
            min(batch_size, safe_batch_size) if batch_size else safe_batch_size
        )

        # Apply row-group-level predicate pushdown via fragment.subset; the
        # row-level filter is applied per-batch below since iter_batches
        # doesn't accept a filter expression. Under schema evolution the
        # filter may reference a column absent from this fragment's
        # physical schema — fragment.subset uses that schema (not the
        # unified one) and raises ArrowInvalid, so skip row-group pruning
        # in that case and let the per-batch filter (post null-fill) do
        # all the row-dropping.
        fragment_physical_columns = set(fragment.physical_schema.names)
        filter_touches_missing_column = filter_columns is not None and any(
            c not in fragment_physical_columns for c in filter_columns
        )
        if filter_expr is not None and not filter_touches_missing_column:
            subset = fragment.subset(filter=filter_expr)
        else:
            subset = fragment
        row_groups = (
            [rg.id for rg in subset.row_groups]
            if subset.row_groups is not None
            else None
        )
        if row_groups is not None and len(row_groups) == 0:
            return

        # ``pq.ParquetFile.iter_batches`` returns batches with the fragment's
        # physical schema, so the fallback path would otherwise emit tables
        # that differ from the scanner path (which pins
        # ``_file_dataset_schema``) in struct field order, integer width,
        # or missing columns. Align + cast to the same unified schema so
        # fallback and non-fallback fragments concat cleanly downstream.
        # Scoped to ``columns`` (not ``read_columns``) since filter-only
        # columns are projected away before alignment.
        file_dataset_schema = self._file_dataset_schema
        if file_dataset_schema is not None and columns is not None:
            align_schema = pa.schema(
                [
                    file_dataset_schema.field(c)
                    for c in columns
                    if file_dataset_schema.get_field_index(c) != -1
                ]
            )
        else:
            align_schema = file_dataset_schema

        # Under schema evolution a filter-referenced column may live in
        # the unified dataset schema but be absent from this fragment.
        # The scanner path null-fills such columns via dataset-level
        # schema pinning; ``pq.ParquetFile.iter_batches`` silently drops
        # them and then ``table.filter(filter_expr)`` raises
        # ``ArrowInvalid: No match for FieldRef.Name``. Mirror the
        # scanner: append a null column of the unified type before the
        # filter evaluates, so ``null > 15`` resolves to false and the
        # fragment contributes 0 rows.
        columns_to_null_fill: List[str] = (
            [c for c in read_columns if c not in fragment_physical_columns]
            if read_columns is not None
            else []
        )
        null_fill_type_by_column = {
            column_name: (
                file_dataset_schema.field(column_name).type
                if file_dataset_schema is not None
                and file_dataset_schema.get_field_index(column_name) != -1
                else pa.null()
            )
            for column_name in columns_to_null_fill
        }

        for batch in pf.iter_batches(
            batch_size=fallback_batch_size,
            columns=read_columns,
            use_threads=False,
            row_groups=row_groups,
        ):
            table = pa.Table.from_batches([batch])
            for column_name in columns_to_null_fill:
                if column_name not in table.column_names:
                    table = table.append_column(
                        column_name,
                        pa.nulls(
                            table.num_rows,
                            type=null_fill_type_by_column[column_name],
                        ),
                    )
            if filter_expr is not None:
                table = table.filter(filter_expr)
                # Skip downstream select/align/cast on fully-filtered
                # batches — the caller discards empty tables anyway.
                if table.num_rows == 0:
                    continue
            if columns is not None:
                table = table.select([c for c in columns if c in table.column_names])
            if align_schema is not None:
                table = _align_struct_fields([table], align_schema)[0].cast(
                    align_schema
                )
            yield table

    @override
    def _arrow_scanner_kwargs(self) -> dict:
        # pre_buffer=True (pyarrow default) holds a whole fragment's worth of
        # decoded column chunks resident before yielding batches, so
        # pa.total_allocated_bytes() climbs monotonically across batches and
        # peaks near full fragment size. Disabling pre_buffer with
        # use_buffered_stream caps peak near a small multiple of one row group
        # while keeping throughput equal to the default. batch_readahead=1
        # (inherited from FileReader base kwargs) plus fragment_readahead=1
        # is enough to keep decode pipelined. See apache/arrow#39808.
        kwargs: dict = {
            "fragment_scan_options": pds.ParquetFragmentScanOptions(
                pre_buffer=False,
                use_buffered_stream=True,
            ),
            "fragment_readahead": 1,
        }
        return kwargs
