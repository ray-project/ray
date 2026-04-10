import logging
import math
from typing import List, Optional

import pyarrow as pa
import pyarrow.dataset as pds
import pyarrow.parquet as pq
from pyarrow import compute as pc
from pyarrow.fs import FileSystem

from ray.data._internal.datasource_v2.readers.file_reader import (
    _ARROW_DEFAULT_BATCH_SIZE,
    FileFormat,
    FileReader,
)
from ray.data._internal.datasource_v2.readers.in_memory_size_estimator import (
    PARQUET_ENCODING_RATIO_ESTIMATE_DEFAULT,
)
from ray.util.annotations import DeveloperAPI

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
        predicate: Optional[pc.Expression] = None,
        limit: Optional[int] = None,
        filesystem: Optional[FileSystem] = None,
        partitioning: Optional[pds.Partitioning] = None,
        ignore_prefixes: Optional[List[str]] = None,
        target_block_size: Optional[int] = None,
        include_paths: bool = False,
    ):
        """Initialize the Parquet reader.

        Args:
            batch_size: Explicit batch size override. If provided, disables
                adaptive batch sizing.
            columns: Columns to read. None means all columns.
            predicate: PyArrow compute expression for filtering.
            limit: Maximum number of rows to read.
            filesystem: Filesystem for reading files.
            partitioning: PyArrow partitioning for reading files.
            ignore_prefixes: Prefixes to ignore when reading files.
            target_block_size: Target in-memory size per batch in bytes.
                Used for adaptive batch sizing when ``batch_size`` is not set.
            include_paths: If True, include the source file path in a
                ``'path'`` column for each row.
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
        )
        self._explicit_batch_size = batch_size
        self._target_block_size = target_block_size
        self._sampled_batch_size: int | object = (
            _UNSET  # pyrefly: ignore[bad-assignment]
        )

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

    def _on_batch_read(self, table: pa.Table) -> None:
        """Refine batch size estimate from actual in-memory data."""
        if self._target_block_size is None or table.nbytes == 0 or table.num_rows == 0:
            return
        row_size = table.nbytes / table.num_rows
        self._sampled_batch_size = max(math.ceil(self._target_block_size / row_size), 1)
