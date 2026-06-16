"""File chunkers for DataSourceV2.

A ``FileChunker`` decides how a single listed file is split into one or
more parallel-read units. The indexer drives the chunker once per file
and emits one manifest row per chunk; downstream the partitioner /
reader carry the per-chunk metadata through to the read task.
"""

import abc
import logging
import math
from typing import (
    TYPE_CHECKING,
    Dict,
    Iterable,
    Optional,
    Set,
    Tuple,
    Type,
    TypedDict,
    TypeVar,
    cast,
    get_type_hints,
)

from ray.data._internal.util import GiB, MiB, infer_compression
from ray.util.annotations import DeveloperAPI

if TYPE_CHECKING:
    import pyarrow as pa
    from pyarrow.fs import FileSystem

logger = logging.getLogger(__name__)


# Width (bytes) of the int32 offset buffer Arrow allocates per element of a
# variable-width column (string / binary / list).
_ARROW_OFFSET_BYTES = 4


def _fixed_arrow_byte_width(arrow_type: "pa.DataType") -> Optional[float]:
    """Per-value in-memory byte width for a *fixed-width* Arrow type.

    Returns ``None`` for variable-width / nested types (string, binary, list,
    map, struct, dictionary, extension, ...). Booleans are bit-packed, so they
    report ``1/8`` byte per value.
    """
    import pyarrow as pa

    if pa.types.is_boolean(arrow_type):
        return 1 / 8
    if pa.types.is_fixed_size_binary(arrow_type):
        return float(arrow_type.byte_width)
    if (
        pa.types.is_integer(arrow_type)
        or pa.types.is_floating(arrow_type)
        or pa.types.is_temporal(arrow_type)
        or pa.types.is_decimal(arrow_type)
    ):
        bit_width = getattr(arrow_type, "bit_width", None)
        if bit_width:
            return bit_width / 8
    return None


def estimate_chunk_in_memory_size(
    arrow_schema: "pa.Schema",
    num_rows: int,
    uncompressed_by_column: Dict[str, int],
    var_width_factor: float,
    projected_columns: Optional[Set[str]] = None,
) -> int:
    """Estimate the Arrow in-memory size (bytes) of a chunk from footer data.

    This is the type-aware estimate that absorbs cross-file compression *and*
    encoding variance for the common analytic case:

    * **Fixed-width** columns (int / float / temporal / decimal / bool /
      fixed-size-binary): ``num_rows × byte_width`` — exact and independent of
      compression codec *and* Parquet encoding (dictionary / RLE / delta).
    * **Variable-width / nested** columns (string / binary / list / map /
      struct): the column's uncompressed page bytes ``× var_width_factor``
      plus an int32 offset buffer. This is the only term that remains
      approximate (a dictionary-encoded low-cardinality string still
      under-counts; ``var_width_factor`` is the conservative knob).

    A validity bitmap (``ceil(num_rows / 8)``) is added per nullable column.

    When ``projected_columns`` is given, only those top-level columns are
    counted — so the estimate reflects a column projection pushed down to the
    read (see the v2c projection-aware sizing path).
    """
    import pyarrow as pa  # noqa: F401  (ensures pa.types is importable)

    total = 0.0
    for field in arrow_schema:
        if projected_columns is not None and field.name not in projected_columns:
            continue
        width = _fixed_arrow_byte_width(field.type)
        if width is not None:
            total += num_rows * width
        else:
            total += uncompressed_by_column.get(field.name, 0) * var_width_factor
            total += num_rows * _ARROW_OFFSET_BYTES
        if field.nullable:
            total += (num_rows + 7) // 8
    return int(total)


class ChunkMetadata(TypedDict):
    """Base interface for chunk metadata types."""

    pass


_ChunkMetadataT = TypeVar("_ChunkMetadataT", bound=ChunkMetadata)


def create_chunk_metadata(cls: Type[_ChunkMetadataT], **kwargs) -> _ChunkMetadataT:
    """Create a metadata instance with validation, ensure the keys are correct."""
    required_keys = list(get_type_hints(cls).keys())

    missing_keys = [key for key in required_keys if key not in kwargs]
    if missing_keys:
        raise ValueError(f"Missing required keys: {missing_keys}")

    extra_keys = [key for key in kwargs if key not in required_keys]
    if extra_keys:
        raise ValueError(f"Unexpected keys: {extra_keys}")

    return cast(_ChunkMetadataT, kwargs)


class LineDelimitedFileChunkMetadata(ChunkMetadata):
    """Metadata for line-delimited file chunks."""

    chunk_byte_start_idx: int
    chunk_byte_end_idx: int


class ParquetFileChunkMetadata(ChunkMetadata):
    """Metadata for Parquet file chunks.

    A chunk is an explicit, half-open range of consecutive row groups
    ``[row_group_start, row_group_end)`` within a single file, computed at
    listing time from the file's footer. The reader slices the fragment to
    exactly this range — no estimation or read-time reconciliation.

    ``in_memory_size`` is the chunk's footer-derived Arrow in-memory size
    estimate (see :func:`estimate_chunk_in_memory_size`), carried through the
    manifest so :class:`FooterDerivedInMemorySizeEstimator` can size partitions
    without a global encoding-ratio guess.
    """

    row_group_start: int  # inclusive
    row_group_end: int  # exclusive
    in_memory_size: int  # footer-derived Arrow in-memory estimate (bytes)


class ByteEstimateParquetFileChunkMetadata(ChunkMetadata):
    """Metadata for legacy byte-estimate Parquet chunks.

    The chunker splits a file into ``total_num_chunks`` equal byte ranges
    *without* reading the footer; ``chunk_idx`` identifies this chunk. The
    reader reconciles the (possibly over-estimated) chunk index to a real
    row-group range at read time. Produced by
    :class:`ByteEstimateParquetFileChunker`, retained behind
    ``DataContext.parquet_chunker_row_group_aware=False`` for A/B experiments
    against the row-group-aware chunker.
    """

    chunk_idx: int
    total_num_chunks: int


@DeveloperAPI
class FileChunker(abc.ABC):
    """Abstract base class for chunking files into smaller pieces for parallel processing.

    File chunkers determine how large files should be split into chunks that can be
    processed in parallel. Different file formats may require different chunking strategies.

    For example:
    - Line-delimited files (JSONL, CSV) can be chunked by byte ranges
    - Parquet files can be chunked by row groups
    """

    # Whether ``generate_chunk_metadatas`` performs file I/O (e.g. reading a
    # Parquet footer). When True, the indexer fans chunking across its thread
    # pool so the per-file reads parallelize even for a single input
    # directory. When False, the indexer chunks inline (no thread hand-off).
    reads_file_metadata: bool = False

    @abc.abstractmethod
    def generate_chunk_metadatas(
        self,
        path: str,
        file_size: int,
        filesystem: Optional["FileSystem"] = None,
    ) -> Iterable[Tuple[Optional[ChunkMetadata], int]]:
        """Generate metadata for file chunks.

        Args:
            path: The file path being chunked.
            file_size: The total size in bytes of the file to be chunked.
            filesystem: PyArrow filesystem used to read per-file metadata
                (e.g. the Parquet footer). Ignored by chunkers that do not
                read file metadata.

        Returns:
            An iterable of tuples containing (metadata, chunk_size) where metadata
            describes the chunk and chunk_size is the size of the chunk in bytes.
            Metadata can be None for chunks that don't require metadata
            (e.g., whole file processing).
        """
        ...


@DeveloperAPI
class WholeFileChunker(FileChunker):
    """File chunker that treats the whole file as a single chunk.

    This chunker is used when files should be processed as a single unit,
    typically for smaller files or when the file format doesn't support
    efficient chunking (e.g., compressed files).

    Yields a single chunk with no metadata, indicating the entire file
    should be processed as one unit.
    """

    def generate_chunk_metadatas(
        self,
        path: str,
        file_size: int,
        filesystem: Optional["FileSystem"] = None,
    ) -> Iterable[Tuple[Optional[ChunkMetadata], int]]:
        yield None, file_size


@DeveloperAPI
class LineDelimitedFileChunker(FileChunker):
    """File chunker for line-delimited files (JSONL, CSV, TSV, etc.).

    This chunker splits files into fixed-size byte chunks (default: 256 MiB)
    and provides metadata about the byte ranges for each chunk. The actual
    line boundaries are handled by the reader to ensure complete records.
    """

    _CHUNK_BYTE_SIZE = 256 * MiB  # 256 MiB

    def generate_chunk_metadatas(
        self,
        path: str,
        file_size: int,
        filesystem: Optional["FileSystem"] = None,
    ) -> Iterable[Tuple[Optional[ChunkMetadata], int]]:
        compression = infer_compression(path)
        if compression is not None:
            yield None, file_size
        else:
            num_chunks = math.ceil(file_size / self._CHUNK_BYTE_SIZE)
            for chunk_idx in range(num_chunks):
                chunk_start = self._CHUNK_BYTE_SIZE * chunk_idx
                chunk_end = min(self._CHUNK_BYTE_SIZE * (chunk_idx + 1), file_size)
                chunk_size = chunk_end - chunk_start
                yield (
                    create_chunk_metadata(
                        LineDelimitedFileChunkMetadata,
                        chunk_byte_start_idx=chunk_start,
                        chunk_byte_end_idx=chunk_end,
                    ),
                    chunk_size,
                )


@DeveloperAPI
class ParquetFileChunker(FileChunker):
    """File chunker for Parquet files.

    Reads each file's footer at listing time and chunks on **true row-group
    boundaries**: consecutive row groups are bundled into a chunk until the
    bundle's on-disk size reaches ``target_chunk_size`` (always at least one
    row group per chunk). Each chunk carries an explicit half-open row-group
    range, so the reader slices to exactly those row groups with no
    estimation or read-time reconciliation, and the listing stage never
    produces empty read tasks.

    The row group is Parquet's atomic read unit, so a chunk can never be
    smaller than a single row group. With the default target (which falls
    back to ``DataContext.target_min_block_size``), a file's row groups map
    1:1 to chunks unless they are smaller than the target, in which case
    consecutive small row groups are bundled to avoid an excessive number of
    tiny chunks.
    """

    # Hard fallback used only when neither an explicit target nor the
    # DataContext size knobs are set.
    _FALLBACK_TARGET_CHUNK_SIZE = 1 * MiB

    # Footer reads are file I/O — let the indexer parallelize them.
    reads_file_metadata: bool = True

    def __init__(
        self,
        target_chunk_size: Optional[int] = None,
        *,
        projected_columns: Optional[Iterable[str]] = None,
    ):
        from ray.data.context import DataContext

        ctx = DataContext.get_current()
        self._target_chunk_size = (
            target_chunk_size
            or ctx.parquet_chunker_target_chunk_size
            or ctx.target_min_block_size
            or self._FALLBACK_TARGET_CHUNK_SIZE
        )
        self._var_width_factor = ctx.parquet_in_memory_var_width_factor
        # Top-level column names the read projects to (``None`` = all columns).
        # When set, the footer-derived in-memory estimate counts only these
        # columns so partition sizing reflects the projection (v2c). Populated
        # by ``plan_list_files_op`` from ``ListFiles.projected_columns``.
        self._projected_columns: Optional[Set[str]] = (
            set(projected_columns) if projected_columns is not None else None
        )

    @property
    def projected_columns(self) -> Optional[Set[str]]:
        return self._projected_columns

    @projected_columns.setter
    def projected_columns(self, columns: Optional[Iterable[str]]) -> None:
        self._projected_columns = set(columns) if columns is not None else None

    def generate_chunk_metadatas(
        self,
        path: str,
        file_size: int,
        filesystem: Optional["FileSystem"] = None,
    ) -> Iterable[Tuple[Optional[ChunkMetadata], int]]:
        import pyarrow.parquet as pq

        try:
            # Reads only the Parquet footer (file metadata), not data.
            metadata = pq.read_metadata(path, filesystem=filesystem)
        except Exception as e:
            # Corrupt / unreadable footer (or a non-Parquet file that slipped
            # through). Fall back to a single whole-file chunk so the file is
            # still read rather than dropped.
            logger.debug(
                "Could not read Parquet footer for chunking (%s): %s; "
                "falling back to a whole-file chunk.",
                path,
                e,
            )
            yield None, file_size
            return

        num_row_groups = metadata.num_row_groups
        if num_row_groups == 0:
            yield None, file_size
            return

        # Arrow schema (column types) for the type-aware in-memory estimate.
        # Best-effort: if it can't be derived, in-memory estimation is skipped
        # for this file and the estimator falls back to the on-disk × ratio.
        try:
            arrow_schema = metadata.schema.to_arrow_schema()
        except Exception:
            arrow_schema = None

        def _emit(
            start: int, end: int, on_disk_size: int, rows: int, unc: Dict[str, int]
        ):
            in_memory = (
                estimate_chunk_in_memory_size(
                    arrow_schema,
                    rows,
                    unc,
                    self._var_width_factor,
                    self._projected_columns,
                )
                if arrow_schema is not None
                else on_disk_size
            )
            return (
                create_chunk_metadata(
                    ParquetFileChunkMetadata,
                    row_group_start=start,
                    row_group_end=end,
                    in_memory_size=in_memory,
                ),
                on_disk_size,
            )

        # Greedily bundle consecutive row groups until the running on-disk
        # size reaches the target. Always emit at least one row group per
        # chunk (the atomic read unit). Alongside the on-disk size, accumulate
        # the row count and per-column uncompressed bytes the in-memory
        # estimate needs.
        start = 0
        running_size = 0
        running_rows = 0
        running_unc: Dict[str, int] = {}
        for rg_idx in range(num_row_groups):
            rg_meta = metadata.row_group(rg_idx)
            # On-disk (compressed) row-group size. ``RowGroupMetaData`` exposes
            # only the *uncompressed* ``total_byte_size``; the on-disk size lives
            # on each ``ColumnChunkMetaData``, so sum the per-column compressed
            # sizes. Keeping chunk sizes in on-disk units matches the manifest's
            # ``file_sizes`` and the ``×encoding_ratio`` in-memory estimator.
            rg_size = 0
            rg_unc: Dict[str, int] = {}
            for c in range(rg_meta.num_columns):
                col = rg_meta.column(c)
                rg_size += col.total_compressed_size
                # Top-level (root) column name for this leaf column chunk.
                root = col.path_in_schema.split(".", 1)[0]
                rg_unc[root] = rg_unc.get(root, 0) + col.total_uncompressed_size

            if running_size > 0 and running_size + rg_size > self._target_chunk_size:
                yield _emit(start, rg_idx, running_size, running_rows, running_unc)
                start = rg_idx
                running_size = 0
                running_rows = 0
                running_unc = {}

            running_size += rg_size
            running_rows += rg_meta.num_rows
            for root, unc in rg_unc.items():
                running_unc[root] = running_unc.get(root, 0) + unc

        # Flush the final bundle.
        yield _emit(start, num_row_groups, running_size, running_rows, running_unc)


@DeveloperAPI
class ByteEstimateParquetFileChunker(FileChunker):
    """Legacy byte-estimate Parquet chunker (pre-row-group-aware).

    Splits a file into ``ceil(file_size / target_chunk_size)`` equal byte
    ranges *without* reading the footer, so the chunk count is an estimate
    that the reader reconciles to real row groups at read time (ignoring any
    over-estimated chunks). Retained behind
    ``DataContext.parquet_chunker_row_group_aware=False`` so experiments can
    A/B it against :class:`ParquetFileChunker`. Emits
    :class:`ByteEstimateParquetFileChunkMetadata`.
    """

    # Large default so a high compression ratio doesn't pack too many row
    # groups into one read task and OOM. The row-group-aware chunker sizes
    # precisely instead; this estimate stays coarse on purpose.
    _DEFAULT_TARGET_CHUNK_SIZE = 1 * GiB

    # Pure byte arithmetic — no footer read — so the indexer chunks inline.
    reads_file_metadata: bool = False

    def __init__(self, target_chunk_size: Optional[int] = None):
        from ray.data.context import DataContext

        ctx = DataContext.get_current()
        if target_chunk_size is not None:
            self._target_chunk_size = target_chunk_size
        elif ctx.parquet_chunker_target_chunk_size is not None:
            self._target_chunk_size = ctx.parquet_chunker_target_chunk_size
        else:
            self._target_chunk_size = self._DEFAULT_TARGET_CHUNK_SIZE

    def generate_chunk_metadatas(
        self,
        path: str,
        file_size: int,
        filesystem: Optional["FileSystem"] = None,
    ) -> Iterable[Tuple[Optional[ChunkMetadata], int]]:
        if file_size <= self._target_chunk_size:
            # Whole-file chunk: avoids read-time metadata fetching since the
            # whole file is read anyway.
            yield None, file_size
            return

        num_chunks = math.ceil(file_size / self._target_chunk_size)
        for i in range(num_chunks):
            chunk_start = self._target_chunk_size * i
            chunk_end = min(self._target_chunk_size * (i + 1), file_size)
            chunk_size = chunk_end - chunk_start
            yield (
                create_chunk_metadata(
                    ByteEstimateParquetFileChunkMetadata,
                    chunk_idx=i,
                    total_num_chunks=num_chunks,
                ),
                chunk_size,
            )
