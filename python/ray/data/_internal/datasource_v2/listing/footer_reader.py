from __future__ import annotations

import functools
import logging
from concurrent.futures import Future, ThreadPoolExecutor, as_completed
from typing import TYPE_CHECKING, Dict, Iterable, Iterator, NamedTuple

import pyarrow as pa
import pyarrow.dataset as pds
import pyarrow.fs as pafs
from pyarrow.parquet import (
    ColumnSchema,
    FileMetaData,
    ParquetFile,
    ParquetSchema,
    RowGroupMetaData,
)

import ray
from ray.data._internal.datasource.parquet_datasource import (
    _row_group_uncompressed_size,
)
from ray.data._internal.datasource_v2.chunkers.parquet_decoded_size import (
    LeafProfile,
    arrow_dictionary_bytes,
    build_leaf_profiles,
    dictionary_decode_size,
    dictionary_page_read_size,
    estimate_row_group_decoded_size,
)
from ray.data._internal.datasource_v2.chunkers.parquet_footer_types import (
    FileChunks,
    RowGroupInfo,
)
from ray.data._internal.datasource_v2.chunkers.parquet_row_group_coalescing import (
    coalesce_row_groups,
)
from ray.data._internal.datasource_v2.chunkers.parquet_size_statistics import (
    DictionaryPage,
    LeafSizeStats,
    read_dictionary_page,
    read_size_statistics,
)
from ray.data._internal.planner.plan_expression.expression_visitors import (
    get_column_references,
)
from ray.data._internal.util import call_with_retry

if TYPE_CHECKING:
    import pyarrow.compute as pc

    from ray.data.expressions import Expr

logger = logging.getLogger(__name__)

# Dictionary pages by row group index, then by leaf index.
_DictionaryPages = Dict[int, Dict[int, DictionaryPage]]

# ``ColumnChunkMetaData.compression`` names to the PyArrow codecs that
# decompress those pages as stored. PyArrow reports parquet-format's LZ4_RAW as
# ``LZ4`` (its writer's ``lz4``) and the deprecated Hadoop-framed LZ4 as
# ``UNKNOWN``, whose pages keep their bound along with LZO's.
_PAGE_CODECS = {
    "SNAPPY": "snappy",
    "GZIP": "gzip",
    "BROTLI": "brotli",
    "ZSTD": "zstd",
    "LZ4": "lz4_raw",
}


def _page_decompressor(compression: str):
    """A ``decompress(values, uncompressed_size)`` for a chunk's pages, or
    ``None`` for a codec this cannot decompress."""
    if compression == "UNCOMPRESSED":
        return lambda values, size: values
    codec = _PAGE_CODECS.get(compression)
    if codec is None:
        return None
    return lambda values, size: pa.decompress(
        values, decompressed_size=size, codec=codec, asbytes=True
    )


def _get_prefix_matches(path: str, names: set[str]) -> set[str]:
    """Return the names that ``path`` equals or is nested under.

    A leaf matches a name outright (a flat column, whose *name* may itself
    contain dots -- ``sepal.length`` is a real one), or as a descendant of it
    (``outer.a`` under ``outer``). Splitting the leaf on ``.`` and comparing the
    first segment gets the nested case right and the dotted-flat-name case
    silently wrong, so match the whole path first.
    """
    return {n for n in names if path == n or path.startswith(f"{n}.")}


def _leaf_matches(leaf_path: str, names: set[str]) -> bool:
    """Whether a Parquet leaf belongs to any of ``names``."""
    return bool(_get_prefix_matches(leaf_path, names))


# Half floats are the odd one out: they ride on FIXED_LEN_BYTE_ARRAY, which
# also carries decimals and UUIDs, so they are only identifiable by logical type.
_FLOAT_PHYSICAL_TYPES = frozenset({"FLOAT", "DOUBLE"})
_FLOAT16_LOGICAL_TYPE = "FLOAT16"


def _is_float_leaf(column: ColumnSchema) -> bool:
    """Whether a Parquet leaf holds floating-point values, and so maybe NaN."""
    if column.physical_type in _FLOAT_PHYSICAL_TYPES:
        return True
    logical_type = column.logical_type
    return logical_type is not None and logical_type.type == _FLOAT16_LOGICAL_TYPE


class _FilterLeaves(NamedTuple):
    """Where the predicate's columns sit in a file's schema.

    A Parquet file has one schema, so this holds for all of its row groups and
    is resolved once per file. ``all_filter_columns_found`` is false when some
    predicate column has no leaf in the schema at all; pruning is then skipped
    because PyArrow would raise on the missing field. ``has_float_leaf`` marks
    a predicate column that may hold NaN -- see ``_rg_can_fully_match``.
    """

    indices: list[int]
    all_filter_columns_found: bool
    has_float_leaf: bool = False


class FooterReader:
    """Reads Parquet footers and chunks files into row-group runs.

    Run as a pool of Ray actors (see :data:`FooterReaderActor`) to spread footer
    IO across the cluster instead of bottlenecking on the driver. ``read_footers``
    is a streaming generator that yields ``FileChunks`` in small batches as their
    footers land, so the driver does far fewer object-store fetches than one per
    file.
    """

    def __init__(
        self,
        filesystem: pafs.FileSystem,
        io_concurrency: int = 128,
        filter_expr: "Expr" | None = None,
        projected_cols: list[str] | None = None,
        coalesce_bytes: int = 0,
        retried_io_errors: list[str] | None = None,
    ):
        from ray.data.context import DEFAULT_RETRIED_IO_ERRORS

        self.retried_io_errors = (
            retried_io_errors
            if retried_io_errors is not None
            else list(DEFAULT_RETRIED_IO_ERRORS)
        )
        # Coalescing target: merge contiguous row groups into chunks of
        # ~coalesce_bytes decoded before returning them, so the driver packs
        # fewer items. 0 disables coalescing (one chunk per physical row group).
        self.coalesce_bytes = coalesce_bytes
        self.filesystem = filesystem
        self.pool = ThreadPoolExecutor(max_workers=io_concurrency)
        # Match Arrow's process-wide pools to the actor's IO concurrency so
        # nested S3/footer work isn't bottlenecked on the default 8 threads.
        pa.set_io_thread_count(io_concurrency)
        # Lower the Ray Data predicate to a native PyArrow compute expression once
        # (per actor) so it can be pushed into split_by_row_group for row-group
        # pruning. ``None`` means "no predicate" -> keep every row group.
        self.filter: "pc.Expression" | None = (
            filter_expr.to_pyarrow() if filter_expr is not None else None
        )
        # Top-level columns the predicate reads. A row group whose value in any
        # of them may be a null or a NaN cannot be an exact-survivor count --
        # see ``_rg_can_fully_match``.
        self.filter_columns: set[str] = (
            set(get_column_references(filter_expr))
            if filter_expr is not None
            else set()
        )
        # Top-level columns a read task will decode, which row-group byte sizes
        # are accounted over. The scanner is handed the projection alone, but it
        # still decodes every predicate column to evaluate the filter, so the
        # union is what a task fetches. Sizing on the
        # projection alone lets a large non-projected filter column overfill read
        # tasks, the failure this path exists to prevent. ``None`` means "all
        # columns", which already covers both.
        self.read_columns: set[str] | None = (
            set(projected_cols) | self.filter_columns
            if projected_cols is not None
            else None
        )
        # Reused across files; make_fragment is what lets us apply
        # split_by_row_group.
        self.file_format = pds.ParquetFileFormat()

    def _read_leaf_indices(self, row_group: RowGroupMetaData) -> list[int] | None:
        # Map the top-level column names a read task decodes to Parquet
        # leaf-column indices (a nested field expands to several leaves, e.g.
        # "a.b.list.element"). Returns None to signal "all columns" so callers
        # can take the cheap path.
        if self.read_columns is None:
            return None
        indices = [
            j
            for j in range(row_group.num_columns)
            if _leaf_matches(row_group.column(j).path_in_schema, self.read_columns)
        ]

        return indices or None

    def _row_group_info(
        self,
        row_group: RowGroupMetaData,
        rg_idx: int,
        leaf_indices: list[int] | None,
        leaf_profiles: list[LeafProfile] | None,
        size_stats: list[LeafSizeStats | None] | None,
        fully_matched: bool = False,
        dictionary_pages: dict[int, DictionaryPage] | None = None,
    ) -> RowGroupInfo:
        # Sum per-column sizes on both paths -- with a projection, only the
        # leaves the reader decodes, so bin packing reflects the bytes it will
        # actually pull. Deliberately not ``row_group.total_byte_size``, which
        # is one cheap accessor but can report the *compressed* size for some
        # files (apache/arrow#48138); undersizing bins here overfills read
        # tasks, which is the failure this whole path exists to avoid. Shares
        # the V1 helper so the three call sites cannot drift.
        uncompressed = _row_group_uncompressed_size(row_group, leaf_indices)
        # The decoded size is what a read task's Arrow block actually costs, so
        # it is what the bin budget wants. ``None`` (the estimator's answer to
        # ``leaf_profiles`` of ``None``, i.e. a schema it does not model) leaves
        # consumers on the uncompressed value; the uncompressed number is
        # recorded either way so both stay comparable.
        decoded = estimate_row_group_decoded_size(
            row_group, leaf_profiles, leaf_indices, size_stats, dictionary_pages
        )
        return RowGroupInfo(
            rg_idx=rg_idx,
            uncompressed_size=uncompressed,
            num_rows=row_group.num_rows,
            fully_matched=fully_matched,
            decoded_size=decoded,
        )

    def _locate_filter_columns(self, schema: ParquetSchema) -> _FilterLeaves:
        # Resolve the predicate's column names to Parquet leaf-column indices,
        # the same mapping ``_read_leaf_indices`` does for the read columns.
        # Called once per file so the per-row-group null check below is an
        # index lookup instead of another scan over every leaf. Leaf order is
        # the schema's, so these index a row group's columns just as well.
        # Read off the schema rather than a row group because a leaf's logical
        # type lives there, whereas a row group only exposes it via statistics
        # -- which a file need not carry.
        if not self.filter_columns:
            return _FilterLeaves([], True)
        indices: list[int] = []
        matched: set[str] = set()
        has_float_leaf = False
        for j in range(len(schema)):
            column = schema.column(j)
            hits = _get_prefix_matches(column.path, self.filter_columns)
            if hits:
                matched.update(hits)
                indices.append(j)
                has_float_leaf = has_float_leaf or _is_float_leaf(column)
        return _FilterLeaves(indices, matched == self.filter_columns, has_float_leaf)

    def _has_filter_nulls(
        self, row_group: RowGroupMetaData, indices: list[int]
    ) -> bool:
        """Whether any predicate column may have a null in this row group.

        Parquet min/max statistics are computed over non-null values only, so
        bounds alone can never rule out nulls. A null in a predicate column
        means that row satisfies neither the filter nor its negation, so the
        group's ``num_rows`` is not an exact survivor count.

        Missing or null-count-less statistics are treated as "may contain
        nulls", i.e. not fully matched -- the safe direction.
        """
        for j in indices:
            stats = row_group.column(j).statistics
            if stats is None or not stats.has_null_count or stats.null_count > 0:
                return True
        return False

    def _rg_can_fully_match(
        self, row_group: RowGroupMetaData, filter_leaves: _FilterLeaves
    ) -> bool:
        """Whether this row group's ``num_rows`` can be an exact survivor count.

        Three conditions must all hold: every predicate column is present in
        the schema, no predicate column is floating point, and none of those
        columns has a null. A missing column is unverified, not null-free --
        treating it as null-free is what lets a partial survivor count as exact.

        Floats are excluded because Parquet min/max statistics skip NaN, so a
        NaN row is invisible to the bounds the negation test reasons over: a
        group of ``[35.0, 40.0, NaN]`` reports min 35 / max 40, which makes
        ``~(id >= 30)`` look impossible even though the NaN row does satisfy it
        and does not survive the filter. No footer field counts NaNs, so a float
        predicate column can never be cleared -- ``num_rows`` would over-count
        and limit push-down would stop early.
        """
        if not filter_leaves.all_filter_columns_found or filter_leaves.has_float_leaf:
            return False
        return not self._has_filter_nulls(row_group, filter_leaves.indices)

    def _try_split_by_row_group(
        self, fragment, expr: "pc.Expression", path: str
    ) -> list[int] | None:
        """Row-group ids that statistics say may match ``expr``, or None on failure.

        ``None`` means pruning cannot be applied, so the caller must fail closed
        (keep every group / treat none as fully matched). Swallows the
        exception so one mismatched file cannot abort a ``read_footers`` batch.
        """
        try:
            return [sub.row_groups[0].id for sub in fragment.split_by_row_group(expr)]
        except Exception as e:
            logger.debug(
                "Error splitting by row group: %s for file %s",
                e,
                path,
                exc_info=True,
            )
            return None

    def _read_dictionary_pages(
        self,
        path: str,
        metadata: FileMetaData,
        rg_indices: Iterable[int],
        leaf_indices: list[int] | None,
        leaf_profiles: list[LeafProfile],
        size_stats: list[list[LeafSizeStats | None]] | None,
    ) -> _DictionaryPages:
        """The dictionary pages the estimator wants, at one read each.

        The footer sizes an Arrow dictionary column's dictionary only to within
        its index pages; its dictionary page header, a few dozen bytes at a
        footer-recorded offset, makes it exact. A view column's footer cannot
        say how many values fit inside their views; its dictionary page, up to
        about 1 MiB, can. A dictionary-encoded string column whose footer lacks
        its values' bytes -- one with no SizeStatistics -- is sized from its
        page's header. Only those leaves ask (see
        ``dictionary_page_read_size``), so files without them pay nothing. An
        Arrow dictionary chunk whose writer fell back to PLAIN partway is also
        decoded whole -- one more read, of at most 16 MiB -- for the dictionary
        the read will build (see ``dictionary_decode_size``). Any failure
        returns what was read so far: a missing page only loosens that column
        back to its bound, which must not fail the read.
        """
        leaves = range(len(leaf_profiles)) if leaf_indices is None else leaf_indices
        wanted = []
        for rg_idx in rg_indices:
            row_group = metadata.row_group(rg_idx)
            for leaf_idx in leaves:
                chunk = row_group.column(leaf_idx)
                profile = leaf_profiles[leaf_idx]
                stats = None if size_stats is None else size_stats[rg_idx][leaf_idx]
                nbytes = dictionary_page_read_size(profile, chunk, stats)
                if nbytes:
                    wanted.append(
                        (
                            rg_idx,
                            leaf_idx,
                            chunk.dictionary_page_offset,
                            nbytes,
                            chunk.compression,
                            dictionary_decode_size(profile, chunk, stats) > 0,
                        )
                    )
        pages: _DictionaryPages = {}
        if not wanted:
            return pages
        try:
            with call_with_retry(
                lambda: self.filesystem.open_input_file(path),
                description=f"open {path} for dictionary pages",
                match=self.retried_io_errors,
            ) as f:
                for rg_idx, leaf_idx, offset, nbytes, compression, decode in wanted:
                    buf = call_with_retry(
                        functools.partial(f.read_at, nbytes, offset),
                        description=f"read a dictionary page of {path}",
                        match=self.retried_io_errors,
                    )
                    page = read_dictionary_page(buf, _page_decompressor(compression))
                    if page is None:
                        continue
                    if decode:
                        page = page._replace(
                            decoded_dictionary_bytes=self._decoded_dictionary_bytes(
                                f, metadata, rg_idx, leaf_idx, path
                            )
                        )
                    pages.setdefault(rg_idx, {})[leaf_idx] = page
        except Exception as e:
            logger.debug(
                "Error reading dictionary pages: %s for file %s",
                e,
                path,
                exc_info=True,
            )
        return pages

    def _decoded_dictionary_bytes(
        self,
        f: pa.NativeFile,
        metadata: FileMetaData,
        rg_idx: int,
        leaf_idx: int,
        path: str,
    ) -> int | None:
        """The Arrow dictionary a read of one leaf chunk builds, by decoding it.

        Reads only that chunk. ``None`` on failure, which keeps the chunk at its
        bound without dropping the other pages.
        """
        try:
            reader = ParquetFile(f, metadata=metadata).reader
            table = call_with_retry(
                functools.partial(
                    reader.read_row_group,
                    rg_idx,
                    column_indices=[leaf_idx],
                    use_threads=False,
                ),
                description=f"decode a dictionary column chunk of {path}",
                match=self.retried_io_errors,
            )
        except Exception as e:
            logger.debug(
                "Error decoding a dictionary column chunk: %s for file %s",
                e,
                path,
                exc_info=True,
            )
            return None
        return arrow_dictionary_bytes(table.column(0))

    def _read_and_chunk(self, path: str, size: int) -> FileChunks:
        fragment = self.file_format.make_fragment(path, filesystem=self.filesystem)
        # ``make_fragment`` is lazy, so this property is the footer read itself
        # (and caches it on the fragment). It raises on a truncated, non-Parquet,
        # or since-deleted file -- terminal conditions that must fail the read
        # rather than silently drop the file's rows, since the reader would fail
        # on the same file anyway.
        metadata = call_with_retry(
            lambda: fragment.metadata,
            description=f"read Parquet footer for {path}",
            match=self.retried_io_errors,
        )
        # Both column look-ups are schema-derived, and a Parquet file has a
        # single schema, so resolve them once rather than rescanning every leaf
        # for each group. (``metadata.schema`` builds a fresh wrapper per access,
        # hence the local.) The read-column lookup reads leaf paths off row group
        # 0, which a file with no row groups has none of; it sizes nothing either
        # way.
        schema = metadata.schema
        filter_leaves = self._locate_filter_columns(schema)
        leaf_indices = (
            self._read_leaf_indices(metadata.row_group(0))
            if metadata.num_row_groups
            else None
        )

        if self.filter is not None and filter_leaves.all_filter_columns_found:
            # Predicate pushdown: drop row groups whose Parquet statistics
            # contradict the filter, so we never emit chunks the reader would
            # skip anyway. Each returned fragment views a single surviving group.
            # Fail closed on any split error -- keep every group -- so a type
            # mismatch or similar cannot abort the rest of the read_footers batch.
            surviving: list[int] | None = self._try_split_by_row_group(
                fragment, self.filter, path
            )
            rg_indices: Iterable[int] = (
                range(metadata.num_row_groups) if surviving is None else surviving
            )
            # Classify surviving groups as fully- vs partially-matching via
            # predicate negation: a group is fully matched iff
            # ~filter cannot match any of its rows, i.e. the negation prunes it
            # out. For those, num_rows is an exact survivor count and can drive
            # the limit push-down. Any failure defaults to "not fully matched",
            # which is always safe (falls back to the post-filter stop).
            not_fully_ids = self._try_split_by_row_group(fragment, ~self.filter, path)
            not_fully = set(rg_indices) if not_fully_ids is None else set(not_fully_ids)
            # The negation test alone is not sufficient: ``~filter`` can prune a
            # group that still holds a row the filter does not return, and then
            # ``num_rows`` over-counts and limit push-down stops early. Nulls do
            # this under three-valued logic and NaNs because min/max skip them,
            # so ``_rg_can_fully_match`` rules both out from the footer instead
            # -- which holds however clever the engine's statistics pruning gets.
            # PyArrow declines the null prune today but already makes the NaN one.
            fully_by_idx: dict[int, bool] = {
                i: i not in not_fully
                and self._rg_can_fully_match(metadata.row_group(i), filter_leaves)
                for i in rg_indices
            }
        elif self.filter is not None:
            # A predicate column is absent from this file's schema. PyArrow's
            # split_by_row_group raises ArrowInvalid ("No match for FieldRef.Name")
            # in that case, which would abort the whole read_footers batch.
            # Skip pruning: keep every row group and let the reader apply the
            # filter after null-fill. None can be an exact survivor.
            rg_indices = range(metadata.num_row_groups)
            fully_by_idx = dict.fromkeys(rg_indices, False)
        else:
            rg_indices = range(metadata.num_row_groups)
            # No predicate -> nothing to disqualify a group, so the lookup below
            # falls through to its fully-matched default for every group.
            fully_by_idx = {}

        # One Thrift walk per file over the footer bytes ``metadata`` already
        # holds -- no extra IO, though ``__reduce__`` does re-serialize the
        # footer, so skip it when the predicate pruned every row group. ``None``
        # when the walk failed its cross-check, which leaves the estimator its
        # footer rules alone, as for a writer that emitted no SizeStatistics.
        # The leaf profiles hoist the estimator's schema-derived facts --
        # including the Arrow schema conversion -- out of the per-row-group loop.
        size_stats = read_size_statistics(metadata) if rg_indices else None
        leaf_profiles = build_leaf_profiles(schema) if rg_indices else None
        dictionary_pages = (
            self._read_dictionary_pages(
                path, metadata, rg_indices, leaf_indices, leaf_profiles, size_stats
            )
            if leaf_profiles is not None
            else {}
        )

        per_rg = [
            self._row_group_info(
                row_group=metadata.row_group(rg_idx),
                rg_idx=rg_idx,
                leaf_indices=leaf_indices,
                leaf_profiles=leaf_profiles,
                size_stats=size_stats[rg_idx] if size_stats is not None else None,
                fully_matched=fully_by_idx.get(rg_idx, True),
                dictionary_pages=dictionary_pages.get(rg_idx),
            )
            for rg_idx in rg_indices
        ]
        # Coalesce contiguous row groups into ~coalesce_bytes chunks (no-op when
        # coalesce_bytes == 0) so only a handful of descriptors per file reach the
        # driver's bin-packer instead of one per physical row group.
        row_groups = coalesce_row_groups(per_rg, self.coalesce_bytes)
        return FileChunks(path=path, size=size, row_groups=row_groups)

    @ray.method(num_returns="streaming")
    def read_footers(
        self,
        files: list[tuple[str, int]],
        *,
        result_batch_size: int = 1,
        preserve_order: bool = False,
    ) -> Iterator[list[FileChunks]]:
        """Read the footers of ``files`` concurrently, yielding ``FileChunks``.

        Yields lists of ``FileChunks`` (not single results): each list the driver
        receives costs one object-store fetch, so batching cuts driver-side
        deserialization overhead ~``result_batch_size``-fold. At the default of
        ``1`` a directory of N files costs N fetches on the single listing task;
        callers set it via ``RAY_DATA_PARQUET_FOOTER_RESULT_BATCH_SIZE``.
        ``num_returns`` is fixed to ``"streaming"`` via ``@ray.method`` so
        results stream out as footers land.

        ``preserve_order`` yields in ``files`` order rather than completion order.
        The reads are all submitted up front either way, so this delays only the
        yield of a footer that landed behind a slower one, never the read itself.
        """
        futures = [
            self.pool.submit(self._read_and_chunk, path, size) for path, size in files
        ]
        # Completion order depends on IO timing, which decides how the driver's
        # bin packer groups row groups into read tasks and -- under a pushed-down
        # limit -- which files it reads before stopping. Walking ``futures``
        # directly makes both a function of the listing order instead.
        ordered: Iterable[Future[FileChunks]] = (
            futures if preserve_order else as_completed(futures)
        )
        buffer: list[FileChunks] = []
        total_row_groups = 0
        for finished in ordered:
            chunk = finished.result()
            if not chunk.row_groups:
                # Only occurs if the file is empty or all row groups are filtered out.
                continue
            total_row_groups += len(chunk.row_groups)
            buffer.append(chunk)
            if len(buffer) >= result_batch_size:
                yield buffer
                buffer = []
        if buffer:
            yield buffer
        logger.debug(
            "FooterReader batch: %d files, %d row groups",
            len(files),
            total_row_groups,
        )


FooterReaderActor = ray.remote(FooterReader)
