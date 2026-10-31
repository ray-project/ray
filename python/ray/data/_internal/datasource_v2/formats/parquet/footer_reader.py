from __future__ import annotations

import functools
import logging
from concurrent.futures import Future, ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from typing import (
    TYPE_CHECKING,
    AbstractSet,
    Any,
    Dict,
    Iterable,
    Iterator,
    Mapping,
    NamedTuple,
    Tuple,
)

import pyarrow as pa
import pyarrow.dataset as pds
import pyarrow.fs as pafs
from pyarrow.parquet import ColumnSchema, FileMetaData, ParquetSchema, RowGroupMetaData

import ray
from ray.data._internal.datasource_v2.common.file_reader import casts_to_schema
from ray.data._internal.datasource_v2.formats.parquet.parquet_decoded_size import (
    LeafProfile,
    added_bytes,
    build_leaf_profiles,
    dictionary_page_read_size,
    estimate_row_group_decoded_size,
)
from ray.data._internal.datasource_v2.formats.parquet.parquet_file_chunking_utils import (
    _row_group_unit_id,
)
from ray.data._internal.datasource_v2.formats.parquet.parquet_row_group_coalescing import (
    coalesce_row_groups,
)
from ray.data._internal.datasource_v2.formats.parquet.parquet_size_estimator import (
    PARQUET_ENCODING_RATIO_ESTIMATE_DEFAULT,
)
from ray.data._internal.datasource_v2.formats.parquet.parquet_size_statistics import (
    DictionaryPage,
    LeafSizeStats,
    read_dictionary_page,
    read_size_statistics,
)
from ray.data._internal.datasource_v2.formats.parquet.parquet_utils import (
    _row_group_uncompressed_size,
)
from ray.data._internal.datasource_v2.interfaces.file_indexer import FileInfo
from ray.data._internal.datasource_v2.interfaces.file_manifest import FileChunk
from ray.data._internal.planner.plan_expression.expression_visitors import (
    get_column_references,
)
from ray.data._internal.util import call_with_retry
from ray.data.checkpoint.generated_id import GENERATED_ID_COLUMN_TYPE

if TYPE_CHECKING:
    import pyarrow.compute as pc

    from ray.data.expressions import Expr

logger = logging.getLogger(__name__)

# Dictionary pages by row group index, then by leaf index.
_DictionaryPages = Dict[int, Dict[int, DictionaryPage]]


@dataclass(frozen=True)
class ChunkedFile:
    """The footer-derived runs of row groups for a single file."""

    file: FileInfo  # path and on-disk size, from the file listing
    row_groups: Tuple[FileChunk, ...]


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
    is a streaming generator that yields ``ChunkedFile`` in small batches as their
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
        excluded_read_unit_ids: AbstractSet[str] | None = None,
        format_kwargs: Mapping[str, Any] | None = None,
        read_schema: pa.Schema | None = None,
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
        # Read unit ids a checkpoint already finished (``"<path>#rg<N>"`` for a
        # row group). Those row groups are left out of every ``ChunkedFile``.
        self.excluded_read_unit_ids: AbstractSet[str] = (
            excluded_read_unit_ids or frozenset()
        )
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
        # The ``ParquetFileFormat`` kwargs the read tasks pass; their type
        # options change what the estimator sizes.
        self.format_kwargs = dict(format_kwargs or {})
        # The dataset schema: the read tasks cast every batch to it, when
        # ``casts_to_schema`` says they do, and append its columns a file lacks.
        self.read_schema = read_schema
        self.pinned_schema = read_schema if casts_to_schema(read_schema) else None

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

    def _row_group_run(
        self,
        row_group: RowGroupMetaData,
        rg_idx: int,
        leaf_indices: list[int] | None,
        leaf_profiles: list[LeafProfile] | None,
        size_stats: list[LeafSizeStats | None] | None,
        fully_matched: bool = False,
        dictionary_pages: dict[int, DictionaryPage] | None = None,
        added: tuple[float, int] = (0, 0),
    ) -> FileChunk:
        # The run's size is the Arrow bytes a read task decodes it to, which is
        # what the bin budget bounds. With a projection, only the leaves the
        # reader decodes count.
        decoded = estimate_row_group_decoded_size(
            row_group=row_group,
            leaf_profiles=leaf_profiles,
            leaf_indices=leaf_indices,
            size_stats=size_stats,
            dictionary_pages=dictionary_pages,
            format_kwargs=self.format_kwargs,
            added=added,
        )
        if decoded is None:
            # A schema the estimator does not model: the uncompressed size times
            # the fixed encoding ratio. Summed per column rather than read off
            # ``row_group.total_byte_size``, which can report the *compressed*
            # size for some files (apache/arrow#48138); undersizing bins
            # overfills read tasks. Shares the V1 helper so the three call
            # sites cannot drift.
            uncompressed = _row_group_uncompressed_size(
                rg_meta=row_group, column_indices=leaf_indices
            )
            decoded = uncompressed * PARQUET_ENCODING_RATIO_ESTIMATE_DEFAULT
        return FileChunk(
            unit_ids=(rg_idx,),
            num_rows=row_group.num_rows,
            size_bytes=decoded,
            fully_matched=fully_matched,
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
        """The dictionary page headers the estimator asks for, one read each.

        A failed read keeps the pages read so far: a missing one only loosens a bound.
        """
        leaves = range(len(leaf_profiles)) if leaf_indices is None else leaf_indices
        wanted = []
        for rg_idx in rg_indices:
            row_group = metadata.row_group(rg_idx)
            for leaf_idx in leaves:
                chunk = row_group.column(leaf_idx)
                profile = leaf_profiles[leaf_idx]
                stats = None if size_stats is None else size_stats[rg_idx][leaf_idx]
                nbytes = dictionary_page_read_size(
                    profile=profile, chunk=chunk, stats=stats
                )
                if nbytes:
                    wanted.append(
                        (rg_idx, leaf_idx, chunk.dictionary_page_offset, nbytes)
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
                for rg_idx, leaf_idx, offset, nbytes in wanted:
                    buf = call_with_retry(
                        functools.partial(f.read_at, nbytes, offset),
                        description=f"read a dictionary page of {path}",
                        match=self.retried_io_errors,
                    )
                    page = read_dictionary_page(buf)
                    if page is not None:
                        pages.setdefault(rg_idx, {})[leaf_idx] = page
        except Exception as e:
            logger.debug(
                "Error reading dictionary pages: %s for file %s",
                e,
                path,
                exc_info=True,
            )
        return pages

    def _added_bytes(
        self, fragment: pds.ParquetFileFragment, path: str
    ) -> tuple[float, int]:
        """:func:`added_bytes` of the columns a read task appends to this file.

        The dataset schema's columns the file lacks or holds as nulls, less those the
        projection drops; it never drops the checkpoint ID.
        """
        if self.read_schema is None:
            return 0, 0
        try:
            # Cached on the fragment by the footer read.
            file_schema = fragment.physical_schema
        except Exception:
            return 0, 0
        added = []
        for field in self.read_schema:
            idx = file_schema.get_field_index(field.name)
            if pa.types.is_null(field.type) or (
                idx >= 0 and not pa.types.is_null(file_schema.field(idx).type)
            ):
                continue
            if (
                self.read_columns is None
                or field.name in self.read_columns
                or field.type == GENERATED_ID_COLUMN_TYPE
            ):
                added.append(field)
        return added_bytes(added=added, path=path)

    def _read_and_chunk(self, file: FileInfo) -> ChunkedFile:
        path = file.path
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
        unprocessed_rg_ids = [
            i
            for i in range(metadata.num_row_groups)
            if _row_group_unit_id(path, i) not in self.excluded_read_unit_ids
        ]
        if len(unprocessed_rg_ids) < metadata.num_row_groups:
            # Some row groups of this file are already checkpointed. Narrow the
            # fragment to the rest now, so that (1) the predicate split below
            # never emits a chunk for a finished row group, so the resumed job
            # does not read it again, and (2) the finished row groups' rows are
            # not summed toward a pushed-down limit, which would otherwise make
            # the listing stop early and the resumed job return too few rows.
            fragment = fragment.subset(row_group_ids=unprocessed_rg_ids)

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
                unprocessed_rg_ids if surviving is None else surviving
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
            rg_indices = unprocessed_rg_ids
            fully_by_idx = dict.fromkeys(rg_indices, False)
        else:
            rg_indices = unprocessed_rg_ids
            # No predicate -> nothing to disqualify a group, so the lookup below
            # falls through to its fully-matched default for every group.
            fully_by_idx = {}

        # One Thrift walk per file over the footer bytes ``metadata`` already
        # holds -- no extra IO, though ``__reduce__`` re-serializes the footer,
        # so skip it when every row group was pruned. The leaf profiles hoist
        # the schema-derived facts, the Arrow schema conversion among them, out
        # of the per-row-group loop.
        size_stats = read_size_statistics(metadata) if rg_indices else None
        leaf_profiles = (
            build_leaf_profiles(
                parquet_schema=schema,
                pinned_schema=self.pinned_schema,
                format_kwargs=self.format_kwargs,
                file_metadata=metadata,
            )
            if rg_indices
            else None
        )
        added = (
            self._added_bytes(fragment=fragment, path=path) if rg_indices else (0, 0)
        )
        dictionary_pages = (
            self._read_dictionary_pages(
                path=path,
                metadata=metadata,
                rg_indices=rg_indices,
                leaf_indices=leaf_indices,
                leaf_profiles=leaf_profiles,
                size_stats=size_stats,
            )
            if leaf_profiles is not None
            else {}
        )

        per_rg = [
            self._row_group_run(
                row_group=metadata.row_group(rg_idx),
                rg_idx=rg_idx,
                leaf_indices=leaf_indices,
                leaf_profiles=leaf_profiles,
                size_stats=size_stats[rg_idx] if size_stats is not None else None,
                fully_matched=fully_by_idx.get(rg_idx, True),
                dictionary_pages=dictionary_pages.get(rg_idx),
                added=added,
            )
            for rg_idx in rg_indices
        ]
        # Coalesce contiguous row groups into ~coalesce_bytes chunks (no-op when
        # coalesce_bytes == 0) so only a handful of descriptors per file reach the
        # driver's bin-packer instead of one per physical row group.
        row_groups = coalesce_row_groups(per_rg, self.coalesce_bytes)
        return ChunkedFile(file=file, row_groups=row_groups)

    @ray.method(num_returns="streaming")
    def read_footers(
        self,
        files: list[FileInfo],
        *,
        result_batch_size: int = 1,
        preserve_order: bool = False,
    ) -> Iterator[list[ChunkedFile]]:
        """Read the footers of ``files`` concurrently, yielding ``ChunkedFile``.

        Yields lists of ``ChunkedFile`` (not single results): each list the driver
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
        futures = [self.pool.submit(self._read_and_chunk, file) for file in files]
        # Completion order depends on IO timing, which decides how the driver's
        # bin packer groups row groups into read tasks and -- under a pushed-down
        # limit -- which files it reads before stopping. Walking ``futures``
        # directly makes both a function of the listing order instead.
        ordered: Iterable[Future[ChunkedFile]] = (
            futures if preserve_order else as_completed(futures)
        )
        buffer: list[ChunkedFile] = []
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
