from __future__ import annotations

import logging
from concurrent.futures import Future, ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from typing import TYPE_CHECKING, AbstractSet, Iterable, Iterator, NamedTuple, Tuple

import pyarrow as pa
import pyarrow.dataset as pds
import pyarrow.fs as pafs
from pyarrow.parquet import ColumnSchema, FileMetaData, ParquetSchema, RowGroupMetaData

import ray
from ray.data._internal.datasource_v2.common.file_reader import casts_to_schema
from ray.data._internal.datasource_v2.formats.parquet.parquet_decode_peak import (
    estimate_cast,
    estimate_row_group_peak,
    estimate_row_group_work,
)
from ray.data._internal.datasource_v2.formats.parquet.parquet_decoded_size import (
    LeafProfile,
    Read,
    added_bytes,
    build_leaf_profiles,
    estimate_added_columns,
    leaf_chunks,
)
from ray.data._internal.datasource_v2.formats.parquet.parquet_file_chunking_utils import (
    _row_group_unit_id,
)
from ray.data._internal.datasource_v2.formats.parquet.parquet_row_group_coalescing import (
    coalesce_row_groups,
)
from ray.data._internal.datasource_v2.formats.parquet.parquet_size_statistics import (
    LeafSizeStats,
    read_footer_stats,
)
from ray.data._internal.datasource_v2.formats.parquet.parquet_utils import (
    READER_CPU_COUNT,
)
from ray.data._internal.datasource_v2.interfaces.file_indexer import FileInfo
from ray.data._internal.datasource_v2.interfaces.file_manifest import FileChunk
from ray.data._internal.planner.plan_expression.expression_visitors import (
    get_column_references,
)
from ray.data._internal.util import call_with_retry, filesystem_for_sized_opens
from ray.data.checkpoint.generated_id import GENERATED_ID_COLUMN_TYPE

if TYPE_CHECKING:
    import pyarrow.compute as pc

    from ray.data.expressions import Expr

logger = logging.getLogger(__name__)


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


def _column_leaves(
    columns: list[ColumnSchema], names: Iterable[str]
) -> dict[str, list[int]]:
    """Each of ``names`` the file has, in that order, as its leaf indices."""
    leaves: dict[str, list[int]] = {name: [] for name in names}
    for leaf_idx, column in enumerate(columns):
        # A leaf belongs to the longest of its path's dotted prefixes in ``names``,
        # once: a flat ``user.id`` beside a ``user`` column is ``user.id``'s alone.
        parts = column.path.split(".")
        for end in range(len(parts), 0, -1):
            indices = leaves.get(".".join(parts[:end]))
            if indices is not None:
                indices.append(leaf_idx)
                break
    return {name: indices for name, indices in leaves.items() if indices}


class _FileSizing(NamedTuple):
    """What sizing a file's row groups needs, resolved once per file."""

    size: int  # of the file on disk
    columns: list[ColumnSchema]  # every leaf, by leaf index
    leaf_indices: list[int]  # the leaves a read task decodes
    column_leaves: list[list[int]]  # each column read, in PyArrow's order, as leaves
    # As PyArrow decodes them, ``None`` for a leaf or a file the estimator doesn't model.
    profiles: list[LeafProfile | None] | None
    # The columns the pin retypes, as their leaves, and the leaves as the pin
    # casts them; empty and ``None`` when it retypes none.
    cast_leaves: dict[str, list[int]]
    cast_profiles: list[LeafProfile | None] | None
    file_schema: pa.Schema | None
    read: Read  # the columns it appends


# What a row group's leaves the estimator doesn't model add to its decoded size,
# and so to its peak: this many times their uncompressed bytes, and never less
# than the floor.
_UNMODELLED_RATIO = 4
_UNMODELLED_MIN_BYTES = 12 << 20


def _unmodelled_bytes(row_group: RowGroupMetaData, leaf_indices: list[int]) -> int:
    """The charge for ``leaf_indices``, the leaves of ``row_group`` left unsized."""
    uncompressed = sum(
        row_group.column(leaf_idx).total_uncompressed_size for leaf_idx in leaf_indices
    )
    return max(_UNMODELLED_MIN_BYTES, _UNMODELLED_RATIO * uncompressed)


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
        format_kwargs: dict | None = None,
        read_schema: pa.Schema | None = None,
    ):
        from ray.data.context import DEFAULT_RETRIED_IO_ERRORS

        self.retried_io_errors = (
            retried_io_errors
            if retried_io_errors is not None
            else list(DEFAULT_RETRIED_IO_ERRORS)
        )
        # Coalescing target: merge contiguous row groups into chunks of
        # ~coalesce_bytes of decoded size before returning them, so the driver packs
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
        self.filter_columns: list[str] = (
            list(dict.fromkeys(get_column_references(filter_expr)))
            if filter_expr is not None
            else []
        )
        # Top-level columns a read task decodes, in PyArrow's order: the
        # predicate's, decoded to filter even when projected out, then the
        # projection's. Sizing on the projection alone lets a large filter column
        # overfill read tasks. ``None`` means every column, the predicate's first.
        self.read_order: list[str] | None = (
            list(dict.fromkeys([*self.filter_columns, *projected_cols]))
            if projected_cols is not None
            else None
        )
        # Reused across files; make_fragment is what lets us apply
        # split_by_row_group.
        self.file_format = pds.ParquetFileFormat()
        # The ``ParquetFileFormat`` kwargs the read tasks pass; the estimator
        # doesn't model some of their options.
        self.format_kwargs = dict(format_kwargs or {})
        # The dataset schema: the read tasks cast every batch to it, when
        # ``casts_to_schema`` says they do, and append its columns a file lacks.
        self.read_schema = read_schema
        self.pinned_schema = read_schema if casts_to_schema(read_schema) else None

    def _file_sizing(
        self, file: FileInfo, fragment: pds.ParquetFileFragment, metadata: FileMetaData
    ) -> _FileSizing:
        """Resolve what sizing ``file``'s row groups needs, once per file."""
        path = file.path
        size = file.size
        if size is None:
            # Ray's listing sizes every file; a row built without one looks it up.
            size = call_with_retry(
                lambda: self.filesystem.get_file_info(path).size,
                description=f"size {path}",
                match=self.retried_io_errors,
            )
        try:
            # Cached on the fragment by the footer read.
            file_schema: pa.Schema | None = fragment.physical_schema
        except Exception:
            file_schema = None
        schema = metadata.schema
        columns = [schema.column(j) for j in range(len(schema))]
        file_names = [] if file_schema is None else list(file_schema.names)
        names: Iterable[str] | None = self.read_order
        if names is None:
            # Every column, the predicate's first. Without the file's schema, no
            # names: every leaf is sized below, not just the predicate's.
            names = (
                dict.fromkeys([*self.filter_columns, *file_names]) if file_names else []
            )
        if file_schema is not None:
            # Only the file's columns: a missing ``sepal`` would match the leaf of
            # a flat ``sepal.length`` too, and size it twice.
            present = set(file_names)
            names = [name for name in names if name in present]
        leaves = _column_leaves(columns=columns, names=names)
        if not leaves:
            # The file has none of the columns read: size it whole, the safe side.
            leaves = _column_leaves(columns=columns, names=file_names)
        column_leaves = list(leaves.values())
        leaf_indices = sorted(j for indices in column_leaves for j in indices)
        if not leaf_indices:
            leaf_indices = list(range(len(columns)))

        profiles = cast_profiles = None
        cast_leaves: dict[str, list[int]] = {}
        if file_schema is not None:
            # With a pin the reader drops ``format_kwargs``' type options, so
            # these are the file's own types, which the cast below retypes.
            profiles = build_leaf_profiles(
                parquet_schema=schema,
                format_kwargs=self.format_kwargs,
                file_metadata=metadata,
            )
        if profiles is not None and self.pinned_schema is not None:
            assert file_schema is not None
            pinned = self.pinned_schema
            cast_leaves = {
                name: indices
                for name, indices in leaves.items()
                if file_schema.get_field_index(name) >= 0
                and pinned.get_field_index(name) >= 0
                and file_schema.field(name).type != pinned.field(name).type
            }
            if cast_leaves:
                cast_profiles = build_leaf_profiles(
                    parquet_schema=schema,
                    pinned_schema=pinned,
                    format_kwargs=self.format_kwargs,
                    file_metadata=metadata,
                )
                if cast_profiles is None:
                    # A cast the estimator doesn't model: charge every leaf as unmodelled.
                    profiles = None
        added_row_bytes, added_row_group_bytes = self._added_bytes(
            file_schema=file_schema, path=path
        )
        return _FileSizing(
            size=size,
            columns=columns,
            leaf_indices=leaf_indices,
            column_leaves=column_leaves,
            profiles=profiles,
            cast_leaves=cast_leaves,
            cast_profiles=cast_profiles,
            file_schema=file_schema,
            read=Read(
                added_row_bytes=added_row_bytes,
                added_row_group_bytes=added_row_group_bytes,
            ),
        )

    def _row_group_sizes(
        self,
        row_group: RowGroupMetaData,
        sizing: _FileSizing,
        size_stats: list[LeafSizeStats | None] | None,
    ) -> tuple[int, int]:
        """``row_group``'s estimated decoded bytes, and PyArrow's estimated peak
        bytes decoding it as a read task does. Both lean high: a read task over its
        estimate is what runs a node out of memory."""
        buffers = 0
        if sizing.profiles is None:
            # A file the estimator doesn't model; the columns Ray appends still count.
            decoded = estimate_added_columns(row_group=row_group, read=sizing.read)
            unmodelled = list(sizing.leaf_indices)
        else:
            leaves = leaf_chunks(
                row_group=row_group,
                leaf_profiles=sizing.profiles,
                leaf_indices=sizing.leaf_indices,
                size_stats=size_stats,
            )
            decoded, buffers = estimate_row_group_work(
                row_group=row_group,
                columns=sizing.columns,
                column_leaves=sizing.column_leaves,
                leaves=leaves,
                counted=set(),
                decode_threads=READER_CPU_COUNT,
            )
            unmodelled = [j for j in sizing.leaf_indices if j not in leaves]
            if self.pinned_schema is None:
                decoded += estimate_added_columns(row_group=row_group, read=sizing.read)
            else:
                assert sizing.file_schema is not None
                # Here ``cast_leaves`` is empty unless ``cast_profiles`` was built.
                cast_leaves = {
                    name: leaf_chunks(
                        row_group=row_group,
                        leaf_profiles=sizing.cast_profiles or [],
                        leaf_indices=indices,
                        size_stats=size_stats,
                    )
                    for name, indices in sizing.cast_leaves.items()
                }
                for name, indices in sizing.cast_leaves.items():
                    unmodelled += [j for j in indices if j not in cast_leaves[name]]
                decoded += estimate_cast(
                    row_group=row_group,
                    column_leaves=cast_leaves,
                    file_schema=sizing.file_schema,
                    pinned_schema=self.pinned_schema,
                    read=sizing.read,
                )
        if unmodelled:
            # A leaf or cast the estimator doesn't model, such as a string without
            # SizeStatistics, a view, a dictionary or a struct gaining a field.
            decoded += _unmodelled_bytes(row_group=row_group, leaf_indices=unmodelled)
        peak = estimate_row_group_peak(
            row_group=row_group,
            leaf_indices=sizing.leaf_indices,
            row_group_work=decoded + buffers,
            file_size=sizing.size,
        )
        return decoded, peak

    def _locate_filter_columns(self, schema: ParquetSchema) -> _FilterLeaves:
        # Resolve the predicate's column names to Parquet leaf-column indices,
        # the same mapping ``_column_leaves`` does for the read columns.
        # Called once per file so the per-row-group null check below is an
        # index lookup instead of another scan over every leaf. Leaf order is
        # the schema's, so these index a row group's columns just as well.
        # Read off the schema rather than a row group because a leaf's logical
        # type lives there, whereas a row group only exposes it via statistics
        # -- which a file need not carry.
        if not self.filter_columns:
            return _FilterLeaves([], True)
        wanted = set(self.filter_columns)
        indices: list[int] = []
        matched: set[str] = set()
        has_float_leaf = False
        for j in range(len(schema)):
            column = schema.column(j)
            hits = _get_prefix_matches(column.path, wanted)
            if hits:
                matched.update(hits)
                indices.append(j)
                has_float_leaf = has_float_leaf or _is_float_leaf(column)
        return _FilterLeaves(indices, matched == wanted, has_float_leaf)

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

    def _added_bytes(
        self, file_schema: pa.Schema | None, path: str
    ) -> tuple[float, int]:
        """:func:`added_bytes` of the dataset schema's columns the file lacks or holds
        as nulls, less those the projection drops; it never drops the checkpoint ID."""
        if self.read_schema is None or file_schema is None:
            return 0, 0
        added = []
        for field in self.read_schema:
            idx = file_schema.get_field_index(field.name)
            if pa.types.is_null(field.type) or (
                idx >= 0 and not pa.types.is_null(file_schema.field(idx).type)
            ):
                continue
            projected = self.read_order is None or field.name in self.read_order
            if projected or field.type == GENERATED_ID_COLUMN_TYPE:
                added.append(field)
        return added_bytes(added=added, path=path)

    def _read_and_chunk(self, file: FileInfo) -> ChunkedFile:
        path = file.path
        # The listing's size spares the open a request for it (a HEAD on S3).
        fragment = self.file_format.make_fragment(
            path,
            filesystem=filesystem_for_sized_opens(self.filesystem),
            file_size=file.size,
        )
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
        # The predicate's leaves are schema-derived, and a Parquet file has a
        # single schema, so resolve them once rather than rescanning every leaf
        # for each group.
        filter_leaves = self._locate_filter_columns(metadata.schema)
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

        per_rg: list[FileChunk] = []
        if rg_indices:
            # One Thrift walk per file over the footer bytes ``metadata`` already
            # holds -- no extra IO, though ``__reduce__`` re-serializes the
            # footer, so skip it when every row group was pruned.
            size_stats = read_footer_stats(metadata)
            sizing = self._file_sizing(file=file, fragment=fragment, metadata=metadata)
            for rg_idx in rg_indices:
                row_group = metadata.row_group(rg_idx)
                decoded, peak = self._row_group_sizes(
                    row_group=row_group,
                    sizing=sizing,
                    size_stats=None if size_stats is None else size_stats[rg_idx],
                )
                per_rg.append(
                    FileChunk(
                        unit_ids=(rg_idx,),
                        num_rows=row_group.num_rows,
                        size_bytes=decoded,
                        memory_bytes=peak,
                        fully_matched=fully_by_idx.get(rg_idx, True),
                    )
                )
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
