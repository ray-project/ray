from __future__ import annotations

import logging
import math
from concurrent.futures import Future, ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from typing import (
    TYPE_CHECKING,
    AbstractSet,
    Iterable,
    Iterator,
    NamedTuple,
    Tuple,
)

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.dataset as pds
import pyarrow.fs as pafs
from pyarrow.parquet import ColumnSchema, ParquetSchema, RowGroupMetaData

import ray
from ray.data._internal.datasource_v2.common.file_pruners import (
    PartitionPredicatePruner,
)
from ray.data._internal.datasource_v2.common.partitioning_utils import (
    _evaluate_path_predicate,
    _partition_table,
    _PartitionValueParser,
)
from ray.data._internal.datasource_v2.common.pushdown_utils import (
    _split_predicate_by_columns,
    combine_predicates,
)
from ray.data._internal.datasource_v2.formats.parquet.parquet_file_chunking_utils import (
    _row_group_unit_id,
)
from ray.data._internal.datasource_v2.formats.parquet.parquet_row_group_coalescing import (
    coalesce_row_groups,
)
from ray.data._internal.datasource_v2.formats.parquet.parquet_utils import (
    _row_group_uncompressed_size,
)
from ray.data._internal.datasource_v2.interfaces.file_indexer import FileInfo
from ray.data._internal.datasource_v2.interfaces.file_manifest import FileChunk
from ray.data._internal.planner.plan_expression.expression_visitors import (
    get_column_references,
)
from ray.data._internal.util import call_with_retry, filesystem_for_sized_opens
from ray.data.datasource.partitioning import Partitioning
from ray.data.expressions import (
    AliasExpr,
    BinaryExpr,
    ColumnExpr,
    LiteralExpr,
    Operation,
    UnaryExpr,
    _PyArrowExpressionVisitor,
)

if TYPE_CHECKING:
    from ray.data._internal.datasource_v2.interfaces.synthesized_columns import (
        SynthesizedColumn,
    )
    from ray.data.expressions import Expr

ResidualPruning = Tuple[
    "Expr", Partitioning, pa.Schema, tuple["SynthesizedColumn", ...]
]

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
        partitioning: Partitioning | None = None,
        partition_pruner: PartitionPredicatePruner | None = None,
        logical_schema: pa.Schema | None = None,
        *,
        residual_pruning: ResidualPruning | None = None,
        residual_read_options: pds.ParquetReadOptions | None = None,
    ):
        from ray.data.context import DEFAULT_RETRIED_IO_ERRORS

        self.retried_io_errors = (
            retried_io_errors
            if retried_io_errors is not None
            else list(DEFAULT_RETRIED_IO_ERRORS)
        )
        # Coalescing target: merge contiguous row groups into chunks of
        # ~coalesce_bytes uncompressed before returning them, so the driver packs
        # fewer items. 0 disables coalescing (one chunk per physical row group).
        self.coalesce_bytes = coalesce_bytes
        # Read unit ids a checkpoint already finished (``"<path>#rg<N>"`` for a
        # row group). Those row groups are left out of every ``ChunkedFile``.
        self.excluded_read_unit_ids: AbstractSet[str] = (
            excluded_read_unit_ids or frozenset()
        )
        self.filesystem = filesystem
        self.partitioning = (
            partition_pruner.partitioning
            if partition_pruner is not None
            else partitioning
        )
        self.logical_schema = (
            partition_pruner.schema if partition_pruner is not None else logical_schema
        )
        self._predicate = combine_predicates(
            filter_expr,
            partition_pruner.predicate if partition_pruner is not None else None,
        )
        self._partition_value_parser = (
            _PartitionValueParser(self.partitioning)
            if self.partitioning is not None and self._predicate is not None
            else None
        )
        self.residual_pruning = residual_pruning
        self.residual_read_options = residual_read_options
        self._residual_partition_value_parser = None
        if residual_pruning is not None:
            try:
                partitioning = residual_pruning[1]
                self._residual_partition_value_parser = (
                    self._partition_value_parser
                    if self._partition_value_parser is not None
                    and partitioning == self.partitioning
                    else _PartitionValueParser(partitioning)
                )
            except Exception:
                # Q's parser initialization above must still raise normally;
                # only initialization needed exclusively by R is optional.
                self.residual_pruning = None
        self._can_bind_residual_partitions = True
        if self.residual_pruning is not None and self._predicate is not None:
            try:
                # Validate Q's grammar once before sharing partition facts.
                # Native expressions in compute data operands can hide field
                # references from get_column_references, so cannot share facts.
                _validate_residual_expression(self._predicate, self.residual_pruning[2])
            except Exception:
                self._can_bind_residual_partitions = False
        filter_expr = self._predicate
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

    def _row_group_run(
        self,
        row_group: RowGroupMetaData,
        rg_idx: int,
        leaf_indices: list[int] | None,
        fully_matched: bool = False,
    ) -> FileChunk:
        # Sum per-column sizes on both paths -- with a projection, only the
        # leaves the reader decodes, so bin packing reflects the bytes it will
        # actually pull. Deliberately not ``row_group.total_byte_size``, which
        # is one cheap accessor but can report the *compressed* size for some
        # files (apache/arrow#48138); undersizing bins here overfills read
        # tasks, which is the failure this whole path exists to avoid. Shares
        # the V1 helper so the three call sites cannot drift.
        uncompressed = _row_group_uncompressed_size(row_group, leaf_indices)
        return FileChunk(
            unit_ids=(rg_idx,),
            num_rows=row_group.num_rows,
            size_bytes=uncompressed,
            fully_matched=fully_matched,
        )

    def _locate_filter_columns(
        self, schema: ParquetSchema, columns: set[str] | None = None
    ) -> _FilterLeaves:
        # Resolve the predicate's column names to Parquet leaf-column indices,
        # the same mapping ``_read_leaf_indices`` does for the read columns.
        # Called once per file so the per-row-group null check below is an
        # index lookup instead of another scan over every leaf. Leaf order is
        # the schema's, so these index a row group's columns just as well.
        # Read off the schema rather than a row group because a leaf's logical
        # type lives there, whereas a row group only exposes it via statistics
        # -- which a file need not carry.
        columns = self.filter_columns if columns is None else columns
        if not columns:
            return _FilterLeaves([], True)
        indices: list[int] = []
        matched: set[str] = set()
        has_float_leaf = False
        for j in range(len(schema)):
            column = schema.column(j)
            hits = _get_prefix_matches(column.path, columns)
            if hits:
                matched.update(hits)
                indices.append(j)
                has_float_leaf = has_float_leaf or _is_float_leaf(column)
        return _FilterLeaves(indices, matched == columns, has_float_leaf)

    def _filter_for_file(
        self, path: str, schema: ParquetSchema
    ) -> tuple[pc.Expression | None, set[str], bool]:
        """Select safe physical conditions and whether they cover the full filter."""
        if self._predicate is None or (
            self.partitioning is None and self.logical_schema is None
        ):
            return self.filter, self.filter_columns, True
        values = (
            self._partition_value_parser(path)
            if self._partition_value_parser is not None
            else {}
        )
        split = _split_predicate_by_columns(self._predicate, set(values))
        complete = split.residual_predicate is None
        if split.partition_predicate is not None:
            # Without the logical schema, path types alone cannot prove the
            # reader's output values. Keep the groups but do not count them.
            complete = complete and self.logical_schema is not None
            if complete:
                complete = (
                    _evaluate_path_predicate(
                        split.partition_predicate,
                        values,
                        self.logical_schema,
                        path=path,
                    )
                    is True
                )
        physical = split.data_predicate
        logical_schema = self.logical_schema
        if physical is not None and logical_schema is not None:
            physical_schema = schema.to_arrow_schema()
            unsafe = {
                name
                for name in get_column_references(physical)
                if physical_schema.get_field_index(name) != -1
                and (
                    logical_schema.get_field_index(name) == -1
                    or physical_schema.field(name).type
                    != logical_schema.field(name).type
                )
            }
            if unsafe:
                safe = _split_predicate_by_columns(physical, unsafe)
                physical = safe.data_predicate
                complete = False
        if physical is None:
            return None, set(), complete
        expression = (
            self.filter if physical is self._predicate else physical.to_pyarrow()
        )
        return expression, set(get_column_references(physical)), complete

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
        self,
        fragment,
        expr: "pc.Expression",
        path: str,
        schema: pa.Schema | None = None,
    ) -> list[int] | None:
        """Row-group ids that statistics say may match ``expr``, or None on failure.

        ``None`` means pruning cannot be applied, so the caller must fail closed
        (keep every group / treat none as fully matched). Swallows the
        exception so one mismatched file cannot abort a ``read_footers`` batch.
        """
        try:
            fragments = (
                fragment.split_by_row_group(expr)
                if schema is None
                else fragment.split_by_row_group(expr, schema=schema)
            )
            return [sub.row_groups[0].id for sub in fragments]
        except Exception as e:
            logger.debug(
                "Error splitting by row group: %s for file %s",
                e,
                path,
                exc_info=True,
            )
            return None

    def _residual_partition_facts(
        self, path: str
    ) -> tuple[pc.Expression, set[str], set[str]] | None:
        """Prepare native facts, overwritten names, and successfully bound names."""
        parser = self._residual_partition_value_parser
        if self.residual_pruning is None or parser is None:
            return None
        try:
            expr, _, logical_schema, synthesized = self.residual_pruning
            references = set(get_column_references(expr))
            synthesized_names = {column.name for column in synthesized}
            values = parser(path)
            values = {
                name: value
                for name, value in values.items()
                if name in references and name not in synthesized_names
            }
            # Only keys actually present in this path are facts. A missing key
            # can be a physical column at the root, rather than an implicit NULL.
            # With the same parser, _filter_for_file removes these keys from Q.
            # With a different parser, do not bind any possible Q reference.
            blocked = (
                self.filter_columns
                if parser is not self._partition_value_parser
                else set()
            )
            if not self._can_bind_residual_partitions:
                blocked = set(values)
            # Preserve R's existing fallback on incompatible path conversion,
            # even when a converted value cannot be bound as a shared fact.
            partition_table = _partition_table(values, logical_schema)
            partition_expression = pds.scalar(True)
            bound_partition_columns = set()
            for name in values:
                if name in blocked:
                    continue
                scalar = partition_table[name][0]
                if (
                    scalar.is_valid
                    and pa.types.is_floating(scalar.type)
                    and math.isnan(scalar.as_py())
                ):
                    # NaN == NaN is not a true partition fact. Its overwrite
                    # also makes same-named physical statistics unusable.
                    continue
                fact = (
                    pds.field(name) == pds.scalar(scalar)
                    if scalar.is_valid
                    else pds.field(name).is_null()
                )
                partition_expression = partition_expression & fact
                bound_partition_columns.add(name)
            return partition_expression, set(values), bound_partition_columns
        except Exception:
            return None

    def _residual_row_groups(
        self,
        fragment,
        schema: ParquetSchema,
        candidates: Iterable[int],
        facts: tuple[pc.Expression, set[str], set[str]] | None,
    ) -> list[int] | None:
        """Additional possible groups, never evidence for ``fully_matched``."""
        if self.residual_pruning is None or facts is None:
            return None
        candidates = list(candidates)
        if not candidates:
            return None
        try:
            expr, _, logical_schema, synthesized = self.residual_pruning
            references = set(get_column_references(expr))
            synthesized_names = {column.name for column in synthesized}
            _, overwritten_columns, bound_partition_columns = facts
            unknown_columns = (
                (
                    reader_unknown_columns(
                        schema, logical_schema, references, self.residual_read_options
                    )
                    | overwritten_columns
                )
                - bound_partition_columns
            ) | synthesized_names
            bound = _validate_residual_expression(expr, logical_schema, unknown_columns)
            bound = _PyArrowExpressionVisitor(preserve_scalar_types=True).visit(bound)
            # Share the already-read metadata, but give R its own statistics
            # cache when binding against the reader's logical schema.
            fragment = fragment.subset(row_group_ids=candidates)
        except Exception:
            return None
        return self._try_split_by_row_group(
            fragment, bound, fragment.path, logical_schema
        )

    def _read_and_chunk(self, file: FileInfo) -> ChunkedFile:
        path = file.path
        residual_facts = self._residual_partition_facts(path)
        # The listing's size spares the open a request for it (a HEAD on S3).
        fragment = self.file_format.make_fragment(
            path,
            filesystem=filesystem_for_sized_opens(self.filesystem),
            file_size=file.size,
            partition_expression=residual_facts[0]
            if residual_facts is not None
            else None,
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
        # Both column look-ups are schema-derived, and a Parquet file has a
        # single schema, so resolve them once rather than rescanning every leaf
        # for each group. The read-column lookup reads leaf paths off row group 0,
        # which a file with no row groups has none of; it sizes nothing either way.
        file_filter, filter_columns, complete_filter = self._filter_for_file(
            path, metadata.schema
        )
        filter_leaves = self._locate_filter_columns(metadata.schema, filter_columns)
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

        if file_filter is not None and filter_leaves.all_filter_columns_found:
            # Predicate pushdown: drop row groups whose Parquet statistics
            # contradict the filter, so we never emit chunks the reader would
            # skip anyway. Each returned fragment views a single surviving group.
            # Fail closed on any split error -- keep every group -- so a type
            # mismatch or similar cannot abort the rest of the read_footers batch.
            surviving: list[int] | None = self._try_split_by_row_group(
                fragment, file_filter, path
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
            not_fully_ids = self._try_split_by_row_group(fragment, ~file_filter, path)
            not_fully = set(rg_indices) if not_fully_ids is None else set(not_fully_ids)
            # The negation test alone is not sufficient: ``~filter`` can prune a
            # group that still holds a row the filter does not return, and then
            # ``num_rows`` over-counts and limit push-down stops early. Nulls do
            # this under three-valued logic and NaNs because min/max skip them,
            # so ``_rg_can_fully_match`` rules both out from the footer instead
            # -- which holds however clever the engine's statistics pruning gets.
            # PyArrow declines the null prune today but already makes the NaN one.
            fully_by_idx: dict[int, bool] = {
                i: complete_filter
                and i not in not_fully
                and self._rg_can_fully_match(metadata.row_group(i), filter_leaves)
                for i in rg_indices
            }
        elif file_filter is not None:
            # A predicate column is absent from this file's schema. PyArrow's
            # split_by_row_group raises ArrowInvalid ("No match for FieldRef.Name")
            # in that case, which would abort the whole read_footers batch.
            # Skip pruning: keep every row group and let the reader apply the
            # filter after null-fill. None can be an exact survivor.
            rg_indices = unprocessed_rg_ids
            fully_by_idx = dict.fromkeys(rg_indices, False)
        else:
            rg_indices = unprocessed_rg_ids
            # No physical predicate does not prove a missing path condition.
            fully_by_idx = dict.fromkeys(rg_indices, complete_filter)

        selected = self._residual_row_groups(
            fragment, metadata.schema, rg_indices, residual_facts
        )
        if selected is not None:
            possible = set(selected)
            rg_indices = [i for i in rg_indices if i in possible]

        per_rg = [
            self._row_group_run(
                row_group=metadata.row_group(rg_idx),
                rg_idx=rg_idx,
                leaf_indices=leaf_indices,
                fully_matched=fully_by_idx.get(rg_idx, True),
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


_COMPARISONS = frozenset(
    (Operation.EQ, Operation.NE, Operation.LT, Operation.LE, Operation.GT, Operation.GE)
)


def _validate_residual_expression(expr: "Expr", schema, unknown_columns=frozenset()):
    """Use native statistics only for an unchanged, row-local expression.

    Unsafe expressions disable this optional hint as a whole. The original
    pushed predicate and residual Filter still run, so this costs only pruning.
    In particular, Arrow statistics can lose NaNs for negated comparisons and
    distinguish signed zeros differently from eager IN evaluation.
    """

    def column_type(column):
        if column.name in unknown_columns:
            raise ValueError("Reader changes the column domain")
        i = schema.get_field_index(column.name)
        if i < 0:
            raise ValueError("Unknown column")
        t = schema.field(i).type
        if (
            pa.types.is_nested(t)
            or pa.types.is_dictionary(t)
            or isinstance(t, pa.BaseExtensionType)
        ):
            raise ValueError("Non-scalar reader domain")
        return t

    def visit(e):
        if isinstance(e, AliasExpr):
            visit(e.expr)
        elif isinstance(e, LiteralExpr):
            value = e.value.as_py() if isinstance(e.value, pa.Scalar) else e.value
            if value is not None and not isinstance(value, bool):
                raise ValueError("Not a Boolean literal")
        elif isinstance(e, ColumnExpr):
            if not pa.types.is_boolean(column_type(e)):
                raise ValueError("Not a Boolean column")
        elif isinstance(e, UnaryExpr) and e.op == Operation.NOT:
            visit(e.operand)
        elif (
            isinstance(e, UnaryExpr)
            and e.op in (Operation.IS_NULL, Operation.IS_NOT_NULL)
            and isinstance(e.operand, ColumnExpr)
        ):
            column_type(e.operand)
        elif isinstance(e, BinaryExpr) and e.op in (Operation.AND, Operation.OR):
            visit(e.left)
            visit(e.right)
        elif isinstance(e, BinaryExpr) and e.op in _COMPARISONS:
            column, literal = (
                (e.left, e.right)
                if isinstance(e.left, ColumnExpr)
                else (e.right, e.left)
            )
            if not isinstance(column, ColumnExpr) or not isinstance(
                literal, LiteralExpr
            ):
                raise ValueError("Not a column-scalar comparison")
            value = pa.scalar(literal.value)
            if (
                pa.types.is_floating(column_type(column))
                or pa.types.is_floating(value.type)
                or pa.types.is_nested(value.type)
            ):
                raise ValueError("Unsafe statistics comparison")
        elif (
            isinstance(e, BinaryExpr)
            and e.op in (Operation.IN, Operation.NOT_IN)
            and isinstance(e.left, ColumnExpr)
            and isinstance(e.right, LiteralExpr)
        ):
            if pa.types.is_floating(column_type(e.left)):
                raise ValueError("Unsafe floating-point membership")
            if not isinstance(e.right.value, list):
                raise ValueError("Not a literal value set")
            values_type = pa.array(e.right.value).type
            if not pa.types.is_null(values_type) and values_type != column_type(e.left):
                raise ValueError("Membership changes the scalar domain")
        else:
            raise ValueError("Not a supported row-local statistics expression")

    visit(expr)
    return expr


def reader_unknown_columns(
    parquet_schema, reader_schema, references, read_options=None
):
    """Identify incompatible reader domains; schema equality is not sufficient."""
    physical = (
        parquet_schema
        if isinstance(parquet_schema, pa.Schema)
        else parquet_schema.to_arrow_schema()
    )
    logical = physical if reader_schema is None else reader_schema
    result = set()
    for name in references:
        i, j = physical.get_field_index(name), logical.get_field_index(name)
        if i < 0:  # Folder-only fields remain usable partition facts.
            continue
        before, after = (
            physical.field(i).type,
            logical.field(j).type if j >= 0 else None,
        )
        if before != after or any(
            isinstance(t, pa.BaseExtensionType) for t in (before, after)
        ):
            result.add(name)
    if read_options is not None and read_options.coerce_int96_timestamp_unit != "ns":
        for i in range(len(parquet_schema)):
            leaf = parquet_schema.column(i)
            if leaf.physical_type == "INT96":
                result.update(_get_prefix_matches(leaf.path, references))
    return frozenset(result)
