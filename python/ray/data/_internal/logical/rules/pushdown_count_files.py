import dataclasses
import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING, Callable, List, Optional, Tuple, cast

from ray.data._internal.datasource_v2.common.pushdown_utils import (
    derive_list_files_pushdown,
)
from ray.data._internal.datasource_v2.interfaces.file_manifest import (
    PATH_COLUMN_NAME,
    FileChunk,
    FileManifest,
)
from ray.data._internal.datasource_v2.interfaces.pushdown import (
    ListFilesPushdown,
    SupportsColumnPruning,
)
from ray.data._internal.datasource_v2.interfaces.scanner import Scanner
from ray.data._internal.datasource_v2.interfaces.supports_metadata import (
    MetadataType,
    SupportsMetadata,
)
from ray.data._internal.logical.interfaces import LogicalPlan, Rule
from ray.data._internal.logical.operators.count_operator import Count
from ray.data._internal.logical.operators.map_operator import MapBatches, Project
from ray.data._internal.logical.operators.read_operator import ListFiles, ReadFiles

if TYPE_CHECKING:
    import pyarrow as pa

logger = logging.getLogger(__name__)

# Footer-read tasks are network-bound; lower their CPU share so at least two
# can run per core. The filtered path may decode partial runs, so it keeps 1.
_UNFILTERED_COUNT_NUM_CPUS = 0.5
_FILTERED_COUNT_NUM_CPUS = 1.0

# Manifest rows per filtered count task. Fully matched rows are cheap; batching
# keeps task overhead down while parallelizing the partial reads.
_FILTERED_COUNT_BATCH_SIZE = 16


@dataclass(frozen=True)
class _CountPushdown:
    """A selected count strategy, ready to be lowered to ``MapBatches``."""

    list_files: ListFiles
    count_rows: Callable[["pa.Table"], "pa.Table"]
    batch_size: Optional[int]
    num_cpus: float


def _manifest_from_batch(batch: "pa.Table") -> FileManifest:
    assert PATH_COLUMN_NAME in batch.column_names, batch.column_names
    return FileManifest(batch)


def _count_result(total_rows: int) -> "pa.Table":
    import pyarrow as pa

    return pa.table({Count.COLUMN_NAME: pa.array([total_rows])})


def _split_fully_matched_rows(manifest: FileManifest) -> Tuple[int, List[int]]:
    """Split a manifest into exact-counted rows and rows that must be read.

    A manifest row has one of three states:

    * chunk metadata absent: the whole file is opaque, so read it;
    * ``fully_matched`` true: footer statistics prove every row survives the
      predicate, so ``num_rows`` is an exact contributor;
    * ``fully_matched`` false: the run may contain non-matching rows, so read it.

    The first case only appears for indexers that do not chunk files; the
    filtered count path requires a chunking indexer, but treating a missing
    entry as "read" keeps the helper correct on its own.
    """
    counted_rows = 0
    to_read: List[int] = []
    for idx, chunk_metadata in enumerate(manifest.file_chunk_metadatas):
        if chunk_metadata is None:
            to_read.append(idx)
            continue
        chunk = FileChunk.from_metadata(chunk_metadata)
        if chunk.fully_matched:
            counted_rows += chunk.num_rows
        else:
            to_read.append(idx)
    return counted_rows, to_read


def _can_count_from_fully_matched_chunks(
    list_files: ListFiles,
    scanner: Scanner,
    pushdown: ListFilesPushdown,
) -> bool:
    """Whether the filtered-count strategy is sound for this listing.

    All row-reducing and synthesized-column cases must decline: the count task
    reads with an empty projection, and only the listing's predicate is applied.
    """
    return (
        pushdown.predicate is not None
        and pushdown.limit is None
        and pushdown.partition_pruner is None
        # Synthesized columns (path / row_hash) are not materialized by the
        # empty count projection, so a predicate on them must read normally.
        and not getattr(scanner, "synthesized_columns", ())
        and list_files.file_indexer.emits_fully_matched_chunks()
    )


def _try_unfiltered_count_pushdown(
    list_files: ListFiles,
    scanner: Scanner,
    pushdown: ListFilesPushdown,
) -> Optional[_CountPushdown]:
    """Build the metadata-only count strategy, or decline.

    Precondition: ``scanner.metadata_row_count_is_exact()``. The listing is
    rebuilt with a whole-file indexer so each file appears exactly once and
    listing does no metadata IO.
    """
    reader = scanner.create_reader()
    if (
        not isinstance(reader, SupportsMetadata)
        or MetadataType.NUM_ROWS not in reader.available_metadata()
    ):
        return None

    # A metadata-aware indexer overrides ``list_files`` outright and would keep
    # footer-sweeping during listing, potentially emitting a path once per bin.
    # ``as_whole_file_indexer`` is the explicit contract for this swap.
    whole_file_indexer = list_files.file_indexer.as_whole_file_indexer()
    if whole_file_indexer is None:
        logger.debug(
            "Skipping count pushdown: %s cannot provide a whole-file indexer",
            type(list_files.file_indexer).__name__,
        )
        return None

    assert pushdown.limit is None, "metadata_row_count_is_exact must refuse a limit"
    list_files = dataclasses.replace(
        list_files,
        file_partitioner=None,
        file_indexer=whole_file_indexer,
        predicate=pushdown.predicate,
        projected_columns=None,
        limit=None,
        partition_pruner=None,
        pushdown_is_final=True,
    )

    # ``reader`` is narrowed by the guard above, but that narrowing is lost
    # inside the closure -- bind a typed local so its type carries through.
    metadata_reader: SupportsMetadata = reader

    def count_rows(batch: "pa.Table") -> "pa.Table":
        # Apply the same scan-side pruning the read path does. Exact row counts
        # alone do not account for scanners that override ``prune_input_split``.
        manifest = scanner.prune_input_split(_manifest_from_batch(batch))
        total_rows = 0
        for block_metadata in metadata_reader.read_metadata(manifest):
            total_rows += block_metadata.num_rows or 0
        return _count_result(total_rows)

    return _CountPushdown(
        list_files=list_files,
        count_rows=count_rows,
        batch_size=metadata_reader.get_target_metadata_batch_size(),
        num_cpus=_UNFILTERED_COUNT_NUM_CPUS,
    )


def _try_filtered_count_pushdown(
    list_files: ListFiles,
    scanner: Scanner,
    pushdown: ListFilesPushdown,
) -> Optional[_CountPushdown]:
    """Build the footer-match count strategy, or decline.

    The listing already classified every run with the pushed predicate. Sum the
    exact ``num_rows`` of fully matched runs and read only the rest. Capture the
    scanner, not a driver-created reader: the reader's ``_UNSET`` batch-size
    sentinel does not survive pickling.
    """
    if not _can_count_from_fully_matched_chunks(list_files, scanner, pushdown):
        return None

    assert pushdown.predicate is not None

    reader_scanner: Scanner = scanner
    if isinstance(scanner, SupportsColumnPruning):
        reader_scanner = cast(Scanner, scanner.prune_columns([]))

    list_files = dataclasses.replace(
        list_files,
        # Keep the metadata-aware indexer so its fully_matched flags survive.
        # Disable packing so fully matched and partial runs stay independent.
        file_partitioner=None,
        predicate=pushdown.predicate,
        # count() needs no data columns; the reader still reads predicate
        # columns for partial runs.
        projected_columns=[],
        limit=None,
        partition_pruner=None,
        pushdown_is_final=True,
    )

    def count_rows(batch: "pa.Table") -> "pa.Table":
        # Prune before splitting: rows the scan would drop must not contribute
        # as fully matched, and the remaining indices must point at the pruned
        # manifest.
        manifest = reader_scanner.prune_input_split(_manifest_from_batch(batch))
        total_rows, to_read = _split_fully_matched_rows(manifest)
        if to_read:
            reader = reader_scanner.create_reader()
            partial_manifest = FileManifest(manifest.as_block().take(to_read))
            for table in reader.read(partial_manifest):
                total_rows += table.num_rows
        return _count_result(total_rows)

    return _CountPushdown(
        list_files=list_files,
        count_rows=count_rows,
        batch_size=_FILTERED_COUNT_BATCH_SIZE,
        num_cpus=_FILTERED_COUNT_NUM_CPUS,
    )


def _make_count_op(pushdown: _CountPushdown, read_files: ReadFiles) -> MapBatches:
    # A filtered count decodes partial runs, so it must inherit the read's
    # compute strategy and remote args (resources, runtime_env, GPUs, ...).
    # The strategy's CPU default only applies when the user did not set one.
    remote_args = dict(read_files.ray_remote_args)
    remote_args.setdefault("num_cpus", pushdown.num_cpus)
    return MapBatches(
        fn=pushdown.count_rows,
        input_dependencies=[pushdown.list_files],
        batch_format="pyarrow",
        batch_size=pushdown.batch_size,
        min_rows_per_bundled_input=pushdown.batch_size,
        zero_copy_batch=True,
        can_modify_num_rows=True,
        compute=read_files.compute,
        ray_remote_args=remote_args,
    )


class PushdownCountFiles(Rule):
    """Answer ``Dataset.count()`` without reading data when metadata can prove it.

    Selection is an explicit two-state machine:

    * **Unfiltered count.** When the scanner's row count is exact, rebuild
      ``ListFiles`` with a whole-file indexer and sum
      ``SupportsMetadata.read_metadata()`` in the parallel count pass. Listing
      does no metadata IO; each file appears exactly once.
    * **Filtered count.** When the listing emits exact
      ``FileChunk.fully_matched`` metadata, keep that metadata-aware indexer
      and sum ``num_rows`` for runs whose footer statistics prove every row
      matches the predicate. Read only the runs the footer could not settle.

    Both paths fail closed: over-claiming makes ``count()`` silently wrong,
    while declining just falls back to a real read. The scanner and indexer
    capability hooks make each component responsible for its own guarantee.
    """

    def apply(self, plan: LogicalPlan) -> LogicalPlan:  # pyrefly: ignore[bad-override]
        count = plan.dag
        if not isinstance(count, Count):
            return plan

        assert len(count.input_dependencies) == 1, len(count.input_dependencies)
        read_files = count.input_dependencies[0]

        # ``Dataset.count()`` projects the read to zero columns before counting;
        # a ``Project`` never changes the row count, so look through it.
        if isinstance(read_files, Project):
            assert len(read_files.input_dependencies) == 1
            read_files = read_files.input_dependencies[0]

        if not isinstance(read_files, ReadFiles) or read_files.block_udf is not None:
            return plan

        assert len(read_files.input_dependencies) == 1, len(
            read_files.input_dependencies
        )
        list_files = read_files.input_dependencies[0]
        assert isinstance(list_files, ListFiles), list_files

        scanner = read_files.scanner
        pushdown = derive_list_files_pushdown(scanner)

        if scanner.metadata_row_count_is_exact():
            strategy = _try_unfiltered_count_pushdown(list_files, scanner, pushdown)
        else:
            strategy = _try_filtered_count_pushdown(list_files, scanner, pushdown)

        if strategy is None:
            return plan

        return LogicalPlan(_make_count_op(strategy, read_files), plan.context)
