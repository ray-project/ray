import dataclasses
import logging
from typing import TYPE_CHECKING, Optional

from ray.data._internal.datasource_v2.common.pushdown_utils import (
    derive_list_files_pushdown,
)
from ray.data._internal.datasource_v2.interfaces.file_manifest import (
    PATH_COLUMN_NAME,
    FileManifest,
)
from ray.data._internal.datasource_v2.interfaces.pushdown import ListFilesPushdown
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
# can run per core.
_COUNT_NUM_CPUS = 0.5


def _manifest_from_batch(batch: "pa.Table") -> FileManifest:
    assert PATH_COLUMN_NAME in batch.column_names, batch.column_names
    return FileManifest(batch)


def _count_result(total_rows: int) -> "pa.Table":
    import pyarrow as pa

    return pa.table({Count.COLUMN_NAME: pa.array([total_rows])})


def _try_count_from_metadata(
    list_files: ListFiles,
    scanner: Scanner,
    pushdown: ListFilesPushdown,
    read_files: ReadFiles,
) -> Optional[MapBatches]:
    """Build a pass that counts each file from its footer, or decline.

    Precondition: ``scanner.metadata_row_count_is_exact()``. The listing is
    rebuilt with a whole-file indexer so each file appears exactly once and
    listing does no metadata IO of its own.
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
    batch_size = metadata_reader.get_target_metadata_batch_size()

    def count_rows(batch: "pa.Table") -> "pa.Table":
        # Apply the same scan-side pruning the read path does. Exact row counts
        # alone do not account for scanners that override ``prune_input_split``.
        manifest = scanner.prune_input_split(_manifest_from_batch(batch))
        total_rows = 0
        for block_metadata in metadata_reader.read_metadata(manifest):
            total_rows += block_metadata.num_rows or 0
        return _count_result(total_rows)

    # This task only reads footers, but it still runs in the read's
    # environment: keep its resources and runtime_env, and lower only the CPU
    # share the read did not pin itself.
    remote_args = dict(read_files.ray_remote_args)
    remote_args.setdefault("num_cpus", _COUNT_NUM_CPUS)

    return MapBatches(
        fn=count_rows,
        input_dependencies=[list_files],
        batch_format="pyarrow",
        batch_size=batch_size,
        min_rows_per_bundled_input=batch_size,
        zero_copy_batch=True,
        can_modify_num_rows=True,
        compute=read_files.compute,
        ray_remote_args=remote_args,
    )


class PushdownCountFiles(Rule):
    """Answer ``Dataset.count()`` without reading data when metadata can prove it.

    Applies only when the scanner's row count is exact, i.e. no row-reducing
    predicate or limit was pushed. ``Dataset.count()`` projects the read to zero
    columns first, so the plan is ``Count(Project([])(ReadFiles(ListFiles)))``;
    the rule looks through the ``Project`` and replaces the read with a parallel
    pass that sums ``SupportsMetadata`` row counts.

    A *filtered* count is deliberately not rewritten here. It reaches
    ``ReadFiles`` with an empty projection and a pushed predicate, and
    ``ParquetFileReader`` answers the runs whose footer statistics proved
    ``fully_matched`` without opening them. Keeping that in the read path means
    one implementation of predicate push-down, input pruning, and task
    resources, instead of a second one in the rule.

    Fails open: over-claiming makes ``count()`` silently wrong, while declining
    just falls back to the read that would have happened.
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
        if not scanner.metadata_row_count_is_exact():
            return plan

        count_op = _try_count_from_metadata(
            list_files,
            scanner,
            derive_list_files_pushdown(scanner),
            read_files,
        )
        if count_op is None:
            return plan

        return LogicalPlan(count_op, plan.context)
