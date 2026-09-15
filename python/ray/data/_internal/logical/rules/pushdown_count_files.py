import dataclasses
import logging
from typing import TYPE_CHECKING, Optional

from ray.data._internal.datasource_v2.listing.file_manifest import (
    PATH_COLUMN_NAME,
    FileManifest,
)
from ray.data._internal.datasource_v2.readers.supports_metadata import (
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


class PushdownCountFiles(Rule):
    """Answer ``Dataset.count()`` from file metadata instead of reading data.

    When a ``Count`` sits directly on top of a bare DataSourceV2 ``ReadFiles``
    (optionally through a row-preserving ``Project``) whose reader implements
    :class:`~ray.data._internal.datasource_v2.readers.supports_metadata.SupportsMetadata`,
    this rule rewrites the plan to::

        Count(ReadFiles(ListFiles))  ->  MapBatches(count_rows, ListFiles)

    ``count_rows`` sums ``read_metadata()`` (e.g. Parquet-footer row counts),
    so no data columns are read. The upstream ``ListFiles`` is rebuilt with a
    plain whole-file indexer and no partitioner, so (a) each file appears in
    exactly one manifest row -- no over-counting -- and (b) listing does no
    metadata IO of its own: footers are read once, in the parallel count pass.

    Two things must hold, and each component is asked rather than type-tested.
    The scan's row count must be answerable from file metadata
    (``metadata_row_count_is_exact``), and the listing must emit each file
    exactly once (``as_whole_file_indexer``). Both hooks default to "no": a
    wrong count is silent, while declining just falls back to a real read.

    Note this *replaces* metadata-aware indexers such as the footer-based
    Parquet one, rather than reconfiguring them: those override ``list_files``
    outright, so reconfiguring is a silent no-op and they would footer-sweep
    every file during listing and pack them into a single read unit.
    """

    # Default CPU allocation per task is 1; lower it so at least 2 footer-read
    # tasks can run per core (the work is network-bound, not CPU-bound).
    _PER_TASK_NUM_CPUS_ALLOCATION = 0.5

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

        # A limit, or a filter the source cannot account for in its metadata,
        # would make the metadata ``num_rows`` an overcount. Column projection
        # is fine: it changes width, not row count.
        scanner = read_files.scanner
        if not scanner.metadata_row_count_is_exact():
            return plan

        reader = scanner.create_reader()
        if (
            not isinstance(reader, SupportsMetadata)
            or MetadataType.NUM_ROWS not in reader.available_metadata()
        ):
            return plan

        assert len(read_files.input_dependencies) == 1, len(
            read_files.input_dependencies
        )
        list_files = read_files.input_dependencies[0]
        assert isinstance(list_files, ListFiles), list_files

        # Rebuild ``ListFiles`` to list each file exactly once: disable
        # partitioning and swap in a plain whole-file indexer. Mutating the
        # existing indexer's chunker isn't enough -- a metadata-aware indexer
        # (e.g. the footer-based Parquet one) overrides ``list_files`` outright
        # and ignores its chunker, so it would keep footer-sweeping during
        # listing and could emit a path once per bin, over-counting it.
        base_indexer = list_files.file_indexer
        whole_file_indexer = base_indexer.as_whole_file_indexer()
        if whole_file_indexer is None:
            # The indexer can't promise one manifest row per file -- it may chunk
            # files or do its own metadata IO -- so leave the plan alone and let
            # ``count()`` fall back to the regular read path.
            logger.debug(
                "Skipping count pushdown: %s cannot provide a whole-file indexer",
                type(base_indexer).__name__,
            )
            return plan

        # ``ListFiles`` is frozen, so ``replace`` it with a fresh indexer.
        list_files = dataclasses.replace(
            list_files,
            file_partitioner=None,
            file_indexer=whole_file_indexer,
        )

        # ``reader`` is narrowed to ``SupportsMetadata`` by the guard above, but
        # that narrowing is lost inside the ``count_rows`` closure -- bind a
        # typed local so its declared type carries into the closure.
        metadata_reader: SupportsMetadata = reader
        batch_size: Optional[int] = metadata_reader.get_target_metadata_batch_size()

        def count_rows(batch: "pa.Table") -> "pa.Table":
            import pyarrow as pa

            assert PATH_COLUMN_NAME in batch.column_names, batch.column_names
            total_rows = 0
            for block_metadata in metadata_reader.read_metadata(FileManifest(batch)):
                total_rows += block_metadata.num_rows or 0
            return pa.table({Count.COLUMN_NAME: pa.array([total_rows])})

        count_rows_op = MapBatches(
            fn=count_rows,
            input_dependencies=[list_files],
            batch_format="pyarrow",
            batch_size=batch_size,
            min_rows_per_bundled_input=batch_size,
            zero_copy_batch=True,
            can_modify_num_rows=True,
            ray_remote_args={"num_cpus": self._PER_TASK_NUM_CPUS_ALLOCATION},
        )

        return LogicalPlan(count_rows_op, plan.context)
