import copy
import dataclasses
from typing import TYPE_CHECKING, Optional

from ray.data._internal.datasource_v2.chunkers.file_chunker import (
    WholeFileChunker,
)
from ray.data._internal.datasource_v2.listing.file_indexer import (
    NonSamplingFileIndexer,
)
from ray.data._internal.datasource_v2.listing.file_manifest import (
    PATH_COLUMN_NAME,
    FileManifest,
)
from ray.data._internal.datasource_v2.readers.supports_metadata import (
    MetadataType,
    SupportsMetadata,
)
from ray.data._internal.datasource_v2.scanners.arrow_file_scanner import (
    ArrowFileScanner,
)
from ray.data._internal.logical.interfaces import LogicalPlan, Rule
from ray.data._internal.logical.operators.count_operator import Count
from ray.data._internal.logical.operators.map_operator import MapBatches, Project
from ray.data._internal.logical.operators.read_operator import ListFiles, ReadFiles

if TYPE_CHECKING:
    import pyarrow as pa


class PushdownCountFiles(Rule):
    """Answer ``Dataset.count()`` from file metadata instead of reading data.

    When a ``Count`` sits directly on top of a bare DataSourceV2 ``ReadFiles``
    (optionally through a row-preserving ``Project``) whose reader implements
    :class:`~ray.data._internal.datasource_v2.readers.supports_metadata.SupportsMetadata`,
    this rule rewrites the plan to::

        Count(ReadFiles(ListFiles))  ->  MapBatches(count_rows, ListFiles)

    ``count_rows`` sums ``read_metadata()`` (e.g. Parquet-footer row counts),
    so no data columns are read. The upstream ``ListFiles`` is rebuilt to list
    each file exactly once (``WholeFileChunker``, no partitioner) so footers are
    read once, in the parallel count pass rather than during listing.
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

        # A row-reducing pushdown on the scanner would make the footer's
        # ``num_rows`` an overcount. Column projection is fine (rows unaffected).
        scanner = read_files.scanner
        if not isinstance(scanner, ArrowFileScanner):
            return plan
        if (
            scanner.predicate is not None
            or scanner.partition_predicate is not None
            or scanner.limit is not None
        ):
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
        # partitioning and use the ``WholeFileChunker`` (otherwise a file could
        # appear once per chunk, in different batches, and be over-counted).
        # ``ListFiles`` is frozen, so ``replace`` a copy with a fresh indexer.
        count_indexer = copy.deepcopy(list_files.file_indexer)
        assert isinstance(count_indexer, NonSamplingFileIndexer), type(count_indexer)
        count_indexer._file_chunker = WholeFileChunker()
        list_files = dataclasses.replace(
            list_files,
            file_partitioner=None,
            file_indexer=count_indexer,
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
