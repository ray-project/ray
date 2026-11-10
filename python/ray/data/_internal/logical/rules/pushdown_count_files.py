import dataclasses
import logging
from typing import TYPE_CHECKING, Optional, cast

from ray.data._internal.datasource_v2.common.pushdown_utils import (
    derive_list_files_pushdown,
)
from ray.data._internal.datasource_v2.interfaces.file_manifest import (
    PATH_COLUMN_NAME,
    FileChunk,
    FileManifest,
)
from ray.data._internal.datasource_v2.interfaces.pushdown import (
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


class PushdownCountFiles(Rule):
    """Answer ``Dataset.count()`` without reading data when metadata can prove it.

    Two strategies are supported:

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

    # Default CPU allocation per task is 1; lower it so at least 2 footer-read
    # tasks can run per core (the work is network-bound, not CPU-bound).
    _PER_TASK_NUM_CPUS_ALLOCATION = 0.5

    # Manifest rows per filtered count task. Fully matched rows are cheap;
    # batching keeps task overhead down while parallelizing the partial reads.
    _FILTERED_COUNT_BATCH_SIZE = 16

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

        scanner = read_files.scanner
        pushdown = derive_list_files_pushdown(scanner)

        assert len(read_files.input_dependencies) == 1, len(
            read_files.input_dependencies
        )
        list_files = read_files.input_dependencies[0]
        assert isinstance(list_files, ListFiles), list_files

        if scanner.metadata_row_count_is_exact():
            # Unfiltered path: metadata gives an exact row count for every file.
            reader = scanner.create_reader()
            if (
                not isinstance(reader, SupportsMetadata)
                or MetadataType.NUM_ROWS not in reader.available_metadata()
            ):
                return plan

            # Rebuild ``ListFiles`` to list each file exactly once: disable
            # partitioning and swap in a plain whole-file indexer. Mutating the
            # existing indexer's chunker isn't enough -- a metadata-aware indexer
            # (e.g. the footer-based Parquet one) overrides ``list_files``
            # outright and ignores its chunker, so it would keep footer-sweeping
            # during listing and could emit a path once per bin, over-counting it.
            whole_file_indexer = list_files.file_indexer.as_whole_file_indexer()
            if whole_file_indexer is None:
                logger.debug(
                    "Skipping count pushdown: %s cannot provide a whole-file indexer",
                    type(list_files.file_indexer).__name__,
                )
                return plan

            assert (
                pushdown.limit is None
            ), "metadata_row_count_is_exact must refuse a limit"
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

            # ``reader`` is narrowed to ``SupportsMetadata`` by the guard above,
            # but that narrowing is lost inside the closure -- bind a typed local.
            metadata_reader: SupportsMetadata = reader
            batch_size: Optional[int] = metadata_reader.get_target_metadata_batch_size()

            def count_rows(batch: "pa.Table") -> "pa.Table":
                import pyarrow as pa

                assert PATH_COLUMN_NAME in batch.column_names, batch.column_names
                total_rows = 0
                for block_metadata in metadata_reader.read_metadata(
                    FileManifest(batch)
                ):
                    total_rows += block_metadata.num_rows or 0
                return pa.table({Count.COLUMN_NAME: pa.array([total_rows])})

            num_cpus = self._PER_TASK_NUM_CPUS_ALLOCATION
        elif (
            pushdown.predicate is not None
            and pushdown.limit is None
            and pushdown.partition_pruner is None
            # Synthesized columns (path / row_hash) are not materialized by the
            # empty count projection, so a predicate on them must read normally.
            and not getattr(scanner, "synthesized_columns", ())
            and list_files.file_indexer.emits_fully_matched_chunks()
        ):
            # Filtered path: the listing already classified each run with the
            # same predicate. Sum runs whose footers prove every row matches and
            # read only the rest. Capture the scanner, not a reader: a reader
            # built on the driver carries the ``_UNSET`` batch-size sentinel
            # through pickling, which breaks its identity check on the worker.
            reader_scanner: Scanner = scanner
            if isinstance(scanner, SupportsColumnPruning):
                reader_scanner = cast(Scanner, scanner.prune_columns([]))

            list_files = dataclasses.replace(
                list_files,
                # Keep the metadata-aware indexer so its fully_matched flags
                # survive. Disable packing so each run stays independently
                # countable/readable.
                file_partitioner=None,
                predicate=pushdown.predicate,
                # ``count()`` needs no data columns; the reader still reads
                # predicate columns for partial runs.
                projected_columns=[],
                limit=None,
                partition_pruner=None,
                pushdown_is_final=True,
            )

            batch_size = self._FILTERED_COUNT_BATCH_SIZE

            def count_rows(batch: "pa.Table") -> "pa.Table":
                import pyarrow as pa

                assert PATH_COLUMN_NAME in batch.column_names, batch.column_names
                manifest = FileManifest(batch)
                total_rows = 0
                partial_indices = []
                for idx, chunk_metadata in enumerate(manifest.file_chunk_metadatas):
                    if chunk_metadata is not None:
                        chunk = FileChunk.from_metadata(chunk_metadata)
                        if chunk.fully_matched:
                            total_rows += chunk.num_rows
                            continue
                    partial_indices.append(idx)

                if partial_indices:
                    reader = reader_scanner.create_reader()
                    partial_manifest = FileManifest(batch.take(partial_indices))
                    for table in reader.read(partial_manifest):
                        total_rows += table.num_rows

                return pa.table({Count.COLUMN_NAME: pa.array([total_rows])})

            # This path may decode partial runs, so reserve a full CPU.
            num_cpus = 1.0
        else:
            return plan

        count_rows_op = MapBatches(
            fn=count_rows,
            input_dependencies=[list_files],
            batch_format="pyarrow",
            batch_size=batch_size,
            min_rows_per_bundled_input=batch_size,
            zero_copy_batch=True,
            can_modify_num_rows=True,
            ray_remote_args={"num_cpus": num_cpus},
        )

        return LogicalPlan(count_rows_op, plan.context)
