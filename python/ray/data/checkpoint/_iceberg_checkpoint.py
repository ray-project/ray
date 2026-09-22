"""Coordinate recoverable Ray Data APPEND writes to Iceberg tables.

Row checkpoints identify source rows that completed, but they don't preserve the
``DataFile`` objects needed for an Iceberg catalog commit. This module pairs each
row checkpoint with durable task metadata and publishes the row checkpoint last.
A committed row checkpoint therefore always has matching recoverable metadata.

Each active checkpoint generation is also written to the Iceberg snapshot
summary. The marker lets a retry distinguish a failed commit from an ambiguous
response to a successful commit and prevents the same files from being appended
twice.
"""

import logging
from typing import TYPE_CHECKING, Any, Dict, Iterable, List, Optional, Set, Tuple

import pyarrow as pa

from ray.data._internal.datasource.iceberg_datasink import (
    IcebergDatasink,
    IcebergWriteResult,
)
from ray.data._internal.planner.plan_write_op import WRITE_UUID_KWARG_NAME
from ray.data._internal.savemode import SaveMode
from ray.data.block import Block, BlockAccessor
from ray.data.checkpoint._iceberg_checkpoint_state import (
    IcebergCheckpointState,
    TaskCheckpointState,
)
from ray.data.checkpoint.checkpoint_writer import BatchBasedCheckpointWriter
from ray.data.datasource.datasink import Datasink, WriteResult

if TYPE_CHECKING:
    from ray.data._internal.execution.interfaces import TaskContext
    from ray.data.checkpoint.interfaces import CheckpointConfig

logger = logging.getLogger(__name__)

_GENERATION_PROPERTY = "ray.data.checkpoint.operation-id"
_WRITE_RETURN_KWARG = "_datasink_write_return"


def _data_file_metadata(data_file: Any) -> Tuple[Any, ...]:
    """Return every persisted field used to detect same-path conflicts."""
    return (
        data_file.content,
        str(data_file.file_path),
        data_file.file_format,
        tuple(data_file.partition),
        data_file.record_count,
        data_file.file_size_in_bytes,
        data_file.column_sizes,
        data_file.value_counts,
        data_file.null_value_counts,
        data_file.nan_value_counts,
        data_file.lower_bounds,
        data_file.upper_bounds,
        data_file.key_metadata,
        data_file.split_offsets,
        data_file.equality_ids,
        data_file.sort_order_id,
        getattr(data_file, "spec_id", None),
    )


class IcebergCheckpointDatasink(Datasink[IcebergWriteResult]):
    """Add task recovery and commit detection to an Iceberg APPEND datasink.

    Workers continue to use the wrapped ``IcebergDatasink`` to create data
    files. The wrapper coordinates durable task metadata on workers and turns
    driver completion into a small state machine:

    1. Detect whether this generation's marked snapshot already exists.
    2. Reconcile any task checkpoints published after that snapshot.
    3. Otherwise merge recovered and current task results and commit them once.
    4. Mark the generation terminal, then optionally remove its namespace.

    ``enable_checkpointing`` must run before source checkpoint filtering so a
    previously committed generation can discard late, uncommitted row markers
    before those markers suppress rows in the new attempt.
    """

    def __init__(self, sink: IcebergDatasink, config: "CheckpointConfig"):
        self._sink = sink
        self._config = config
        self._state: Optional[IcebergCheckpointState] = None
        self._checkpointing_active = False

    @property
    def state(self) -> IcebergCheckpointState:
        """Return initialized durable state for the active generation."""
        if self._state is None:
            raise RuntimeError("Iceberg checkpointing has not been enabled")
        return self._state

    @property
    def checkpointing_active(self) -> bool:
        """Whether namespace validation and generation selection completed."""
        return self._checkpointing_active

    def enable_checkpointing(self) -> None:
        """Validate settings and select a recoverable generation before reads.

        If an adopted generation's snapshot is already visible, this method
        reconciles its task checkpoints against the exact files in that
        snapshot. Fully uncommitted late tasks lose their row markers so their
        rows remain eligible for the fresh generation created afterward.
        """
        if self._checkpointing_active:
            return
        if self._config.checkpoint_manager_cls is not None:
            raise ValueError(
                "Checkpointed Iceberg writes require the default checkpoint manager"
            )
        if self._config.checkpoint_filter_cls is not None:
            raise ValueError(
                "Checkpointed Iceberg writes require the default checkpoint filter"
            )
        if self._sink._mode != SaveMode.APPEND:
            raise ValueError(
                "Checkpointed Iceberg writes currently support only APPEND mode; "
                f"got {self._sink._mode.value}"
            )
        if _GENERATION_PROPERTY in self._sink._snapshot_properties:
            raise ValueError(
                f"Snapshot property {_GENERATION_PROPERTY!r} is reserved by Ray Data "
                "checkpointing"
            )

        self._sink._reload_table()
        self._state = IcebergCheckpointState(
            self._config.filesystem,
            self._config.checkpoint_path,
            table_identifier=self._sink.table_identifier,
            table_uuid=str(self._sink._table.metadata.table_uuid),
            mode=SaveMode.APPEND.value,
        )
        self.state.initialize()

        snapshot = self._find_generation_snapshot()
        if snapshot is not None:
            committed_paths = self._snapshot_added_file_paths(snapshot)
            self.state.discard_uncommitted_results(
                committed_paths,
                self._config.checkpoint_path_partition_filter,
            )
            self.state.mark_terminal()
            self.state.initialize()
        self._checkpointing_active = True

    def on_write_start(self, schema: Optional[pa.Schema] = None) -> None:
        self._sink.on_write_start(schema)

    def write(self, blocks: Iterable[Block], ctx: "TaskContext") -> IcebergWriteResult:
        return self._sink.write(blocks, ctx)

    def _find_generation_snapshot(self) -> Optional[Any]:
        """Refresh the table and find this generation's catalog marker."""
        self._sink._reload_table()
        for snapshot in self._sink._table.snapshots():
            if (
                snapshot.summary is not None
                and snapshot.summary.get(_GENERATION_PROPERTY)
                == self.state.generation_id
            ):
                return snapshot
        return None

    def _snapshot_added_file_paths(self, snapshot: Any) -> Set[str]:
        """Return the exact data-file paths added by a marked snapshot."""
        from pyiceberg.manifest import ManifestEntryStatus

        io = self._sink._table.io
        paths = set()
        for manifest in snapshot.manifests(io):
            if manifest.added_snapshot_id != snapshot.snapshot_id:
                continue
            for entry in manifest.fetch_manifest_entry(io, discard_deleted=False):
                if (
                    entry.status == ManifestEntryStatus.ADDED
                    and entry.snapshot_id == snapshot.snapshot_id
                ):
                    paths.add(str(entry.data_file.file_path))
        return paths

    @staticmethod
    def _merge_results(
        recovered: List[IcebergWriteResult], current: List[IcebergWriteResult]
    ) -> List[IcebergWriteResult]:
        """Merge task results, deduplicating only identical file metadata.

        A retried task can return a file already represented by recovered state.
        The path is the Iceberg file identity, but all persisted metadata and the
        task's associated schemas must also match. A same-path disagreement
        fails closed instead of relying on PyIceberg's path-only equality.
        """
        merged = []
        seen: Dict[str, Tuple[Tuple[Any, ...], Tuple[pa.Schema, ...]]] = {}
        for result in [*recovered, *current]:
            if result.upsert_keys is not None:
                raise ValueError(
                    "Checkpointed Iceberg writes don't support UPSERT metadata"
                )
            unique_files = []
            for data_file in result.data_files:
                path = str(data_file.file_path)
                metadata = (_data_file_metadata(data_file), tuple(result.schemas))
                previous = seen.get(path)
                if previous is not None:
                    if previous != metadata:
                        raise ValueError(
                            "Conflicting Iceberg checkpoint metadata for data file "
                            f"{path}"
                        )
                    continue
                seen[path] = metadata
                unique_files.append(data_file)
            if unique_files:
                merged.append(
                    IcebergWriteResult(data_files=unique_files, schemas=result.schemas)
                )
        return merged

    @staticmethod
    def _current_results(
        write_result: WriteResult[IcebergWriteResult],
    ) -> List[IcebergWriteResult]:
        """Return nonempty task results from the current write attempt."""
        return [
            result
            for result in write_result.write_returns
            if result is not None and result.data_files
        ]

    def _handle_visible_snapshot(self) -> bool:
        """Finalize a generation whose marked snapshot became visible late."""
        snapshot = self._find_generation_snapshot()
        if snapshot is None:
            return False

        committed_paths = self._snapshot_added_file_paths(snapshot)
        discarded = self.state.discard_uncommitted_results(
            committed_paths,
            self._config.checkpoint_path_partition_filter,
        )
        self.state.mark_terminal()
        if discarded:
            raise RuntimeError(
                "Iceberg checkpoint generation was committed while new task results "
                "were being written; late task checkpoints were discarded and will "
                "be retried"
            )
        self._cleanup_after_success()
        return True

    def _commit_with_generation_marker(
        self,
        write_result: WriteResult[IcebergWriteResult],
        merged: List[IcebergWriteResult],
    ) -> None:
        """Commit through the wrapped sink while adding the generation marker."""
        original_properties = self._sink._snapshot_properties
        self._sink._snapshot_properties = {
            **original_properties,
            _GENERATION_PROPERTY: self.state.generation_id,
        }
        try:
            self._sink.on_write_complete(
                WriteResult(
                    num_rows=write_result.num_rows,
                    size_bytes=write_result.size_bytes,
                    write_returns=merged,
                )
            )
        finally:
            self._sink._snapshot_properties = original_properties

        if self._find_generation_snapshot() is None:
            raise RuntimeError(
                "Iceberg commit succeeded without the expected Ray checkpoint marker"
            )

    def on_write_complete(self, write_result: WriteResult[IcebergWriteResult]) -> None:
        """Recover task files, commit once, and finalize checkpoint state."""
        if not self._checkpointing_active:
            self._sink.on_write_complete(write_result)
            return
        if self._handle_visible_snapshot():
            return

        recovered = [
            loaded.result
            for loaded in self.state.load_active_results(
                self._config.checkpoint_path_partition_filter
            )
        ]
        merged = self._merge_results(recovered, self._current_results(write_result))
        if not merged:
            self.state.remove_empty_generation()
            self._cleanup_after_success()
            return

        self._commit_with_generation_marker(write_result, merged)
        self.state.mark_terminal()
        self._cleanup_after_success()

    def _cleanup_after_success(self) -> None:
        """Delete checkpoint state only after destination success is confirmed."""
        if not self._config.delete_checkpoint_on_success:
            return
        try:
            self.state.delete_namespace()
        except Exception:
            logger.warning("Failed to delete Iceberg checkpoint data.", exc_info=True)

    def on_write_failed(self, error: Exception) -> None:
        self._sink.on_write_failed(error)

    def get_name(self) -> str:
        return self._sink.get_name()

    @property
    def supports_distributed_writes(self) -> bool:
        return self._sink.supports_distributed_writes

    @property
    def min_rows_per_write(self) -> Optional[int]:
        return self._sink.min_rows_per_write

    @property
    def min_bytes_per_write(self) -> Optional[int]:
        return self._sink.min_bytes_per_write


def wrap_iceberg_datasink(
    datasink: Datasink, config: Optional["CheckpointConfig"]
) -> Datasink:
    """Return a private checkpoint wrapper for an Iceberg datasink."""
    if (
        config is not None
        and isinstance(datasink, IcebergDatasink)
        and not isinstance(datasink, IcebergCheckpointDatasink)
    ):
        return IcebergCheckpointDatasink(datasink, config)
    return datasink


def write_task_checkpoint(
    datasink: IcebergCheckpointDatasink,
    checkpoint_writer: BatchBasedCheckpointWriter,
    block: BlockAccessor,
    ctx: "TaskContext",
) -> None:
    """Publish one task's destination metadata before its row checkpoint.

    The generation, write UUID, and task index form a deterministic artifact ID.
    A retry first reuses either the committed result or metadata whose final row
    marker wasn't published. New tasks write pending row IDs, publish destination
    metadata, and commit the row marker last so filtering and recovery become
    visible at the same boundary.
    """
    write_result = ctx.kwargs.get(_WRITE_RETURN_KWARG)
    if not isinstance(write_result, IcebergWriteResult):
        raise TypeError("Iceberg write task did not return IcebergWriteResult")

    write_uuid = ctx.kwargs.get(WRITE_UUID_KWARG_NAME)
    if not write_uuid:
        raise RuntimeError("Iceberg checkpoint write is missing the write UUID")
    artifact_id = f"{datasink.state.generation_id}-{write_uuid}-{ctx.task_idx}"
    task_checkpoint = datasink.state.resolve_task_checkpoint(
        artifact_id,
        datasink._config.checkpoint_path_partition_filter,
    )
    if task_checkpoint.state is TaskCheckpointState.COMMITTED:
        assert task_checkpoint.loaded_result is not None
        ctx.kwargs[_WRITE_RETURN_KWARG] = task_checkpoint.loaded_result.result
        return
    if task_checkpoint.state is TaskCheckpointState.METADATA_PUBLISHED:
        assert task_checkpoint.loaded_result is not None
        write_result = task_checkpoint.loaded_result.result
        ctx.kwargs[_WRITE_RETURN_KWARG] = write_result

    selected = block.select(columns=[datasink._config.id_column])
    id_column_data = BlockAccessor.for_block(selected).to_arrow()[
        datasink._config.id_column
    ]
    pending = checkpoint_writer.write_pending_checkpoint(
        id_column_data,
        checkpoint_id=artifact_id,
    )
    if pending is None:
        return
    if task_checkpoint.state is TaskCheckpointState.ABSENT:
        datasink.state.persist_task_result(
            artifact_id,
            pending.committed_path,
            write_result,
        )
    checkpoint_writer.commit_checkpoint(pending)
