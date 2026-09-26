"""Coordinate row checkpoints with Iceberg APPEND snapshot commits."""

import logging
import uuid
from typing import TYPE_CHECKING, Iterable, Optional

from ray.data._internal.datasource.iceberg_datasink import (
    IcebergDatasink,
    IcebergWriteResult,
)
from ray.data._internal.savemode import SaveMode
from ray.data.block import Block
from ray.data.checkpoint._iceberg_checkpoint_state import IcebergCheckpointState
from ray.data.datasource.datasink import Datasink, WriteResult

if TYPE_CHECKING:
    import pyarrow as pa

    from ray.data._internal.execution.interfaces import TaskContext
    from ray.data.checkpoint.interfaces import CheckpointConfig

logger = logging.getLogger(__name__)

ICEBERG_CHECKPOINT_OPERATION_ID_PROPERTY = "ray.data.checkpoint.operation-id"


class IcebergCheckpointDatasink(Datasink[IcebergWriteResult]):
    """Commit row checkpoints only after confirming an Iceberg snapshot.

    Worker writes still delegate to ``IcebergDatasink``. Their row checkpoints
    remain pending until the driver confirms a snapshot marked with the current
    operation ID. On a later execution, pending checkpoints with a visible
    marker are promoted; those without one are discarded for recomputation.
    """

    def __init__(self, sink: IcebergDatasink, config: "CheckpointConfig") -> None:
        self._sink = sink
        self._config = config
        self._state = IcebergCheckpointState(config.checkpoint_path, config.filesystem)
        self._operation_id: Optional[str] = None

    def enable_checkpointing(self) -> None:
        """Validate the protocol, resolve pending operations, and start one."""
        if self._operation_id is not None:
            return
        self._validate_configuration()

        self._sink._reload_table()
        table_uuid = str(self._sink._table.metadata.table_uuid)
        self._state.ensure_namespace(
            self._sink.table_identifier,
            table_uuid,
            mode=self._sink._mode.value,
        )

        pending_operations = self._state.list_pending_operations()
        committed_operations = self._marked_operation_ids()
        for operation_id in pending_operations:
            if operation_id in committed_operations:
                self._state.promote_operation(operation_id)
            else:
                self._state.discard_operation(operation_id)

        self._operation_id = uuid.uuid4().hex

    @property
    def operation_id(self) -> str:
        """Return the active operation ID after checkpointing is enabled."""
        self._require_enabled()
        assert self._operation_id is not None
        return self._operation_id

    @property
    def checkpoint_state(self) -> IcebergCheckpointState:
        """Return the operation state used by this datasink."""
        return self._state

    def on_write_start(self, schema: Optional["pa.Schema"] = None) -> None:
        self._require_enabled()
        self._sink.on_write_start(schema)

    def write(self, blocks: Iterable[Block], ctx: "TaskContext") -> IcebergWriteResult:
        self._require_enabled()
        return self._sink.write(blocks, ctx)

    def on_write_complete(self, write_result: WriteResult[IcebergWriteResult]) -> None:
        """Commit current files, confirm the snapshot, and promote row IDs."""
        operation_id = self.operation_id
        if not self._has_data_files(write_result):
            self._sink.on_write_complete(write_result)
            self._cleanup_after_success()
            return

        original_properties = self._sink._snapshot_properties
        self._sink._snapshot_properties = {
            **original_properties,
            ICEBERG_CHECKPOINT_OPERATION_ID_PROPERTY: operation_id,
        }
        try:
            self._sink.on_write_complete(write_result)
        finally:
            self._sink._snapshot_properties = original_properties

        if not self._operation_snapshot_exists(operation_id):
            raise RuntimeError(
                "Iceberg commit completed without the expected Ray checkpoint "
                "operation marker"
            )

        self._state.promote_operation(operation_id)
        self._cleanup_after_success()

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

    def _require_enabled(self) -> None:
        if self._operation_id is None:
            raise RuntimeError("Iceberg checkpointing has not been enabled")

    def _validate_configuration(self) -> None:
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
        if ICEBERG_CHECKPOINT_OPERATION_ID_PROPERTY in self._sink._snapshot_properties:
            raise ValueError(
                f"Snapshot property {ICEBERG_CHECKPOINT_OPERATION_ID_PROPERTY!r} "
                "is reserved by Ray Data checkpointing"
            )

    def _marked_operation_ids(self) -> set[str]:
        operation_ids = set()
        for snapshot in self._sink._table.snapshots():
            if snapshot.summary is None:
                continue
            operation_id = snapshot.summary.get(
                ICEBERG_CHECKPOINT_OPERATION_ID_PROPERTY
            )
            if operation_id is not None:
                operation_ids.add(operation_id)
        return operation_ids

    def _operation_snapshot_exists(self, operation_id: str) -> bool:
        self._sink._reload_table()
        return operation_id in self._marked_operation_ids()

    @staticmethod
    def _has_data_files(write_result: WriteResult[IcebergWriteResult]) -> bool:
        return any(
            result is not None and bool(result.data_files)
            for result in write_result.write_returns
        )

    def _cleanup_after_success(self) -> None:
        if not self._config.delete_checkpoint_on_success:
            return
        try:
            self._state.delete()
        except Exception:
            logger.warning("Failed to delete Iceberg checkpoint data.", exc_info=True)


def wrap_iceberg_datasink(
    datasink: Datasink, config: Optional["CheckpointConfig"]
) -> Datasink:
    """Wrap an Iceberg datasink when checkpointing is configured."""
    if config is not None and isinstance(datasink, IcebergDatasink):
        return IcebergCheckpointDatasink(datasink, config)
    return datasink
