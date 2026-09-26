import os
import sys
from types import SimpleNamespace

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from pyarrow.fs import FileType

from ray.data._internal.datasource.iceberg_datasink import (
    IcebergDatasink,
    IcebergWriteResult,
)
from ray.data._internal.execution.interfaces import TaskContext
from ray.data._internal.planner.checkpoint.plan_write_op import (
    WRITE_UUID_KWARG_NAME,
    _generate_iceberg_write_checkpoint_transform,
)
from ray.data._internal.savemode import SaveMode
from ray.data.checkpoint import CheckpointConfig
from ray.data.checkpoint._iceberg_checkpoint import (
    ICEBERG_CHECKPOINT_OPERATION_ID_PROPERTY,
    IcebergCheckpointDatasink,
    wrap_iceberg_datasink,
)
from ray.data.checkpoint._iceberg_checkpoint_state import (
    IcebergCheckpointState,
    build_task_checkpoint_id,
)
from ray.data.checkpoint.checkpoint_writer import BatchBasedCheckpointWriter
from ray.data.context import DataContext
from ray.data.datasource.datasink import WriteResult

pytestmark = pytest.mark.usefixtures("restore_data_context")

_ID_COLUMN = "id"
_TABLE_IDENTIFIER = "db.table"
_TABLE_UUID = "table-uuid"
_WRITE_ID = "c" * 32
_OLD_OPERATION_1 = "a" * 32
_OLD_OPERATION_2 = "b" * 32


class _FakeSnapshot:
    def __init__(self, summary):
        self.summary = summary


class _FakeTable:
    def __init__(self, table_uuid):
        self.metadata = SimpleNamespace(table_uuid=table_uuid)
        self._snapshots = []

    def snapshots(self):
        return list(self._snapshots)


class _FakeIcebergSink:
    def __init__(
        self,
        *,
        table_uuid=_TABLE_UUID,
        mode=SaveMode.APPEND,
        snapshot_properties=None,
        commit_behavior="success",
    ):
        self.table_identifier = _TABLE_IDENTIFIER
        self._mode = mode
        self._snapshot_properties = snapshot_properties or {}
        self._table = _FakeTable(table_uuid)
        self.commit_behavior = commit_behavior
        self.reload_count = 0
        self.completed_results = []
        self.commit_properties = []
        self.started_schemas = []
        self.failed_errors = []
        self.written_result = IcebergWriteResult(data_files=[object()])

    def _reload_table(self):
        self.reload_count += 1

    def on_write_start(self, schema=None):
        self.started_schemas.append(schema)

    def write(self, blocks, ctx):
        list(blocks)
        return self.written_result

    def on_write_complete(self, write_result):
        self.completed_results.append(write_result)
        if not _has_data_files(write_result):
            return

        properties = dict(self._snapshot_properties)
        self.commit_properties.append(properties)
        if self.commit_behavior == "fail":
            raise RuntimeError("catalog commit failed")
        if self.commit_behavior == "missing_marker":
            properties.pop(ICEBERG_CHECKPOINT_OPERATION_ID_PROPERTY, None)
        self._table._snapshots.append(_FakeSnapshot(properties))
        if self.commit_behavior == "commit_then_raise":
            raise RuntimeError("ambiguous catalog response")

    def on_write_failed(self, error):
        self.failed_errors.append(error)

    def get_name(self):
        return "Iceberg"

    @property
    def supports_distributed_writes(self):
        return True

    @property
    def min_rows_per_write(self):
        return 10

    @property
    def min_bytes_per_write(self):
        return 20


def _has_data_files(write_result):
    return any(
        result is not None and result.data_files
        for result in write_result.write_returns
    )


def _checkpoint_config(tmp_path, *, delete_checkpoint_on_success=False):
    return CheckpointConfig(
        id_column=_ID_COLUMN,
        checkpoint_path=str(tmp_path / "checkpoints"),
        delete_checkpoint_on_success=delete_checkpoint_on_success,
    )


def _write_result(*, with_files=True):
    task_result = IcebergWriteResult(data_files=[object()] if with_files else [])
    return WriteResult(
        num_rows=1 if with_files else 0, size_bytes=1, write_returns=[task_result]
    )


def _write_pending(config, operation_id, task_index, ids=(1,)):
    writer = BatchBasedCheckpointWriter(config)
    checkpoint_id = build_task_checkpoint_id(operation_id, _WRITE_ID, task_index)
    pending = writer.write_pending_checkpoint(pa.array(ids), checkpoint_id)
    assert pending is not None
    return pending


def _initialize_namespace(config, sink):
    state = IcebergCheckpointState(config.checkpoint_path, config.filesystem)
    state.ensure_namespace(
        sink.table_identifier,
        str(sink._table.metadata.table_uuid),
    )
    return state


def test_wrap_iceberg_datasink(tmp_path):
    config = _checkpoint_config(tmp_path)
    sink = IcebergDatasink(_TABLE_IDENTIFIER)

    wrapped = wrap_iceberg_datasink(sink, config)
    assert isinstance(wrapped, IcebergCheckpointDatasink)
    assert wrap_iceberg_datasink(sink, None) is sink
    assert wrap_iceberg_datasink(wrapped, config) is wrapped


@pytest.mark.parametrize(
    ("mode", "manager", "filter_cls", "properties", "match"),
    [
        (SaveMode.UPSERT, None, None, {}, "only APPEND"),
        (SaveMode.OVERWRITE, None, None, {}, "only APPEND"),
        (SaveMode.APPEND, object(), None, {}, "default checkpoint manager"),
        (SaveMode.APPEND, None, object(), {}, "default checkpoint filter"),
        (
            SaveMode.APPEND,
            None,
            None,
            {ICEBERG_CHECKPOINT_OPERATION_ID_PROPERTY: "user-value"},
            "is reserved",
        ),
    ],
)
def test_enable_checkpointing_rejects_unsupported_configuration(
    tmp_path, mode, manager, filter_cls, properties, match
):
    config = _checkpoint_config(tmp_path)
    config.checkpoint_manager_cls = manager
    config.checkpoint_filter_cls = filter_cls
    sink = _FakeIcebergSink(mode=mode, snapshot_properties=properties)
    wrapper = IcebergCheckpointDatasink(sink, config)

    with pytest.raises(ValueError, match=match):
        wrapper.enable_checkpointing()
    assert sink.reload_count == 0


def test_enable_checkpointing_rejects_namespace_identity_mismatch(tmp_path):
    config = _checkpoint_config(tmp_path)
    first = IcebergCheckpointDatasink(_FakeIcebergSink(), config)
    first.enable_checkpointing()

    second = IcebergCheckpointDatasink(
        _FakeIcebergSink(table_uuid="different-table-uuid"), config
    )
    with pytest.raises(ValueError, match="table_uuid mismatch"):
        second.enable_checkpointing()


def test_enable_checkpointing_resolves_all_pending_operations(tmp_path):
    config = _checkpoint_config(tmp_path)
    sink = _FakeIcebergSink()
    _initialize_namespace(config, sink)
    committed_operation = _write_pending(config, _OLD_OPERATION_1, 0)
    discarded_operation = _write_pending(config, _OLD_OPERATION_2, 0)
    sink._table._snapshots.append(
        _FakeSnapshot({ICEBERG_CHECKPOINT_OPERATION_ID_PROPERTY: _OLD_OPERATION_1})
    )

    wrapper = IcebergCheckpointDatasink(sink, config)
    wrapper.enable_checkpointing()

    assert (
        config.filesystem.get_file_info(committed_operation.pending_path).type
        == FileType.NotFound
    )
    assert (
        config.filesystem.get_file_info(committed_operation.committed_path).type
        == FileType.File
    )
    assert (
        config.filesystem.get_file_info(discarded_operation.pending_path).type
        == FileType.NotFound
    )
    assert wrapper.operation_id not in {_OLD_OPERATION_1, _OLD_OPERATION_2}


def test_successful_commit_marks_snapshot_then_promotes_rows(tmp_path):
    config = _checkpoint_config(tmp_path)
    original_properties = {"user-property": "value"}
    sink = _FakeIcebergSink(snapshot_properties=original_properties)
    wrapper = IcebergCheckpointDatasink(sink, config)
    wrapper.enable_checkpointing()
    pending = _write_pending(config, wrapper.operation_id, 0)
    write_result = _write_result()

    wrapper.on_write_complete(write_result)

    assert sink.completed_results == [write_result]
    assert sink.completed_results[0] is write_result
    assert sink.commit_properties == [
        {
            "user-property": "value",
            ICEBERG_CHECKPOINT_OPERATION_ID_PROPERTY: wrapper.operation_id,
        }
    ]
    assert sink._snapshot_properties is original_properties
    assert (
        config.filesystem.get_file_info(pending.pending_path).type == FileType.NotFound
    )
    assert config.filesystem.get_file_info(pending.committed_path).type == FileType.File


@pytest.mark.parametrize(
    ("commit_behavior", "match"),
    [
        ("fail", "catalog commit failed"),
        ("missing_marker", "without the expected.*operation marker"),
    ],
)
def test_commit_failure_leaves_rows_pending(tmp_path, commit_behavior, match):
    config = _checkpoint_config(tmp_path)
    original_properties = {"user-property": "value"}
    sink = _FakeIcebergSink(
        snapshot_properties=original_properties,
        commit_behavior=commit_behavior,
    )
    wrapper = IcebergCheckpointDatasink(sink, config)
    wrapper.enable_checkpointing()
    pending = _write_pending(config, wrapper.operation_id, 0)

    with pytest.raises(RuntimeError, match=match):
        wrapper.on_write_complete(_write_result())

    assert sink._snapshot_properties is original_properties
    assert config.filesystem.get_file_info(pending.pending_path).type == FileType.File
    assert (
        config.filesystem.get_file_info(pending.committed_path).type
        == FileType.NotFound
    )


def test_ambiguous_commit_is_promoted_on_next_execution(tmp_path):
    config = _checkpoint_config(tmp_path)
    sink = _FakeIcebergSink(commit_behavior="commit_then_raise")
    first = IcebergCheckpointDatasink(sink, config)
    first.enable_checkpointing()
    pending = _write_pending(config, first.operation_id, 0)

    with pytest.raises(RuntimeError, match="ambiguous catalog response"):
        first.on_write_complete(_write_result())
    assert len(sink._table.snapshots()) == 1
    assert config.filesystem.get_file_info(pending.pending_path).type == FileType.File

    sink.commit_behavior = "success"
    retry = IcebergCheckpointDatasink(sink, config)
    retry.enable_checkpointing()
    retry.on_write_complete(_write_result(with_files=False))

    assert config.filesystem.get_file_info(pending.committed_path).type == FileType.File
    assert len(sink._table.snapshots()) == 1


def test_retry_completes_partial_operation_promotion(tmp_path):
    config = _checkpoint_config(tmp_path)
    sink = _FakeIcebergSink()
    _initialize_namespace(config, sink)
    already_committed = _write_pending(config, _OLD_OPERATION_1, 0)
    still_pending = _write_pending(config, _OLD_OPERATION_1, 1)
    config.filesystem.move(
        already_committed.pending_path, already_committed.committed_path
    )
    sink._table._snapshots.append(
        _FakeSnapshot({ICEBERG_CHECKPOINT_OPERATION_ID_PROPERTY: _OLD_OPERATION_1})
    )

    wrapper = IcebergCheckpointDatasink(sink, config)
    wrapper.enable_checkpointing()

    assert (
        config.filesystem.get_file_info(already_committed.committed_path).type
        == FileType.File
    )
    assert (
        config.filesystem.get_file_info(still_pending.pending_path).type
        == FileType.NotFound
    )
    assert (
        config.filesystem.get_file_info(still_pending.committed_path).type
        == FileType.File
    )


@pytest.mark.parametrize("delete_checkpoint_on_success", [False, True])
def test_empty_write_cleanup(tmp_path, delete_checkpoint_on_success):
    config = _checkpoint_config(
        tmp_path,
        delete_checkpoint_on_success=delete_checkpoint_on_success,
    )
    sink = _FakeIcebergSink()
    wrapper = IcebergCheckpointDatasink(sink, config)
    wrapper.enable_checkpointing()

    wrapper.on_write_complete(_write_result(with_files=False))

    assert sink._table.snapshots() == []
    checkpoint_type = config.filesystem.get_file_info(
        wrapper.checkpoint_state.checkpoint_path
    ).type
    expected = FileType.NotFound if delete_checkpoint_on_success else FileType.Directory
    assert checkpoint_type == expected


def test_wrapper_delegates_datasink_behavior(tmp_path):
    config = _checkpoint_config(tmp_path)
    sink = _FakeIcebergSink()
    wrapper = IcebergCheckpointDatasink(sink, config)
    schema = pa.schema([(_ID_COLUMN, pa.int64())])
    task_context = TaskContext(task_idx=0, op_name="test")

    with pytest.raises(RuntimeError, match="has not been enabled"):
        wrapper.on_write_start(schema)
    wrapper.enable_checkpointing()
    wrapper.on_write_start(schema)
    assert (
        wrapper.write([pa.table({_ID_COLUMN: [1]})], task_context)
        is sink.written_result
    )
    error = RuntimeError("failed")
    wrapper.on_write_failed(error)

    assert sink.started_schemas == [schema]
    assert sink.failed_errors == [error]
    assert wrapper.get_name() == "Iceberg"
    assert wrapper.supports_distributed_writes
    assert wrapper.min_rows_per_write == 10
    assert wrapper.min_bytes_per_write == 20


def test_iceberg_post_write_transform_publishes_pending_rows(tmp_path):
    config = _checkpoint_config(tmp_path)
    data_context = DataContext.get_current()
    data_context.checkpoint_config = config
    sink = _FakeIcebergSink()
    wrapper = IcebergCheckpointDatasink(sink, config)
    wrapper.enable_checkpointing()
    checkpoint_writer = BatchBasedCheckpointWriter(config)
    transform = _generate_iceberg_write_checkpoint_transform(
        data_context, wrapper, checkpoint_writer
    )
    task_context = TaskContext(task_idx=3, op_name="test")
    task_context.kwargs[WRITE_UUID_KWARG_NAME] = _WRITE_ID
    write_return = IcebergWriteResult(data_files=[object()])
    block = pa.table({_ID_COLUMN: [1, 2], "value": ["a", "b"]})

    def written_blocks():
        yield block
        task_context.kwargs["_datasink_write_return"] = write_return

    output = list(transform._apply_transform(task_context, written_blocks()))

    assert len(output) == 1 and output[0].equals(block)
    assert task_context.kwargs["_datasink_write_return"] is write_return
    checkpoint_id = build_task_checkpoint_id(wrapper.operation_id, _WRITE_ID, 3)
    pending_path = os.path.join(
        config.checkpoint_path, f"{checkpoint_id}.pending.parquet"
    )
    committed_path = os.path.join(config.checkpoint_path, f"{checkpoint_id}.parquet")
    assert config.filesystem.get_file_info(pending_path).type == FileType.File
    assert config.filesystem.get_file_info(committed_path).type == FileType.NotFound
    assert pq.read_table(pending_path).column(_ID_COLUMN).to_pylist() == [1, 2]


def test_iceberg_post_write_transform_requires_id_column(tmp_path):
    config = _checkpoint_config(tmp_path)
    data_context = DataContext.get_current()
    data_context.checkpoint_config = config
    wrapper = IcebergCheckpointDatasink(_FakeIcebergSink(), config)
    wrapper.enable_checkpointing()
    transform = _generate_iceberg_write_checkpoint_transform(
        data_context,
        wrapper,
        BatchBasedCheckpointWriter(config),
    )
    task_context = TaskContext(task_idx=0, op_name="test")
    task_context.kwargs[WRITE_UUID_KWARG_NAME] = _WRITE_ID

    with pytest.raises(ValueError, match="ID column id is absent"):
        list(
            transform._apply_transform(
                task_context,
                [pa.table({"value": ["missing-id"]})],
            )
        )


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
