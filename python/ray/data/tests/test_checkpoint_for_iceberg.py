from pathlib import Path
from unittest.mock import patch

import pyarrow as pa
import pytest
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.manifest import DataFile, DataFileContent, FileFormat
from pyiceberg.schema import Schema
from pyiceberg.typedef import Record
from pyiceberg.types import LongType, NestedField, StringType

from ray.data._internal.datasource.iceberg_datasink import (
    IcebergDatasink,
    IcebergWriteResult,
)
from ray.data._internal.execution.interfaces import TaskContext
from ray.data._internal.planner.plan_write_op import WRITE_UUID_KWARG_NAME
from ray.data._internal.savemode import SaveMode
from ray.data.block import BlockAccessor
from ray.data.checkpoint import CheckpointConfig
from ray.data.checkpoint._iceberg_checkpoint import (
    _GENERATION_PROPERTY,
    IcebergCheckpointDatasink,
    write_task_checkpoint,
)
from ray.data.checkpoint.checkpoint_filter import (
    IdColumnCheckpointManager,
    NumpyArrayBasedCheckpointFilter,
)
from ray.data.checkpoint.checkpoint_writer import BatchBasedCheckpointWriter
from ray.data.datasource.datasink import WriteResult

_WRITE_RETURN_KWARG = "_datasink_write_return"


def _data_file(
    path: str = "file:///warehouse/data.parquet", record_count: int = 2
) -> DataFile:
    data_file = DataFile.from_args(
        content=DataFileContent.DATA,
        file_path=path,
        file_format=FileFormat.PARQUET,
        partition=Record("partition", 3),
        record_count=record_count,
        file_size_in_bytes=100,
        column_sizes={1: 20},
        value_counts={1: 2},
        null_value_counts={1: 0},
        nan_value_counts={1: 0},
        lower_bounds={1: b"a"},
        upper_bounds={1: b"z"},
        key_metadata=b"key",
        split_offsets=[4],
        equality_ids=None,
        sort_order_id=0,
    )
    data_file.spec_id = 7
    return data_file


def _write_result(
    path: str = "file:///warehouse/data.parquet", record_count: int = 2
) -> IcebergWriteResult:
    return IcebergWriteResult(
        data_files=[_data_file(path, record_count)],
        schemas=[
            pa.schema([pa.field("id", pa.int64()), pa.field("value", pa.string())])
        ],
    )


def _create_catalog(tmp_path):
    warehouse = tmp_path / "warehouse"
    warehouse.mkdir()
    catalog = SqlCatalog(
        "checkpoint_catalog",
        uri=f"sqlite:///{tmp_path / 'catalog.db'}",
        warehouse=f"file://{warehouse}",
    )
    catalog.create_namespace("db")
    schema = Schema(
        NestedField(1, "id", LongType(), required=False),
        NestedField(2, "value", StringType(), required=False),
    )
    catalog.create_table("db.table", schema=schema)
    catalog_kwargs = {
        "name": "checkpoint_catalog",
        "type": "sql",
        "uri": f"sqlite:///{tmp_path / 'catalog.db'}",
        "warehouse": f"file://{warehouse}",
    }
    return catalog, catalog_kwargs


def _wrapper(tmp_path, *, delete_checkpoint_on_success=False):
    catalog, catalog_kwargs = _create_catalog(tmp_path)
    config = CheckpointConfig(
        id_column="id",
        checkpoint_path=str(tmp_path / "checkpoints"),
        delete_checkpoint_on_success=delete_checkpoint_on_success,
    )
    wrapped = IcebergCheckpointDatasink(
        IcebergDatasink("db.table", catalog_kwargs=catalog_kwargs),
        config,
    )
    wrapped.enable_checkpointing()
    return catalog, catalog_kwargs, config, wrapped


def _write_task(
    wrapped: IcebergCheckpointDatasink,
    config: CheckpointConfig,
    block: pa.Table,
    *,
    write_uuid: str,
    task_idx: int = 0,
):
    wrapped.on_write_start(block.schema)
    context = TaskContext(
        task_idx,
        "Write",
        kwargs={WRITE_UUID_KWARG_NAME: write_uuid},
    )
    result = wrapped.write([block], context)
    context.kwargs[_WRITE_RETURN_KWARG] = result
    write_task_checkpoint(
        wrapped,
        BatchBasedCheckpointWriter(config),
        BlockAccessor.for_block(block),
        context,
    )
    return context.kwargs[_WRITE_RETURN_KWARG], context


@pytest.mark.parametrize(
    "config_attr,config_value,error",
    [
        (
            "checkpoint_manager_cls",
            IdColumnCheckpointManager,
            "default checkpoint manager",
        ),
        (
            "checkpoint_filter_cls",
            NumpyArrayBasedCheckpointFilter,
            "default checkpoint filter",
        ),
    ],
)
def test_rejects_custom_restore_classes(tmp_path, config_attr, config_value, error):
    config = CheckpointConfig(
        id_column="id",
        checkpoint_path=str(tmp_path),
        **{config_attr: config_value},
    )
    wrapped = IcebergCheckpointDatasink(IcebergDatasink("db.table"), config)

    with pytest.raises(ValueError, match=error):
        wrapped.enable_checkpointing()


@pytest.mark.parametrize("mode", [SaveMode.UPSERT, SaveMode.OVERWRITE])
def test_rejects_unsupported_modes(tmp_path, mode):
    config = CheckpointConfig(id_column="id", checkpoint_path=str(tmp_path))
    wrapped = IcebergCheckpointDatasink(IcebergDatasink("db.table", mode=mode), config)

    with pytest.raises(ValueError, match="only APPEND"):
        wrapped.enable_checkpointing()


def test_rejects_reserved_snapshot_property(tmp_path):
    config = CheckpointConfig(id_column="id", checkpoint_path=str(tmp_path))
    wrapped = IcebergCheckpointDatasink(
        IcebergDatasink(
            "db.table",
            snapshot_properties={_GENERATION_PROPERTY: "user-value"},
        ),
        config,
    )

    with pytest.raises(ValueError, match="reserved"):
        wrapped.enable_checkpointing()


def test_merge_results_deduplicates_and_rejects_conflicts():
    first = _write_result("file:///same")
    duplicate = _write_result("file:///same")

    merged = IcebergCheckpointDatasink._merge_results([first], [duplicate])
    assert len(merged) == 1
    assert len(merged[0].data_files) == 1

    conflicting = _write_result("file:///same", record_count=3)
    with pytest.raises(ValueError, match="Conflicting Iceberg checkpoint metadata"):
        IcebergCheckpointDatasink._merge_results([first], [conflicting])


def test_task_checkpoint_reuses_committed_result(tmp_path):
    _, _, config, wrapped = _wrapper(tmp_path)
    writer = BatchBasedCheckpointWriter(config)
    block = BlockAccessor.for_block(pa.table({"id": [1, 2]}))
    first_context = TaskContext(
        0,
        "Write",
        kwargs={
            WRITE_UUID_KWARG_NAME: "write",
            _WRITE_RETURN_KWARG: _write_result("file:///first"),
        },
    )
    write_task_checkpoint(wrapped, writer, block, first_context)

    retry_context = TaskContext(
        0,
        "Write",
        kwargs={
            WRITE_UUID_KWARG_NAME: "write",
            _WRITE_RETURN_KWARG: _write_result("file:///orphaned-retry"),
        },
    )
    write_task_checkpoint(wrapped, writer, block, retry_context)

    restored = retry_context.kwargs[_WRITE_RETURN_KWARG]
    assert str(restored.data_files[0].file_path) == "file:///first"
    assert [
        str(loaded.result.data_files[0].file_path)
        for loaded in wrapped.state.load_active_results()
    ] == ["file:///first"]


def test_task_checkpoint_recovers_metadata_before_row_commit(tmp_path):
    _, _, config, wrapped = _wrapper(tmp_path)
    writer = BatchBasedCheckpointWriter(config)
    block = BlockAccessor.for_block(pa.table({"id": [1]}))
    artifact_id = f"{wrapped.state.generation_id}-write-0"
    pending = writer.write_pending_checkpoint(pa.array([1]), artifact_id)
    assert pending is not None
    wrapped.state.persist_task_result(
        artifact_id,
        pending.committed_path,
        _write_result("file:///first"),
    )
    context = TaskContext(
        0,
        "Write",
        kwargs={
            WRITE_UUID_KWARG_NAME: "write",
            _WRITE_RETURN_KWARG: _write_result("file:///orphaned-retry"),
        },
    )

    write_task_checkpoint(wrapped, writer, block, context)

    restored = context.kwargs[_WRITE_RETURN_KWARG]
    assert str(restored.data_files[0].file_path) == "file:///first"
    assert Path(pending.committed_path).exists()


def test_append_commits_marker_and_exact_files(tmp_path):
    catalog, _, config, wrapped = _wrapper(tmp_path)
    block = pa.table({"id": [1, 2], "value": ["a", "b"]})
    result, _ = _write_task(wrapped, config, block, write_uuid="write")
    wrapped._sink._snapshot_properties = {"user-property": "preserved"}

    wrapped.on_write_complete(WriteResult(2, block.nbytes, [result]))

    table = catalog.load_table("db.table")
    assert table.scan().to_arrow().sort_by("id").to_pydict() == {
        "id": [1, 2],
        "value": ["a", "b"],
    }
    snapshots = table.snapshots()
    assert len(snapshots) == 1
    assert snapshots[0].summary.get(_GENERATION_PROPERTY) == wrapped.state.generation_id
    assert snapshots[0].summary.get("user-property") == "preserved"
    assert wrapped._snapshot_added_file_paths(snapshots[0]) == {
        str(data_file.file_path) for data_file in result.data_files
    }


@pytest.mark.parametrize("delete_checkpoint_on_success", [False, True])
def test_empty_append_honors_cleanup_setting(tmp_path, delete_checkpoint_on_success):
    catalog, _, _, wrapped = _wrapper(
        tmp_path,
        delete_checkpoint_on_success=delete_checkpoint_on_success,
    )

    wrapped.on_write_complete(WriteResult(0, 0, []))

    assert catalog.load_table("db.table").current_snapshot() is None
    checkpoint_path = tmp_path / "checkpoints"
    assert checkpoint_path.exists() is (not delete_checkpoint_on_success)


@pytest.mark.parametrize("delete_checkpoint_on_success", [False, True])
def test_successful_append_honors_cleanup_setting(
    tmp_path, delete_checkpoint_on_success
):
    _, _, config, wrapped = _wrapper(
        tmp_path,
        delete_checkpoint_on_success=delete_checkpoint_on_success,
    )
    block = pa.table({"id": [1], "value": ["a"]})
    result, _ = _write_task(wrapped, config, block, write_uuid="write")

    wrapped.on_write_complete(WriteResult(1, block.nbytes, [result]))

    checkpoint_path = tmp_path / "checkpoints"
    assert checkpoint_path.exists() is (not delete_checkpoint_on_success)


def test_recovers_files_after_failure_before_catalog_commit(tmp_path):
    catalog, catalog_kwargs, config, wrapped = _wrapper(tmp_path)
    block = pa.table({"id": [1], "value": ["a"]})
    result, _ = _write_task(wrapped, config, block, write_uuid="write")

    with patch.object(
        IcebergDatasink,
        "on_write_complete",
        side_effect=RuntimeError("failure before catalog commit"),
    ):
        with pytest.raises(RuntimeError, match="failure before catalog commit"):
            wrapped.on_write_complete(WriteResult(1, block.nbytes, [result]))

    assert catalog.load_table("db.table").current_snapshot() is None
    retry = IcebergCheckpointDatasink(
        IcebergDatasink("db.table", catalog_kwargs=catalog_kwargs), config
    )
    retry.enable_checkpointing()
    retry.on_write_complete(WriteResult(0, 0, []))

    table = catalog.load_table("db.table")
    assert len(table.snapshots()) == 1
    assert table.scan().to_arrow().to_pydict() == {"id": [1], "value": ["a"]}


def test_recovers_ambiguous_commit_without_recommitting(tmp_path):
    catalog, catalog_kwargs, config, wrapped = _wrapper(tmp_path)
    block = pa.table({"id": [1], "value": ["a"]})
    result, _ = _write_task(wrapped, config, block, write_uuid="write")
    original = IcebergDatasink.on_write_complete

    def commit_then_raise(self, write_result):
        original(self, write_result)
        raise RuntimeError("ambiguous catalog response")

    with patch.object(IcebergDatasink, "on_write_complete", commit_then_raise):
        with pytest.raises(RuntimeError, match="ambiguous catalog response"):
            wrapped.on_write_complete(WriteResult(1, block.nbytes, [result]))

    retry = IcebergCheckpointDatasink(
        IcebergDatasink("db.table", catalog_kwargs=catalog_kwargs), config
    )
    retry.enable_checkpointing()
    second = pa.table({"id": [2], "value": ["b"]})
    retry_result, _ = _write_task(retry, config, second, write_uuid="retry")
    retry.on_write_complete(WriteResult(1, second.nbytes, [retry_result]))

    table = catalog.load_table("db.table")
    assert len(table.snapshots()) == 2
    assert table.scan().to_arrow().sort_by("id").to_pydict() == {
        "id": [1, 2],
        "value": ["a", "b"],
    }


@pytest.mark.parametrize("recover_on_retry", [False, True])
def test_late_commit_discards_uncommitted_task_checkpoints(tmp_path, recover_on_retry):
    catalog, catalog_kwargs, config, wrapped = _wrapper(tmp_path)
    committed_block = pa.table({"id": [1], "value": ["a"]})
    committed_result, _ = _write_task(
        wrapped, config, committed_block, write_uuid="committed"
    )

    wrapped._sink._snapshot_properties = {
        _GENERATION_PROPERTY: wrapped.state.generation_id
    }
    wrapped._sink.on_write_complete(
        WriteResult(1, committed_block.nbytes, [committed_result])
    )

    late_block = pa.table({"id": [2], "value": ["b"]})
    late_result, _ = _write_task(wrapped, config, late_block, write_uuid="late")

    if recover_on_retry:
        retry = IcebergCheckpointDatasink(
            IcebergDatasink("db.table", catalog_kwargs=catalog_kwargs), config
        )
        retry.enable_checkpointing()
    else:
        with pytest.raises(RuntimeError, match="late task checkpoints were discarded"):
            wrapped.on_write_complete(WriteResult(1, late_block.nbytes, [late_result]))
        retry = IcebergCheckpointDatasink(
            IcebergDatasink("db.table", catalog_kwargs=catalog_kwargs), config
        )
        retry.enable_checkpointing()

    row_checkpoints = sorted((tmp_path / "checkpoints").glob("*.parquet"))
    assert len(row_checkpoints) == 1
    assert "committed" in row_checkpoints[0].name
    assert retry.state.load_active_results() == []
    table = catalog.load_table("db.table")
    assert len(table.snapshots()) == 1
    assert table.scan().to_arrow().to_pydict() == {"id": [1], "value": ["a"]}


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
