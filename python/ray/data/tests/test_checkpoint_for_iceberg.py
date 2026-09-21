import os
from types import SimpleNamespace
from unittest.mock import patch

import pyarrow as pa
import pytest
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.manifest import DataFile, DataFileContent, FileFormat
from pyiceberg.schema import Schema
from pyiceberg.typedef import Record
from pyiceberg.types import LongType, NestedField, StringType

import ray
from ray.data._internal.datasource.iceberg_datasink import (
    IcebergDatasink,
    IcebergWriteResult,
)
from ray.data._internal.execution.interfaces import TaskContext
from ray.data._internal.savemode import SaveMode
from ray.data.block import BlockAccessor
from ray.data.checkpoint import CheckpointConfig
from ray.data.checkpoint._iceberg_checkpoint import (
    _GENERATION_PROPERTY,
    IcebergCheckpointCoordinator,
    IcebergCheckpointDatasink,
    _TaskCheckpointState,
    write_task_checkpoint,
)
from ray.data.checkpoint._iceberg_checkpoint_serialization import (
    deserialize_write_result,
    serialize_write_result,
)
from ray.data.checkpoint.checkpoint_filter import (
    IdColumnCheckpointManager,
    NumpyArrayBasedCheckpointFilter,
)
from ray.data.checkpoint.checkpoint_writer import BatchBasedCheckpointWriter
from ray.data.checkpoint.load_checkpoint_callback import LoadCheckpointCallback
from ray.data.datasource.datasink import WriteResult


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


def _write_result(path: str = "file:///warehouse/data.parquet") -> IcebergWriteResult:
    return IcebergWriteResult(
        data_files=[_data_file(path)],
        schemas=[
            pa.schema([pa.field("id", pa.int64()), pa.field("value", pa.string())])
        ],
    )


def _coordinator(tmp_path, table_uuid="table-uuid", **config_kwargs):
    config = CheckpointConfig(
        id_column="id", checkpoint_path=str(tmp_path), **config_kwargs
    )
    sink = SimpleNamespace(table_identifier="db.table")
    coordinator = IcebergCheckpointCoordinator(config, sink)
    coordinator.initialize(table_uuid)
    return config, coordinator


def _publish_task(config, coordinator, artifact_suffix="task", result=None):
    result = result or _write_result()
    artifact_id = f"{coordinator.generation_id}-{artifact_suffix}"
    writer = BatchBasedCheckpointWriter(config)
    pending = writer.write_pending_checkpoint(pa.array([1, 2]), artifact_id)
    assert pending is not None
    coordinator.persist_task_result(artifact_id, pending.committed_path, result)
    writer.commit_checkpoint(pending)
    return pending.committed_path


def test_write_result_arrow_round_trip():
    result = _write_result()
    restored = deserialize_write_result(serialize_write_result(result))

    assert len(restored.data_files) == 1
    restored_file = restored.data_files[0]
    assert str(restored_file.file_path) == str(result.data_files[0].file_path)
    assert list(restored_file.partition) == ["partition", 3]
    assert restored_file.spec_id == 7
    assert restored_file.lower_bounds == {1: b"a"}
    assert restored.schemas == result.schemas


def test_write_result_rejects_upsert_keys():
    result = _write_result()
    result.upsert_keys = pa.table({"id": [1]})
    with pytest.raises(ValueError, match="UPSERT"):
        serialize_write_result(result)


def test_manifest_adoption_terminal_and_new_generation(tmp_path):
    config, coordinator = _coordinator(tmp_path)
    generation_id = coordinator.generation_id
    _publish_task(config, coordinator)

    adopted = IcebergCheckpointCoordinator(
        config, SimpleNamespace(table_identifier="db.table")
    )
    adopted.initialize("table-uuid")
    assert adopted.generation_id == generation_id
    assert len(adopted.load_active_results()) == 1

    adopted.mark_terminal()
    fresh = IcebergCheckpointCoordinator(
        config, SimpleNamespace(table_identifier="db.table")
    )
    fresh.initialize("table-uuid")
    assert fresh.generation_id != generation_id
    assert fresh.load_active_results() == []
    fresh.discard_empty_generation()


def test_interrupted_manifest_publication_preserves_previous_version(tmp_path):
    config, coordinator = _coordinator(tmp_path)
    generation_id = coordinator.generation_id
    manifest_dir = tmp_path / "_iceberg" / "manifests"
    original_manifest = manifest_dir / "00000000000000000000.json"
    assert original_manifest.exists()

    (manifest_dir / "00000000000000000001.json.tmp.interrupted").write_text("{")
    (manifest_dir / "00000000000000000001.json").write_text("{")
    adopted = IcebergCheckpointCoordinator(
        config, SimpleNamespace(table_identifier="db.table")
    )
    adopted.initialize("table-uuid")

    assert adopted.generation_id == generation_id
    assert original_manifest.exists()
    adopted.mark_terminal()
    assert original_manifest.exists()
    assert (manifest_dir / "00000000000000000002.json").exists()
    assert len(list(manifest_dir.glob("*.json"))) == 2


def test_namespace_identity_mismatch(tmp_path):
    config, _ = _coordinator(tmp_path)
    coordinator = IcebergCheckpointCoordinator(
        config, SimpleNamespace(table_identifier="other.table")
    )
    with pytest.raises(ValueError, match="different Iceberg table"):
        coordinator.initialize("table-uuid")

    coordinator = IcebergCheckpointCoordinator(
        config, SimpleNamespace(table_identifier="db.table")
    )
    with pytest.raises(ValueError, match="different Iceberg table"):
        coordinator.initialize("different-uuid")


def test_missing_and_corrupt_task_metadata_fail_closed(tmp_path):
    config, coordinator = _coordinator(tmp_path)
    writer = BatchBasedCheckpointWriter(config)
    artifact_id = f"{coordinator.generation_id}-missing"
    pending = writer.write_pending_checkpoint(pa.array([1]), artifact_id)
    assert pending is not None
    writer.commit_checkpoint(pending)

    with pytest.raises(ValueError, match="missing its destination metadata"):
        coordinator.load_active_results()

    os.remove(pending.committed_path)
    payload_path = _publish_task(config, coordinator, artifact_suffix="corrupt")
    del payload_path
    task_dir = (
        tmp_path / "_iceberg" / "generations" / coordinator.generation_id / "tasks"
    )
    arrow_path = next(task_dir.glob("*.arrow"))
    arrow_path.write_bytes(b"corrupt")
    with pytest.raises(ValueError, match="Corrupt Iceberg checkpoint payload"):
        coordinator.load_active_results()


def test_metadata_without_row_checkpoint_is_ignored(tmp_path):
    config, coordinator = _coordinator(tmp_path)
    result = _write_result()
    artifact_id = f"{coordinator.generation_id}-orphan"
    checkpoint_path = tmp_path / f"{artifact_id}.parquet"
    coordinator.persist_task_result(artifact_id, str(checkpoint_path), result)
    assert coordinator.load_active_results() == []


def test_manifest_history_is_bounded(tmp_path):
    _, coordinator = _coordinator(tmp_path)
    for _ in range(5):
        coordinator.mark_terminal()
        coordinator.initialize("table-uuid")

    manifest_dir = tmp_path / "_iceberg" / "manifests"
    assert len(list(manifest_dir.glob("*.json"))) == 2


def test_task_checkpoint_retry_reuses_committed_result(tmp_path):
    config, coordinator = _coordinator(tmp_path)
    sink = SimpleNamespace(
        coordinator=coordinator,
        _config=config,
    )
    writer = BatchBasedCheckpointWriter(config)
    block = BlockAccessor.for_block(pa.table({"id": [1, 2]}))
    first_context = TaskContext(
        0,
        "Write",
        kwargs={
            "write_uuid": "write",
            "_datasink_write_return": _write_result("file:///first"),
        },
    )
    write_task_checkpoint(sink, writer, block, first_context)

    retry_context = TaskContext(
        0,
        "Write",
        kwargs={
            "write_uuid": "write",
            "_datasink_write_return": _write_result("file:///orphaned-retry"),
        },
    )
    write_task_checkpoint(sink, writer, block, retry_context)

    assert str(
        retry_context.kwargs["_datasink_write_return"].data_files[0].file_path
    ) == ("file:///first")
    results = coordinator.load_active_results()
    assert [str(result.data_files[0].file_path) for result in results] == [
        "file:///first"
    ]


def test_task_checkpoint_lookup_does_not_list_namespace(tmp_path):
    config, coordinator = _coordinator(tmp_path)
    path = _publish_task(config, coordinator, artifact_suffix="task")
    task_id = f"{coordinator.generation_id}-task"

    with patch.object(
        coordinator,
        "_selected_row_checkpoint_paths",
        side_effect=AssertionError("namespace listing is not allowed"),
    ):
        checkpoint = coordinator.resolve_task_checkpoint(task_id)

    assert checkpoint.state is _TaskCheckpointState.COMMITTED
    assert checkpoint.result is not None
    assert path.endswith(f"{task_id}.parquet")


def test_task_checkpoint_retry_recovers_metadata_before_row_commit(tmp_path):
    config, coordinator = _coordinator(tmp_path)
    sink = SimpleNamespace(coordinator=coordinator, _config=config)
    writer = BatchBasedCheckpointWriter(config)
    block = BlockAccessor.for_block(pa.table({"id": [1]}))
    task_id = f"{coordinator.generation_id}-write-0"
    result = _write_result("file:///first")
    artifact_id = task_id
    pending = writer.write_pending_checkpoint(pa.array([1]), artifact_id)
    assert pending is not None
    coordinator.persist_task_result(artifact_id, pending.committed_path, result)

    context = TaskContext(
        0,
        "Write",
        kwargs={
            "write_uuid": "write",
            "_datasink_write_return": _write_result("file:///orphaned-retry"),
        },
    )
    write_task_checkpoint(sink, writer, block, context)

    assert str(context.kwargs["_datasink_write_return"].data_files[0].file_path) == (
        "file:///first"
    )
    assert os.path.exists(pending.committed_path)


def test_partition_filter_selects_matching_task_metadata(tmp_path):
    config, coordinator = _coordinator(tmp_path)
    first = _publish_task(
        config, coordinator, artifact_suffix="first", result=_write_result("file:///a")
    )
    _publish_task(
        config, coordinator, artifact_suffix="second", result=_write_result("file:///b")
    )
    config.checkpoint_path_partition_filter = lambda paths: [
        path for path in paths if path == first
    ]

    results = coordinator.load_active_results()
    assert [str(result.data_files[0].file_path) for result in results] == ["file:///a"]


def test_default_checkpoint_loader_ignores_metadata_only_directory(tmp_path):
    config = CheckpointConfig(id_column="id", checkpoint_path=str(tmp_path))
    metadata_dir = tmp_path / "_iceberg"
    metadata_dir.mkdir()
    (metadata_dir / "manifest.json").write_text("{}")
    manager = IdColumnCheckpointManager(
        checkpoint_config=config, data_context=ray.data.DataContext.get_current()
    )
    assert manager.load_checkpoint() == (None, 0)


@pytest.mark.parametrize("defer_cleanup", [True, False])
def test_checkpoint_callback_can_defer_success_cleanup(tmp_path, defer_cleanup):
    config = CheckpointConfig(id_column="id", checkpoint_path=str(tmp_path))
    tmp_path.mkdir(exist_ok=True)
    (tmp_path / "checkpoint.parquet").touch()
    callback = LoadCheckpointCallback(
        config, delete_on_execution_success=not defer_cleanup
    )
    executor = SimpleNamespace(_data_context=SimpleNamespace(checkpoint_config=config))
    callback.after_execution_succeeds(executor)
    assert tmp_path.exists() is defer_cleanup


def test_legacy_row_only_checkpoint_fails(tmp_path):
    config = CheckpointConfig(id_column="id", checkpoint_path=str(tmp_path))
    writer = BatchBasedCheckpointWriter(config)
    writer.write_block_checkpoint(BlockAccessor.for_block(pa.table({"id": [1]})))
    coordinator = IcebergCheckpointCoordinator(
        config, SimpleNamespace(table_identifier="db.table")
    )
    with pytest.raises(ValueError, match="row-only checkpoint"):
        coordinator.initialize("table-uuid")


def test_merge_results_deduplicates_and_rejects_conflicts():
    first = _write_result("file:///same")
    duplicate = deserialize_write_result(serialize_write_result(first))
    merged = IcebergCheckpointDatasink._merge_results([first], [duplicate])
    assert len(merged) == 1
    assert len(merged[0].data_files) == 1

    conflicting = IcebergWriteResult(
        data_files=[_data_file("file:///same", record_count=3)],
        schemas=first.schemas,
    )
    with pytest.raises(ValueError, match="Conflicting Iceberg checkpoint metadata"):
        IcebergCheckpointDatasink._merge_results([first], [conflicting])


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
def test_checkpointed_iceberg_rejects_custom_restore_classes(
    tmp_path, config_attr, config_value, error
):
    config = CheckpointConfig(
        id_column="id",
        checkpoint_path=str(tmp_path),
        **{config_attr: config_value},
    )
    sink = IcebergDatasink("db.table")
    wrapped = IcebergCheckpointDatasink(sink, config)
    with pytest.raises(ValueError, match=error):
        wrapped.enable_checkpointing()


@pytest.mark.parametrize("mode", [SaveMode.UPSERT, SaveMode.OVERWRITE])
def test_checkpointed_iceberg_rejects_unsupported_modes(tmp_path, mode):
    config = CheckpointConfig(id_column="id", checkpoint_path=str(tmp_path))
    sink = IcebergDatasink("db.table", mode=mode)
    wrapped = IcebergCheckpointDatasink(sink, config)
    with pytest.raises(ValueError, match="only APPEND"):
        wrapped.enable_checkpointing()


def test_checkpointed_iceberg_rejects_reserved_snapshot_property(tmp_path):
    config = CheckpointConfig(id_column="id", checkpoint_path=str(tmp_path))
    sink = IcebergDatasink(
        "db.table", snapshot_properties={_GENERATION_PROPERTY: "user-value"}
    )
    wrapped = IcebergCheckpointDatasink(sink, config)
    with pytest.raises(ValueError, match="reserved"):
        wrapped.enable_checkpointing()


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
    source = catalog.create_table("db.source", schema=schema)
    source.append(pa.table({"id": [1, 2, 3], "value": ["a", "b", "c"]}))
    catalog_kwargs = {
        "name": "checkpoint_catalog",
        "type": "sql",
        "uri": f"sqlite:///{tmp_path / 'catalog.db'}",
        "warehouse": f"file://{warehouse}",
    }
    return catalog, catalog_kwargs


def test_append_protocol_commits_snapshot_marker_without_ray_cluster(tmp_path):
    catalog, catalog_kwargs = _create_catalog(tmp_path)
    checkpoint_path = tmp_path / "checkpoints"
    config = CheckpointConfig(
        id_column="id",
        checkpoint_path=str(checkpoint_path),
        delete_checkpoint_on_success=False,
    )
    wrapped = IcebergCheckpointDatasink(
        IcebergDatasink("db.table", catalog_kwargs=catalog_kwargs), config
    )
    wrapped.enable_checkpointing()
    block = pa.table({"id": [1, 2], "value": ["a", "b"]})
    wrapped.on_write_start(block.schema)
    task_context = TaskContext(0, "Write", kwargs={"write_uuid": "write"})
    write_return = wrapped.write([block], task_context)
    task_context.kwargs["_datasink_write_return"] = write_return
    write_task_checkpoint(
        wrapped,
        BatchBasedCheckpointWriter(config),
        BlockAccessor.for_block(block),
        task_context,
    )

    wrapped.on_write_complete(WriteResult(2, block.nbytes, [write_return]))

    table = catalog.load_table("db.table")
    assert table.scan().to_arrow().sort_by("id").to_pydict() == {
        "id": [1, 2],
        "value": ["a", "b"],
    }
    snapshots = table.snapshots()
    assert len(snapshots) == 1
    assert (
        snapshots[0].summary.get(_GENERATION_PROPERTY)
        == wrapped.coordinator.generation_id
    )


@pytest.mark.parametrize("recover_on_retry", [False, True])
def test_late_commit_discards_uncommitted_task_checkpoints(tmp_path, recover_on_retry):
    catalog, catalog_kwargs = _create_catalog(tmp_path)
    checkpoint_path = tmp_path / "checkpoints"
    config = CheckpointConfig(
        id_column="id",
        checkpoint_path=str(checkpoint_path),
        delete_checkpoint_on_success=False,
    )
    wrapped = IcebergCheckpointDatasink(
        IcebergDatasink("db.table", catalog_kwargs=catalog_kwargs), config
    )
    wrapped.enable_checkpointing()

    committed_block = pa.table({"id": [1], "value": ["a"]})
    wrapped.on_write_start(committed_block.schema)
    committed_context = TaskContext(0, "Write", kwargs={"write_uuid": "committed"})
    committed_result = wrapped.write([committed_block], committed_context)
    committed_context.kwargs["_datasink_write_return"] = committed_result
    write_task_checkpoint(
        wrapped,
        BatchBasedCheckpointWriter(config),
        BlockAccessor.for_block(committed_block),
        committed_context,
    )

    wrapped._sink._snapshot_properties = {
        _GENERATION_PROPERTY: wrapped.coordinator.generation_id
    }
    wrapped._sink.on_write_complete(
        WriteResult(1, committed_block.nbytes, [committed_result])
    )

    late_block = pa.table({"id": [2], "value": ["b"]})
    late_context = TaskContext(0, "Write", kwargs={"write_uuid": "late"})
    late_result = wrapped.write([late_block], late_context)
    late_context.kwargs["_datasink_write_return"] = late_result
    write_task_checkpoint(
        wrapped,
        BatchBasedCheckpointWriter(config),
        BlockAccessor.for_block(late_block),
        late_context,
    )

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

    row_checkpoints = sorted(checkpoint_path.glob("*.parquet"))
    assert len(row_checkpoints) == 1
    assert "committed" in row_checkpoints[0].name
    assert retry.coordinator.load_active_results() == []


def test_empty_append_cleans_checkpoint_namespace_without_ray_cluster(tmp_path):
    _, catalog_kwargs = _create_catalog(tmp_path)
    checkpoint_path = tmp_path / "checkpoints"
    config = CheckpointConfig(
        id_column="id",
        checkpoint_path=str(checkpoint_path),
        delete_checkpoint_on_success=True,
    )
    wrapped = IcebergCheckpointDatasink(
        IcebergDatasink("db.table", catalog_kwargs=catalog_kwargs), config
    )
    wrapped.enable_checkpointing()

    wrapped.on_write_complete(WriteResult(0, 0, []))

    assert not checkpoint_path.exists()


def test_append_protocol_recovers_ambiguous_commit_without_ray_cluster(tmp_path):
    catalog, catalog_kwargs = _create_catalog(tmp_path)
    config = CheckpointConfig(
        id_column="id",
        checkpoint_path=str(tmp_path / "checkpoints"),
        delete_checkpoint_on_success=False,
    )
    wrapped = IcebergCheckpointDatasink(
        IcebergDatasink("db.table", catalog_kwargs=catalog_kwargs), config
    )
    wrapped.enable_checkpointing()
    block = pa.table({"id": [1], "value": ["a"]})
    wrapped.on_write_start(block.schema)
    task_context = TaskContext(0, "Write", kwargs={"write_uuid": "write"})
    write_return = wrapped.write([block], task_context)
    task_context.kwargs["_datasink_write_return"] = write_return
    write_task_checkpoint(
        wrapped,
        BatchBasedCheckpointWriter(config),
        BlockAccessor.for_block(block),
        task_context,
    )

    original = IcebergDatasink.on_write_complete

    def commit_then_raise(self, write_result):
        original(self, write_result)
        raise RuntimeError("ambiguous catalog response")

    with patch.object(IcebergDatasink, "on_write_complete", commit_then_raise):
        with pytest.raises(RuntimeError, match="ambiguous catalog response"):
            wrapped.on_write_complete(WriteResult(1, block.nbytes, [write_return]))

    retry = IcebergCheckpointDatasink(
        IcebergDatasink("db.table", catalog_kwargs=catalog_kwargs), config
    )
    retry.enable_checkpointing()
    new_block = pa.table({"id": [2], "value": ["b"]})
    retry.on_write_start(new_block.schema)
    retry_context = TaskContext(0, "Write", kwargs={"write_uuid": "retry"})
    retry_return = retry.write([new_block], retry_context)
    retry_context.kwargs["_datasink_write_return"] = retry_return
    write_task_checkpoint(
        retry,
        BatchBasedCheckpointWriter(config),
        BlockAccessor.for_block(new_block),
        retry_context,
    )
    retry.on_write_complete(WriteResult(1, new_block.nbytes, [retry_return]))

    table = catalog.load_table("db.table")
    assert len(table.snapshots()) == 2
    assert table.scan().to_arrow().sort_by("id").to_pydict() == {
        "id": [1, 2],
        "value": ["a", "b"],
    }


def _iceberg_input_dataset(catalog_kwargs):
    return ray.data.read_iceberg(
        table_identifier="db.source", catalog_kwargs=catalog_kwargs
    )


@pytest.mark.parametrize("delete_checkpoint_on_success", [True, False])
def test_append_recovers_after_failure_before_commit(
    ray_start_10_cpus_shared,
    restore_data_context,
    tmp_path,
    delete_checkpoint_on_success,
):
    catalog, catalog_kwargs = _create_catalog(tmp_path)
    checkpoint_path = tmp_path / "checkpoints"
    ray.data.DataContext.get_current().checkpoint_config = CheckpointConfig(
        id_column="id",
        checkpoint_path=str(checkpoint_path),
        delete_checkpoint_on_success=delete_checkpoint_on_success,
    )

    dataset = _iceberg_input_dataset(catalog_kwargs)
    with patch.object(
        IcebergDatasink,
        "on_write_complete",
        side_effect=RuntimeError("failure before catalog commit"),
    ):
        with pytest.raises(RuntimeError, match="failure before catalog commit"):
            dataset.write_iceberg("db.table", catalog_kwargs=catalog_kwargs)

    assert catalog.load_table("db.table").current_snapshot() is None
    _iceberg_input_dataset(catalog_kwargs).write_iceberg(
        "db.table", catalog_kwargs=catalog_kwargs
    )

    table = catalog.load_table("db.table")
    assert table.scan().to_arrow().sort_by("id").to_pydict() == {
        "id": [1, 2, 3],
        "value": ["a", "b", "c"],
    }
    assert len(table.snapshots()) == 1
    assert checkpoint_path.exists() is (not delete_checkpoint_on_success)


def test_ambiguous_append_starts_new_generation_for_new_rows(
    ray_start_10_cpus_shared, restore_data_context, tmp_path
):
    catalog, catalog_kwargs = _create_catalog(tmp_path)
    checkpoint_path = tmp_path / "checkpoints"
    ray.data.DataContext.get_current().checkpoint_config = CheckpointConfig(
        id_column="id",
        checkpoint_path=str(checkpoint_path),
        delete_checkpoint_on_success=False,
    )
    original = IcebergDatasink.on_write_complete

    def commit_then_raise(self, write_result):
        original(self, write_result)
        raise RuntimeError("ambiguous catalog response")

    with patch.object(IcebergDatasink, "on_write_complete", commit_then_raise):
        with pytest.raises(RuntimeError, match="ambiguous catalog response"):
            _iceberg_input_dataset(catalog_kwargs).write_iceberg(
                "db.table", catalog_kwargs=catalog_kwargs
            )

    assert len(catalog.load_table("db.table").snapshots()) == 1
    catalog.load_table("db.source").append(pa.table({"id": [4], "value": ["d"]}))
    _iceberg_input_dataset(catalog_kwargs).write_iceberg(
        "db.table", catalog_kwargs=catalog_kwargs
    )
    table = catalog.load_table("db.table")
    assert len(table.snapshots()) == 2
    assert table.scan().to_arrow().sort_by("id").to_pydict() == {
        "id": [1, 2, 3, 4],
        "value": ["a", "b", "c", "d"],
    }


def test_retained_terminal_checkpoint_starts_new_generation_for_new_rows(
    ray_start_10_cpus_shared, restore_data_context, tmp_path
):
    catalog, catalog_kwargs = _create_catalog(tmp_path)
    checkpoint_path = tmp_path / "checkpoints"
    ray.data.DataContext.get_current().checkpoint_config = CheckpointConfig(
        id_column="id",
        checkpoint_path=str(checkpoint_path),
        delete_checkpoint_on_success=False,
    )

    _iceberg_input_dataset(catalog_kwargs).write_iceberg(
        "db.table", catalog_kwargs=catalog_kwargs
    )
    _iceberg_input_dataset(catalog_kwargs).write_iceberg(
        "db.table", catalog_kwargs=catalog_kwargs
    )
    assert len(catalog.load_table("db.table").snapshots()) == 1

    catalog.load_table("db.source").append(pa.table({"id": [4], "value": ["d"]}))
    _iceberg_input_dataset(catalog_kwargs).write_iceberg(
        "db.table", catalog_kwargs=catalog_kwargs
    )
    table = catalog.load_table("db.table")
    assert len(table.snapshots()) == 2
    assert sorted(table.scan().to_arrow()["id"].to_pylist()) == [1, 2, 3, 4]


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
