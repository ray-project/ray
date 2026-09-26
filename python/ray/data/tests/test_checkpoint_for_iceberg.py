import sys
from pathlib import Path
from unittest.mock import patch

import pyarrow as pa
import pytest
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.schema import Schema
from pyiceberg.types import LongType, NestedField, StringType

import ray
from ray.data._internal.datasource.iceberg_datasink import IcebergDatasink
from ray.data._internal.savemode import SaveMode
from ray.data.checkpoint import CheckpointConfig
from ray.data.checkpoint._iceberg_checkpoint_state import IcebergCheckpointState
from ray.data.checkpoint.checkpoint_filter import (
    IdColumnCheckpointManager,
    NumpyArrayBasedCheckpointFilter,
)

pytestmark = pytest.mark.usefixtures("restore_data_context")

_SOURCE = "db.source"
_DESTINATION = "db.destination"
_OTHER_DESTINATION = "db.other_destination"


class _CustomCheckpointManager(IdColumnCheckpointManager):
    pass


class _CustomCheckpointFilter(NumpyArrayBasedCheckpointFilter):
    pass


def _create_catalog(tmp_path):
    warehouse = tmp_path / "warehouse"
    warehouse.mkdir()
    catalog_kwargs = {
        "name": "checkpoint_catalog",
        "type": "sql",
        "uri": f"sqlite:///{tmp_path / 'catalog.db'}",
        "warehouse": f"file://{warehouse}",
    }
    catalog = SqlCatalog(
        catalog_kwargs["name"],
        uri=catalog_kwargs["uri"],
        warehouse=catalog_kwargs["warehouse"],
    )
    catalog.create_namespace("db")
    schema = Schema(
        NestedField(1, "id", LongType(), required=False),
        NestedField(2, "value", StringType(), required=False),
    )
    source = catalog.create_table(_SOURCE, schema=schema)
    source.append(pa.table({"id": [1, 2, 3], "value": ["a", "b", "c"]}))
    catalog.create_table(_DESTINATION, schema=schema)
    catalog.create_table(_OTHER_DESTINATION, schema=schema)
    return catalog, catalog_kwargs


def _configure_checkpointing(
    checkpoint_path: Path,
    *,
    delete_checkpoint_on_success: bool = False,
    checkpoint_manager_cls=None,
    checkpoint_filter_cls=None,
):
    ray.data.DataContext.get_current().checkpoint_config = CheckpointConfig(
        id_column="id",
        checkpoint_path=str(checkpoint_path),
        delete_checkpoint_on_success=delete_checkpoint_on_success,
        checkpoint_manager_cls=checkpoint_manager_cls,
        checkpoint_filter_cls=checkpoint_filter_cls,
    )


def _input_dataset(catalog_kwargs):
    return ray.data.read_iceberg(
        table_identifier=_SOURCE,
        catalog_kwargs=catalog_kwargs,
    )


def _write_input(
    catalog_kwargs, *, table_identifier=_DESTINATION, mode=SaveMode.APPEND
):
    _input_dataset(catalog_kwargs).write_iceberg(
        table_identifier,
        catalog_kwargs=catalog_kwargs,
        mode=mode,
    )


def _rows(catalog, table_identifier=_DESTINATION):
    return sorted(
        catalog.load_table(table_identifier).scan().to_arrow().to_pylist(),
        key=lambda row: row["id"],
    )


def _checkpoint_files(checkpoint_path, suffix):
    if not checkpoint_path.exists():
        return []
    return sorted(
        path.name
        for path in checkpoint_path.iterdir()
        if path.is_file() and path.name.endswith(suffix)
    )


def _operation_ids(checkpoint_files):
    return {filename.split("-", 1)[0] for filename in checkpoint_files}


@pytest.mark.parametrize("delete_checkpoint_on_success", [False, True])
def test_checkpointed_append(
    ray_start_10_cpus_shared,
    tmp_path,
    delete_checkpoint_on_success,
):
    catalog, catalog_kwargs = _create_catalog(tmp_path)
    checkpoint_path = tmp_path / "checkpoints"
    _configure_checkpointing(
        checkpoint_path,
        delete_checkpoint_on_success=delete_checkpoint_on_success,
    )

    _write_input(catalog_kwargs)

    assert _rows(catalog) == [
        {"id": 1, "value": "a"},
        {"id": 2, "value": "b"},
        {"id": 3, "value": "c"},
    ]
    if delete_checkpoint_on_success:
        assert not checkpoint_path.exists()
    else:
        assert (checkpoint_path / "_iceberg" / "namespace.json").is_file()
        assert _checkpoint_files(checkpoint_path, ".parquet")
        assert not _checkpoint_files(checkpoint_path, ".pending.parquet")


def test_failure_before_commit_discards_pending_rows_and_recomputes(
    ray_start_10_cpus_shared,
    tmp_path,
):
    catalog, catalog_kwargs = _create_catalog(tmp_path)
    checkpoint_path = tmp_path / "checkpoints"
    _configure_checkpointing(checkpoint_path)

    with patch.object(
        IcebergDatasink,
        "on_write_complete",
        side_effect=RuntimeError("failure before catalog commit"),
    ):
        with pytest.raises(RuntimeError, match="failure before catalog commit"):
            _write_input(catalog_kwargs)

    failed_pending = _checkpoint_files(checkpoint_path, ".pending.parquet")
    assert failed_pending
    failed_operation_ids = _operation_ids(failed_pending)
    assert catalog.load_table(_DESTINATION).current_snapshot() is None

    _write_input(catalog_kwargs)

    destination = catalog.load_table(_DESTINATION)
    assert [row["id"] for row in _rows(catalog)] == [1, 2, 3]
    assert len(destination.snapshots()) == 1
    committed = [
        filename
        for filename in _checkpoint_files(checkpoint_path, ".parquet")
        if not filename.endswith(".pending.parquet")
    ]
    assert committed
    assert _operation_ids(committed).isdisjoint(failed_operation_ids)
    assert not _checkpoint_files(checkpoint_path, ".pending.parquet")


def test_ambiguous_commit_is_recovered_without_another_snapshot(
    ray_start_10_cpus_shared,
    tmp_path,
):
    catalog, catalog_kwargs = _create_catalog(tmp_path)
    checkpoint_path = tmp_path / "checkpoints"
    _configure_checkpointing(checkpoint_path)
    original = IcebergDatasink.on_write_complete

    def commit_then_raise(self, write_result):
        original(self, write_result)
        raise RuntimeError("ambiguous catalog response")

    with patch.object(IcebergDatasink, "on_write_complete", commit_then_raise):
        with pytest.raises(RuntimeError, match="ambiguous catalog response"):
            _write_input(catalog_kwargs)

    destination = catalog.load_table(_DESTINATION)
    assert len(destination.snapshots()) == 1
    assert _checkpoint_files(checkpoint_path, ".pending.parquet")

    _write_input(catalog_kwargs)

    destination = catalog.load_table(_DESTINATION)
    assert len(destination.snapshots()) == 1
    assert [row["id"] for row in _rows(catalog)] == [1, 2, 3]
    assert not _checkpoint_files(checkpoint_path, ".pending.parquet")


def test_retry_completes_partial_checkpoint_promotion(
    ray_start_10_cpus_shared,
    tmp_path,
):
    catalog, catalog_kwargs = _create_catalog(tmp_path)
    checkpoint_path = tmp_path / "checkpoints"
    _configure_checkpointing(checkpoint_path)
    original = IcebergCheckpointState.promote_operation
    interrupted = False

    def promote_one_then_raise(self, operation_id, checkpoints=None):
        nonlocal interrupted
        if checkpoints is None:
            checkpoints = self.list_pending_operations().get(operation_id, [])
        if not interrupted:
            assert len(checkpoints) > 1
            self._promote_checkpoint(checkpoints[0])
            interrupted = True
            raise RuntimeError("promotion interrupted")
        return original(self, operation_id, checkpoints)

    with patch.object(
        IcebergCheckpointState,
        "promote_operation",
        promote_one_then_raise,
    ):
        with pytest.raises(RuntimeError, match="promotion interrupted"):
            _input_dataset(catalog_kwargs).repartition(2).write_iceberg(
                _DESTINATION,
                catalog_kwargs=catalog_kwargs,
            )

    assert len(catalog.load_table(_DESTINATION).snapshots()) == 1
    assert _checkpoint_files(checkpoint_path, ".pending.parquet")

    _input_dataset(catalog_kwargs).repartition(2).write_iceberg(
        _DESTINATION,
        catalog_kwargs=catalog_kwargs,
    )

    assert len(catalog.load_table(_DESTINATION).snapshots()) == 1
    assert [row["id"] for row in _rows(catalog)] == [1, 2, 3]
    assert not _checkpoint_files(checkpoint_path, ".pending.parquet")


def test_retained_checkpoints_filter_old_rows_and_append_new_rows(
    ray_start_10_cpus_shared,
    tmp_path,
):
    catalog, catalog_kwargs = _create_catalog(tmp_path)
    checkpoint_path = tmp_path / "checkpoints"
    _configure_checkpointing(checkpoint_path)

    _write_input(catalog_kwargs)
    first_operation_ids = _operation_ids(_checkpoint_files(checkpoint_path, ".parquet"))
    _write_input(catalog_kwargs)
    assert len(catalog.load_table(_DESTINATION).snapshots()) == 1

    catalog.load_table(_SOURCE).append(pa.table({"id": [4], "value": ["d"]}))
    _write_input(catalog_kwargs)

    assert [row["id"] for row in _rows(catalog)] == [1, 2, 3, 4]
    assert len(catalog.load_table(_DESTINATION).snapshots()) == 2
    operation_ids = _operation_ids(_checkpoint_files(checkpoint_path, ".parquet"))
    assert first_operation_ids < operation_ids


@pytest.mark.parametrize(
    ("mode", "manager", "filter_cls", "match"),
    [
        (SaveMode.UPSERT, None, None, "only APPEND"),
        (SaveMode.OVERWRITE, None, None, "only APPEND"),
        (
            SaveMode.APPEND,
            _CustomCheckpointManager,
            None,
            "default checkpoint manager",
        ),
        (
            SaveMode.APPEND,
            None,
            _CustomCheckpointFilter,
            "default checkpoint filter",
        ),
    ],
)
def test_unsupported_checkpoint_configuration_fails_before_worker_write(
    ray_start_10_cpus_shared,
    tmp_path,
    mode,
    manager,
    filter_cls,
    match,
):
    _, catalog_kwargs = _create_catalog(tmp_path)
    _configure_checkpointing(
        tmp_path / "checkpoints",
        checkpoint_manager_cls=manager,
        checkpoint_filter_cls=filter_cls,
    )

    with patch.object(IcebergDatasink, "write", autospec=True) as write:
        with pytest.raises(ValueError, match=match):
            _write_input(catalog_kwargs, mode=mode)
        write.assert_not_called()


def test_checkpoint_namespace_cannot_be_reused_for_another_table(
    ray_start_10_cpus_shared,
    tmp_path,
):
    _, catalog_kwargs = _create_catalog(tmp_path)
    checkpoint_path = tmp_path / "checkpoints"
    _configure_checkpointing(checkpoint_path)
    _write_input(catalog_kwargs)

    with patch.object(IcebergDatasink, "write", autospec=True) as write:
        with pytest.raises(ValueError, match="does not match the destination"):
            _write_input(catalog_kwargs, table_identifier=_OTHER_DESTINATION)
        write.assert_not_called()


def test_unreleased_checkpoint_layout_is_rejected_before_worker_write(
    ray_start_10_cpus_shared,
    tmp_path,
):
    _, catalog_kwargs = _create_catalog(tmp_path)
    checkpoint_path = tmp_path / "checkpoints"
    legacy_path = checkpoint_path / "_iceberg" / "manifests"
    legacy_path.mkdir(parents=True)
    (legacy_path / "0001.json").write_text("{}", encoding="utf-8")
    _configure_checkpointing(checkpoint_path)

    with patch.object(IcebergDatasink, "write", autospec=True) as write:
        with pytest.raises(ValueError, match="unsupported unreleased"):
            _write_input(catalog_kwargs)
        write.assert_not_called()


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
