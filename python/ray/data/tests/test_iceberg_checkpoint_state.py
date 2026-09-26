import json
import posixpath
import sys

import pytest
from pyarrow.fs import FileType, LocalFileSystem

from ray.data.checkpoint._iceberg_checkpoint_state import (
    IcebergCheckpointState,
    build_task_checkpoint_id,
    parse_pending_checkpoint_name,
    validate_operation_id,
)

_OPERATION_1 = "a" * 32
_OPERATION_2 = "b" * 32
_WRITE_ID = "c" * 32
_TABLE_IDENTIFIER = "db.table"
_TABLE_UUID = "table-uuid"


class _TrackingFileSystem:
    def __init__(self, filesystem):
        self._filesystem = filesystem
        self.get_file_info_calls = []

    def get_file_info(self, paths):
        self.get_file_info_calls.append(paths)
        return self._filesystem.get_file_info(paths)

    def __getattr__(self, name):
        return getattr(self._filesystem, name)


def _write_file(filesystem, path, data=b"checkpoint"):
    filesystem.create_dir(posixpath.dirname(path), recursive=True)
    with filesystem.open_output_stream(path) as stream:
        stream.write(data)


def _read_file(filesystem, path):
    with filesystem.open_input_file(path) as stream:
        return stream.read()


def _pending_path(checkpoint_path, operation_id, task_index):
    checkpoint_id = build_task_checkpoint_id(operation_id, _WRITE_ID, task_index)
    return posixpath.join(checkpoint_path, f"{checkpoint_id}.pending.parquet")


def _committed_path(checkpoint_path, operation_id, task_index):
    checkpoint_id = build_task_checkpoint_id(operation_id, _WRITE_ID, task_index)
    return posixpath.join(checkpoint_path, f"{checkpoint_id}.parquet")


def test_namespace_creation_and_idempotent_reuse(tmp_path):
    filesystem = LocalFileSystem()
    checkpoint_path = str(tmp_path / "checkpoints")
    state = IcebergCheckpointState(checkpoint_path, filesystem)

    namespace = state.ensure_namespace(_TABLE_IDENTIFIER, _TABLE_UUID)
    assert namespace.table_identifier == _TABLE_IDENTIFIER
    assert namespace.table_uuid == _TABLE_UUID
    assert namespace.mode == "append"
    assert namespace.format_version == 1

    namespace_path = posixpath.join(checkpoint_path, "_iceberg", "namespace.json")
    original_payload = _read_file(filesystem, namespace_path)
    assert state.ensure_namespace(_TABLE_IDENTIFIER, _TABLE_UUID) == namespace
    assert _read_file(filesystem, namespace_path) == original_payload


@pytest.mark.parametrize(
    ("table_identifier", "table_uuid", "match"),
    [
        ("other.table", _TABLE_UUID, "table_identifier mismatch"),
        (_TABLE_IDENTIFIER, "other-uuid", "table_uuid mismatch"),
    ],
)
def test_namespace_rejects_destination_mismatch(
    tmp_path, table_identifier, table_uuid, match
):
    filesystem = LocalFileSystem()
    checkpoint_path = str(tmp_path / "checkpoints")
    state = IcebergCheckpointState(checkpoint_path, filesystem)
    state.ensure_namespace(_TABLE_IDENTIFIER, _TABLE_UUID)

    with pytest.raises(ValueError, match=match):
        state.ensure_namespace(table_identifier, table_uuid)


@pytest.mark.parametrize(
    ("field", "value"),
    [("format_version", 2), ("mode", "overwrite")],
)
def test_namespace_rejects_persisted_format_or_mode_mismatch(tmp_path, field, value):
    filesystem = LocalFileSystem()
    checkpoint_path = str(tmp_path / "checkpoints")
    namespace_path = posixpath.join(checkpoint_path, "_iceberg", "namespace.json")
    namespace = {
        "format_version": 1,
        "table_identifier": _TABLE_IDENTIFIER,
        "table_uuid": _TABLE_UUID,
        "mode": "append",
    }
    namespace[field] = value
    _write_file(filesystem, namespace_path, json.dumps(namespace).encode())
    state = IcebergCheckpointState(checkpoint_path, filesystem)

    with pytest.raises(ValueError, match=rf"{field} mismatch"):
        state.ensure_namespace(_TABLE_IDENTIFIER, _TABLE_UUID)


def test_namespace_rejects_unsupported_mode_before_writing(tmp_path):
    filesystem = LocalFileSystem()
    checkpoint_path = str(tmp_path / "checkpoints")
    state = IcebergCheckpointState(checkpoint_path, filesystem)

    with pytest.raises(ValueError, match="only supports append mode"):
        state.ensure_namespace(_TABLE_IDENTIFIER, _TABLE_UUID, mode="overwrite")
    assert filesystem.get_file_info(checkpoint_path).type == FileType.NotFound


def test_namespace_rejects_committed_rows_without_identity(tmp_path):
    filesystem = LocalFileSystem()
    checkpoint_path = str(tmp_path / "checkpoints")
    _write_file(filesystem, posixpath.join(checkpoint_path, "unowned.parquet"))
    _write_file(
        filesystem,
        posixpath.join(checkpoint_path, "unrelated.pending.parquet"),
    )
    state = IcebergCheckpointState(checkpoint_path, filesystem)

    with pytest.raises(ValueError, match="no Iceberg namespace identity"):
        state.ensure_namespace(_TABLE_IDENTIFIER, _TABLE_UUID)


def test_namespace_publish_retries_transient_io_error(tmp_path):
    class FlakyFilesystem:
        def __init__(self):
            self.delegate = LocalFileSystem()
            self.output_attempts = 0

        def __getattr__(self, name):
            return getattr(self.delegate, name)

        def open_output_stream(self, path):
            self.output_attempts += 1
            if self.output_attempts == 1:
                raise OSError("AWS Error NETWORK_CONNECTION")
            return self.delegate.open_output_stream(path)

    filesystem = FlakyFilesystem()
    state = IcebergCheckpointState(str(tmp_path / "checkpoints"), filesystem)

    state.ensure_namespace(_TABLE_IDENTIFIER, _TABLE_UUID)
    assert filesystem.output_attempts == 2


def test_task_checkpoint_id_and_pending_name_validation(tmp_path):
    checkpoint_path = str(tmp_path / "checkpoints")
    checkpoint_id = build_task_checkpoint_id(_OPERATION_1, _WRITE_ID, 7)
    filename = f"{checkpoint_id}.pending.parquet"
    parsed = parse_pending_checkpoint_name(filename, checkpoint_path)

    assert parsed is not None
    assert parsed.operation_id == _OPERATION_1
    assert parsed.checkpoint_id == checkpoint_id
    assert parsed.pending_path == posixpath.join(checkpoint_path, filename)
    assert parsed.committed_path == posixpath.join(
        checkpoint_path, f"{checkpoint_id}.parquet"
    )

    for operation_id in ["short", "A" * 32, "g" * 32, "../" + _OPERATION_1]:
        with pytest.raises(ValueError, match="operation IDs"):
            validate_operation_id(operation_id)
    for write_id in ["short", "C" * 32, "../" + _WRITE_ID]:
        with pytest.raises(ValueError, match="write IDs"):
            build_task_checkpoint_id(_OPERATION_1, write_id, 0)
    for task_index in [-1, True, "0"]:
        with pytest.raises(ValueError, match="task indexes"):
            build_task_checkpoint_id(_OPERATION_1, _WRITE_ID, task_index)

    malformed_names = [
        f"{_OPERATION_1}-{_WRITE_ID}-1.pending.parquet",
        f"{_OPERATION_1}-{_WRITE_ID}-0000000.pending.parquet",
        f"{'A' * 32}-{_WRITE_ID}-000001.pending.parquet",
        f"../{filename}",
        f"{checkpoint_id}.parquet",
        "backup.pending.parquet",
    ]
    assert all(
        parse_pending_checkpoint_name(name, checkpoint_path) is None
        for name in malformed_names
    )


def test_list_pending_operations_groups_only_valid_files(tmp_path):
    filesystem = LocalFileSystem()
    checkpoint_path = str(tmp_path / "checkpoints")
    state = IcebergCheckpointState(checkpoint_path, filesystem)

    for path in [
        _pending_path(checkpoint_path, _OPERATION_1, 1),
        _pending_path(checkpoint_path, _OPERATION_1, 0),
        _pending_path(checkpoint_path, _OPERATION_2, 0),
    ]:
        _write_file(filesystem, path)
    _write_file(filesystem, posixpath.join(checkpoint_path, "backup.pending.parquet"))
    _write_file(filesystem, posixpath.join(checkpoint_path, "temporary.tmp"))
    _write_file(filesystem, _committed_path(checkpoint_path, _OPERATION_2, 1))

    grouped = state.list_pending_operations()
    assert list(grouped) == [_OPERATION_1, _OPERATION_2]
    assert [item.checkpoint_id for item in grouped[_OPERATION_1]] == [
        build_task_checkpoint_id(_OPERATION_1, _WRITE_ID, 0),
        build_task_checkpoint_id(_OPERATION_1, _WRITE_ID, 1),
    ]
    assert len(grouped[_OPERATION_2]) == 1


def test_promote_operation_is_idempotent_and_operation_scoped(tmp_path):
    filesystem = LocalFileSystem()
    checkpoint_path = str(tmp_path / "checkpoints")
    state = IcebergCheckpointState(checkpoint_path, filesystem)

    first_pending = _pending_path(checkpoint_path, _OPERATION_1, 0)
    first_committed = _committed_path(checkpoint_path, _OPERATION_1, 0)
    duplicate_pending = _pending_path(checkpoint_path, _OPERATION_1, 1)
    duplicate_committed = _committed_path(checkpoint_path, _OPERATION_1, 1)
    other_pending = _pending_path(checkpoint_path, _OPERATION_2, 0)
    _write_file(filesystem, first_pending, b"first")
    _write_file(filesystem, duplicate_pending, b"stale")
    _write_file(filesystem, duplicate_committed, b"committed")
    _write_file(filesystem, other_pending, b"other")

    checkpoints = state.list_pending_operations()[_OPERATION_1]
    tracking_filesystem = _TrackingFileSystem(filesystem)
    state._filesystem = tracking_filesystem

    state.promote_operation(_OPERATION_1, checkpoints)

    assert tracking_filesystem.get_file_info_calls == [
        [
            first_pending,
            first_committed,
            duplicate_pending,
            duplicate_committed,
        ]
    ]

    state.promote_operation(_OPERATION_1)

    assert filesystem.get_file_info(first_pending).type == FileType.NotFound
    assert _read_file(filesystem, first_committed) == b"first"
    assert filesystem.get_file_info(duplicate_pending).type == FileType.NotFound
    assert _read_file(filesystem, duplicate_committed) == b"committed"
    assert filesystem.get_file_info(other_pending).type == FileType.File


def test_discard_operation_removes_only_its_pending_files(tmp_path):
    filesystem = LocalFileSystem()
    checkpoint_path = str(tmp_path / "checkpoints")
    state = IcebergCheckpointState(checkpoint_path, filesystem)
    discarded_pending = _pending_path(checkpoint_path, _OPERATION_1, 0)
    second_discarded_pending = _pending_path(checkpoint_path, _OPERATION_1, 1)
    retained_pending = _pending_path(checkpoint_path, _OPERATION_2, 0)
    retained_committed = _committed_path(checkpoint_path, _OPERATION_1, 2)
    for path in [
        discarded_pending,
        second_discarded_pending,
        retained_pending,
        retained_committed,
    ]:
        _write_file(filesystem, path)

    checkpoints = state.list_pending_operations()[_OPERATION_1]
    tracking_filesystem = _TrackingFileSystem(filesystem)
    state._filesystem = tracking_filesystem

    state.discard_operation(_OPERATION_1, checkpoints)

    assert tracking_filesystem.get_file_info_calls == [
        [discarded_pending, second_discarded_pending]
    ]

    state.discard_operation(_OPERATION_1)

    assert filesystem.get_file_info(discarded_pending).type == FileType.NotFound
    assert filesystem.get_file_info(second_discarded_pending).type == FileType.NotFound
    assert filesystem.get_file_info(retained_pending).type == FileType.File
    assert filesystem.get_file_info(retained_committed).type == FileType.File


def test_empty_namespace_and_complete_deletion(tmp_path):
    filesystem = LocalFileSystem()
    checkpoint_path = str(tmp_path / "checkpoints")
    state = IcebergCheckpointState(checkpoint_path, filesystem)

    assert state.list_pending_operations() == {}
    state.ensure_namespace(_TABLE_IDENTIFIER, _TABLE_UUID)
    _write_file(filesystem, _committed_path(checkpoint_path, _OPERATION_1, 0))

    state.delete()
    state.delete()
    assert filesystem.get_file_info(checkpoint_path).type == FileType.NotFound


def test_superseded_layout_is_rejected(tmp_path):
    filesystem = LocalFileSystem()
    checkpoint_path = str(tmp_path / "checkpoints")
    legacy_manifest = posixpath.join(
        checkpoint_path, "_iceberg", "manifests", "00000001.json"
    )
    _write_file(filesystem, legacy_manifest, b"{}")
    state = IcebergCheckpointState(checkpoint_path, filesystem)

    with pytest.raises(ValueError, match="unsupported unreleased.*new checkpoint path"):
        state.ensure_namespace(_TABLE_IDENTIFIER, _TABLE_UUID)


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
