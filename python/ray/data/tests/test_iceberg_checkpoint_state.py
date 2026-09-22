import hashlib
import json
from pathlib import Path

import pyarrow as pa
import pyarrow.fs as pafs
import pytest
from pyiceberg.manifest import DataFile, DataFileContent, FileFormat
from pyiceberg.typedef import Record

from ray.data._internal.datasource.iceberg_datasink import IcebergWriteResult
from ray.data.checkpoint import CheckpointConfig
from ray.data.checkpoint._iceberg_checkpoint_state import (
    CheckpointManifest,
    GenerationStatus,
    IcebergCheckpointState,
    TaskCheckpointState,
)
from ray.data.checkpoint.checkpoint_writer import BatchBasedCheckpointWriter

_TABLE_IDENTIFIER = "db.table"
_TABLE_UUID = "table-uuid"
_MODE = "append"
_GENERATION_ID = "a" * 32


def _state(tmp_path: Path, **overrides) -> IcebergCheckpointState:
    values = {
        "table_identifier": _TABLE_IDENTIFIER,
        "table_uuid": _TABLE_UUID,
        "mode": _MODE,
    }
    values.update(overrides)
    return IcebergCheckpointState(pafs.LocalFileSystem(), str(tmp_path), **values)


def _manifest_json(**overrides):
    value = {
        "format_version": 1,
        "revision": 0,
        "table_identifier": _TABLE_IDENTIFIER,
        "table_uuid": _TABLE_UUID,
        "mode": _MODE,
        "active_generation": _GENERATION_ID,
        "generations": {_GENERATION_ID: "active"},
    }
    value.update(overrides)
    return value


def _write_result(*paths: str) -> IcebergWriteResult:
    return IcebergWriteResult(
        data_files=[
            DataFile.from_args(
                content=DataFileContent.DATA,
                file_path=path,
                file_format=FileFormat.PARQUET,
                partition=Record(),
                record_count=2,
                file_size_in_bytes=100,
            )
            for path in paths
        ],
        schemas=[pa.schema([pa.field("id", pa.int64())])],
    )


def _publish_task(
    tmp_path: Path,
    state: IcebergCheckpointState,
    suffix: str,
    *,
    result: IcebergWriteResult | None = None,
    commit: bool = True,
):
    artifact_id = f"{state.generation_id}-{suffix}"
    writer = BatchBasedCheckpointWriter(
        CheckpointConfig(id_column="id", checkpoint_path=str(tmp_path))
    )
    pending = writer.write_pending_checkpoint(pa.array([1, 2]), artifact_id)
    assert pending is not None
    state.persist_task_result(
        artifact_id,
        pending.committed_path,
        result or _write_result(f"file:///warehouse/{suffix}.parquet"),
    )
    if commit:
        writer.commit_checkpoint(pending)
    return artifact_id, pending, writer


def test_generation_creation_adoption_terminal_and_fresh_generation(tmp_path):
    state = _state(tmp_path)
    first_generation = state.initialize()

    adopted = _state(tmp_path)
    assert adopted.initialize() == first_generation

    adopted.mark_terminal()
    terminal_manifest = adopted.load_manifest()
    assert terminal_manifest.active_generation is None
    assert terminal_manifest.generations[first_generation] is GenerationStatus.TERMINAL

    fresh = _state(tmp_path)
    second_generation = fresh.initialize()
    assert second_generation != first_generation
    assert fresh.load_manifest().generations == {
        first_generation: GenerationStatus.TERMINAL,
        second_generation: GenerationStatus.ACTIVE,
    }


def test_empty_generation_removal_and_namespace_deletion(tmp_path):
    state = _state(tmp_path)
    generation_id = state.initialize()

    state.remove_empty_generation()
    manifest = state.load_manifest()
    assert manifest.active_generation is None
    assert generation_id not in manifest.generations

    new_generation = state.initialize()
    artifact = tmp_path / "_iceberg" / "generations" / new_generation / "task.json"
    artifact.parent.mkdir(parents=True)
    artifact.write_text("{}")
    with pytest.raises(ValueError, match="non-empty"):
        state.remove_empty_generation()

    state.delete_namespace()
    assert not tmp_path.exists()


def test_interrupted_manifest_publication_preserves_valid_fallback(tmp_path):
    state = _state(tmp_path)
    generation_id = state.initialize()
    manifest_dir = tmp_path / "_iceberg" / "manifests"
    original_manifest = manifest_dir / "00000000000000000000.json"

    (manifest_dir / "00000000000000000001.json.tmp.interrupted").write_text("{")
    (manifest_dir / "00000000000000000001.json").write_text("{")

    adopted = _state(tmp_path)
    assert adopted.initialize() == generation_id
    adopted.mark_terminal()

    manifests = sorted(manifest_dir.glob("*.json"))
    assert manifests == [
        original_manifest,
        manifest_dir / "00000000000000000002.json",
    ]
    assert adopted.load_manifest().generations[generation_id] is (
        GenerationStatus.TERMINAL
    )


def test_manifest_history_retains_current_and_one_fallback(tmp_path):
    state = _state(tmp_path)
    state.initialize()
    for _ in range(5):
        state.mark_terminal()
        state.initialize()

    manifests = list((tmp_path / "_iceberg" / "manifests").glob("*.json"))
    assert len(manifests) == 2
    assert state.load_manifest().revision == max(int(path.stem) for path in manifests)


@pytest.mark.parametrize(
    "overrides",
    [
        {"table_identifier": "other.table"},
        {"table_uuid": "other-uuid"},
        {"mode": "overwrite"},
    ],
)
def test_namespace_identity_mismatch_fails_closed(tmp_path, overrides):
    _state(tmp_path).initialize()

    with pytest.raises(ValueError, match="different Iceberg table or write mode"):
        _state(tmp_path, **overrides).initialize()


def test_manifest_rejects_incompatible_version_and_invalid_state(tmp_path):
    manifest_dir = tmp_path / "_iceberg" / "manifests"
    manifest_dir.mkdir(parents=True)
    manifest_path = manifest_dir / "00000000000000000000.json"
    manifest_path.write_text(json.dumps(_manifest_json(format_version=2)))

    with pytest.raises(ValueError, match="format version"):
        _state(tmp_path).initialize()

    invalid_manifests = [
        _manifest_json(active_generation=None),
        _manifest_json(
            active_generation=_GENERATION_ID,
            generations={_GENERATION_ID: "terminal"},
        ),
        _manifest_json(generations={_GENERATION_ID: "active", "b" * 32: "active"}),
        _manifest_json(active_generation="not-a-generation"),
        _manifest_json(generations={_GENERATION_ID: "unknown"}),
    ]
    for value in invalid_manifests:
        with pytest.raises(ValueError):
            CheckpointManifest.from_json(value, 0)


def test_malformed_namespace_without_fallback_fails_closed(tmp_path):
    manifest_dir = tmp_path / "_iceberg" / "manifests"
    manifest_dir.mkdir(parents=True)
    (manifest_dir / "00000000000000000000.json").write_text("{")

    with pytest.raises(ValueError, match="No valid"):
        _state(tmp_path).initialize()


def test_legacy_row_only_namespace_fails_closed(tmp_path):
    (tmp_path / "legacy.parquet").touch()

    with pytest.raises(ValueError, match="row-only checkpoint"):
        _state(tmp_path).initialize()


def test_pending_or_non_parquet_files_do_not_create_legacy_state(tmp_path):
    (tmp_path / "task.pending.parquet").touch()
    (tmp_path / "notes.json").write_text("{}")

    generation_id = _state(tmp_path).initialize()
    assert len(generation_id) == 32


def test_idempotent_artifact_publication_rejects_conflicts(tmp_path):
    state = _state(tmp_path)
    state.initialize()
    path = state.paths.generation_root(state.generation_id) + "/artifact.json"

    state._write_bytes(path, b"first")
    state._write_bytes(path, b"first")
    with pytest.raises(RuntimeError, match="Conflicting"):
        state._write_bytes(path, b"second")


def test_task_publication_states_and_committed_result_loading(tmp_path):
    state = _state(tmp_path)
    state.initialize()
    committed_id, _, _ = _publish_task(tmp_path, state, "committed")
    metadata_id, _, _ = _publish_task(tmp_path, state, "metadata-only", commit=False)

    committed = state.resolve_task_checkpoint(committed_id)
    assert committed.state is TaskCheckpointState.COMMITTED
    assert committed.loaded_result is not None
    assert str(committed.loaded_result.result.data_files[0].file_path).endswith(
        "committed.parquet"
    )

    metadata = state.resolve_task_checkpoint(metadata_id)
    assert metadata.state is TaskCheckpointState.METADATA_PUBLISHED
    assert metadata.loaded_result is not None

    loaded = state.load_active_results()
    assert [result.artifact_id for result in loaded] == [committed_id]


def test_metadata_publication_can_be_committed_by_deterministic_retry(tmp_path):
    state = _state(tmp_path)
    state.initialize()
    artifact_id, pending, writer = _publish_task(tmp_path, state, "retry", commit=False)

    published = state.resolve_task_checkpoint(artifact_id)
    assert published.state is TaskCheckpointState.METADATA_PUBLISHED
    writer.commit_checkpoint(pending)

    committed = state.resolve_task_checkpoint(artifact_id)
    assert committed.state is TaskCheckpointState.COMMITTED
    assert committed.loaded_result == published.loaded_result


def test_task_artifacts_are_published_before_row_commit_marker(tmp_path):
    state = _state(tmp_path)
    state.initialize()
    artifact_id = f"{state.generation_id}-ordered"
    writer = BatchBasedCheckpointWriter(
        CheckpointConfig(id_column="id", checkpoint_path=str(tmp_path))
    )
    pending = writer.write_pending_checkpoint(pa.array([1]), artifact_id)
    assert pending is not None
    payload_path = Path(state.paths.task_payload(state.generation_id, artifact_id))
    envelope_path = Path(state.paths.task_envelope(state.generation_id, artifact_id))

    assert Path(pending.pending_path).exists()
    assert not Path(pending.committed_path).exists()
    assert not payload_path.exists()
    assert not envelope_path.exists()

    state.persist_task_result(
        artifact_id,
        pending.committed_path,
        _write_result("file:///warehouse/ordered.parquet"),
    )
    assert Path(pending.pending_path).exists()
    assert not Path(pending.committed_path).exists()
    assert payload_path.exists()
    assert envelope_path.exists()

    writer.commit_checkpoint(pending)
    assert not Path(pending.pending_path).exists()
    assert Path(pending.committed_path).exists()


def test_payload_without_envelope_is_discarded_on_retry(tmp_path):
    state = _state(tmp_path)
    state.initialize()
    artifact_id = f"{state.generation_id}-payload-only"
    payload_path = state.paths.task_payload(state.generation_id, artifact_id)
    Path(payload_path).parent.mkdir(parents=True)
    Path(payload_path).write_bytes(b"incomplete")

    checkpoint = state.resolve_task_checkpoint(artifact_id)

    assert checkpoint.state is TaskCheckpointState.ABSENT
    assert not Path(payload_path).exists()


def test_committed_marker_without_metadata_fails_closed(tmp_path):
    state = _state(tmp_path)
    state.initialize()
    artifact_id = f"{state.generation_id}-missing"
    Path(state.paths.row_checkpoint(artifact_id)).touch()

    with pytest.raises(ValueError, match="missing its destination metadata"):
        state.load_active_results()


def test_missing_corrupt_and_digest_mismatched_payloads_fail_closed(tmp_path):
    for suffix, mutation, error in [
        ("missing", "missing", "payload is missing"),
        ("corrupt", "corrupt", "Corrupt Iceberg checkpoint payload"),
        ("digest", "digest", "Corrupt Iceberg checkpoint payload"),
    ]:
        case_path = tmp_path / suffix
        state = _state(case_path)
        state.initialize()
        artifact_id, _, _ = _publish_task(case_path, state, suffix)
        payload_path = Path(state.paths.task_payload(state.generation_id, artifact_id))
        envelope_path = Path(
            state.paths.task_envelope(state.generation_id, artifact_id)
        )
        if mutation == "missing":
            payload_path.unlink()
        elif mutation == "corrupt":
            payload_path.write_bytes(b"not Arrow IPC")
            envelope = json.loads(envelope_path.read_text())
            envelope["payload_sha256"] = hashlib.sha256(
                payload_path.read_bytes()
            ).hexdigest()
            envelope_path.write_text(json.dumps(envelope))
        else:
            payload_path.write_bytes(b"different bytes")

        with pytest.raises(ValueError, match=error):
            state.load_active_results()


def test_invalid_and_incompatible_task_envelopes_fail_closed(tmp_path):
    for suffix, update, error in [
        ("invalid", {"unexpected": "field"}, "Invalid"),
        ("table", {"table_uuid": "other-table"}, "Incompatible"),
        ("generation", {"generation_id": "b" * 32}, "Incompatible"),
    ]:
        case_path = tmp_path / suffix
        state = _state(case_path)
        state.initialize()
        artifact_id, _, _ = _publish_task(case_path, state, suffix)
        envelope_path = Path(
            state.paths.task_envelope(state.generation_id, artifact_id)
        )
        envelope = json.loads(envelope_path.read_text())
        envelope.update(update)
        envelope_path.write_text(json.dumps(envelope))

        with pytest.raises(ValueError, match=error):
            state.load_active_results()


def test_task_retry_lookup_uses_direct_file_checks(tmp_path):
    state = _state(tmp_path)
    state.initialize()
    artifact_id, _, _ = _publish_task(tmp_path, state, "direct")
    filesystem = state.filesystem

    class NoListFileSystem:
        def __getattr__(self, name):
            return getattr(filesystem, name)

        def get_file_info(self, target):
            if isinstance(target, pafs.FileSelector):
                raise AssertionError("task lookup must not list the namespace")
            return filesystem.get_file_info(target)

    state.filesystem = NoListFileSystem()

    assert (
        state.resolve_task_checkpoint(artifact_id).state
        is TaskCheckpointState.COMMITTED
    )


def test_partition_filter_selects_matching_task_results(tmp_path):
    state = _state(tmp_path)
    state.initialize()
    first_id, first_pending, _ = _publish_task(tmp_path, state, "first")
    _publish_task(tmp_path, state, "second")

    selected = state.load_active_results(
        lambda paths: [path for path in paths if path == first_pending.committed_path]
    )

    assert [result.artifact_id for result in selected] == [first_id]


def test_unknown_generation_and_unrelated_parquet_fail_closed(tmp_path):
    state = _state(tmp_path)
    state.initialize()
    Path(state.paths.root, f"{'b' * 32}-task.parquet").touch()

    with pytest.raises(ValueError, match="without a known recoverable generation"):
        state.load_active_results()

    Path(state.paths.root, f"{'b' * 32}-task.parquet").unlink()
    Path(state.paths.root, "backup.parquet").touch()
    with pytest.raises(ValueError, match="without a known recoverable generation"):
        state.load_active_results()


def test_late_task_reconciliation_discards_wholly_uncommitted_task(tmp_path):
    state = _state(tmp_path)
    state.initialize()
    artifact_id, pending, _ = _publish_task(
        tmp_path,
        state,
        "late",
        result=_write_result(
            "file:///warehouse/a.parquet", "file:///warehouse/b.parquet"
        ),
    )

    assert state.discard_uncommitted_results(set())
    assert not Path(pending.committed_path).exists()
    assert not Path(
        state.paths.task_envelope(state.generation_id, artifact_id)
    ).exists()
    assert not Path(state.paths.task_payload(state.generation_id, artifact_id)).exists()


def test_late_task_cleanup_removes_row_marker_before_metadata(tmp_path):
    state = _state(tmp_path)
    state.initialize()
    artifact_id, pending, _ = _publish_task(tmp_path, state, "interrupted-cleanup")
    filesystem = state.filesystem
    envelope_path = state.paths.task_envelope(state.generation_id, artifact_id)

    class FailingMetadataDeleteFileSystem:
        def __getattr__(self, name):
            return getattr(filesystem, name)

        def delete_file(self, path):
            if path == envelope_path:
                raise RuntimeError("metadata cleanup interrupted")
            return filesystem.delete_file(path)

    state.filesystem = FailingMetadataDeleteFileSystem()

    with pytest.raises(RuntimeError, match="cleanup interrupted"):
        state.discard_uncommitted_results(set())
    assert not Path(pending.committed_path).exists()
    assert Path(envelope_path).exists()


def test_late_task_reconciliation_retains_fully_committed_task(tmp_path):
    state = _state(tmp_path)
    state.initialize()
    artifact_id, pending, _ = _publish_task(
        tmp_path,
        state,
        "committed",
        result=_write_result(
            "file:///warehouse/a.parquet", "file:///warehouse/b.parquet"
        ),
    )

    assert not state.discard_uncommitted_results(
        {"file:///warehouse/a.parquet", "file:///warehouse/b.parquet"}
    )
    assert Path(pending.committed_path).exists()
    assert Path(state.paths.task_envelope(state.generation_id, artifact_id)).exists()


def test_late_task_reconciliation_rejects_partially_committed_task(tmp_path):
    state = _state(tmp_path)
    state.initialize()
    _publish_task(
        tmp_path,
        state,
        "partial",
        result=_write_result(
            "file:///warehouse/a.parquet", "file:///warehouse/b.parquet"
        ),
    )

    with pytest.raises(ValueError, match="only part"):
        state.discard_uncommitted_results({"file:///warehouse/a.parquet"})


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
