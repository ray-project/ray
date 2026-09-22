import json
from pathlib import Path

import pyarrow.fs as pafs
import pytest

from ray.data.checkpoint._iceberg_checkpoint_state import (
    CheckpointManifest,
    GenerationStatus,
    IcebergCheckpointState,
)

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


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
