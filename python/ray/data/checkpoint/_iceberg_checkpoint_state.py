"""Manage durable namespace state for recoverable Iceberg writes."""

import json
import logging
import posixpath
import uuid
from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple

from pyarrow.fs import FileSelector, FileType

from ray.data._internal.util import call_with_retry
from ray.data.context import DataContext
from ray.data.datasource.path_util import _unwrap_protocol

logger = logging.getLogger(__name__)

_FORMAT_VERSION = 1
_METADATA_DIR = "_iceberg"
_MANIFEST_DIR = "manifests"
_MANIFEST_RETENTION = 2


class GenerationStatus(str, Enum):
    """Durable lifecycle state for one Iceberg checkpoint generation."""

    ACTIVE = "active"
    TERMINAL = "terminal"


@dataclass(frozen=True)
class CheckpointPaths:
    """Build paths within one Iceberg checkpoint namespace."""

    root: str

    @property
    def metadata_root(self) -> str:
        return posixpath.join(self.root, _METADATA_DIR)

    @property
    def manifest_dir(self) -> str:
        return posixpath.join(self.metadata_root, _MANIFEST_DIR)

    def manifest(self, revision: int) -> str:
        return posixpath.join(self.manifest_dir, f"{revision:020d}.json")

    def generation_root(self, generation_id: str) -> str:
        return posixpath.join(self.metadata_root, "generations", generation_id)

    def task_payload(self, generation_id: str, artifact_id: str) -> str:
        return posixpath.join(
            self.generation_root(generation_id), "tasks", f"{artifact_id}.arrow"
        )

    def task_envelope(self, generation_id: str, artifact_id: str) -> str:
        return posixpath.join(
            self.generation_root(generation_id), "tasks", f"{artifact_id}.json"
        )

    def row_checkpoint(self, artifact_id: str) -> str:
        return posixpath.join(self.root, f"{artifact_id}.parquet")


@dataclass
class CheckpointManifest:
    """Versioned identity and generation state for one checkpoint namespace."""

    revision: int
    table_identifier: str
    table_uuid: str
    mode: str
    active_generation: Optional[str]
    generations: Dict[str, GenerationStatus]

    @classmethod
    def from_json(cls, value: Dict[str, Any], revision: int) -> "CheckpointManifest":
        required = {
            "format_version",
            "revision",
            "table_identifier",
            "table_uuid",
            "mode",
            "active_generation",
            "generations",
        }
        if value.get("format_version") != _FORMAT_VERSION:
            raise ValueError(
                "Unsupported Iceberg checkpoint namespace format version: "
                f"{value.get('format_version')}"
            )
        if (
            set(value) != required
            or not isinstance(revision, int)
            or revision < 0
            or value.get("revision") != revision
            or not isinstance(value.get("table_identifier"), str)
            or not value["table_identifier"]
            or not isinstance(value.get("table_uuid"), str)
            or not value["table_uuid"]
            or not isinstance(value.get("mode"), str)
            or not value["mode"]
            or (
                value.get("active_generation") is not None
                and not _is_generation_id(value["active_generation"])
            )
            or not isinstance(value.get("generations"), dict)
        ):
            raise ValueError("Invalid Iceberg checkpoint manifest")

        try:
            generations = {
                generation_id: GenerationStatus(status)
                for generation_id, status in value["generations"].items()
                if _is_generation_id(generation_id)
            }
        except ValueError as exc:
            raise ValueError("Invalid Iceberg checkpoint generation status") from exc
        if len(generations) != len(value["generations"]):
            raise ValueError("Invalid Iceberg checkpoint generation ID")

        active_generation = value["active_generation"]
        active_ids = [
            generation_id
            for generation_id, status in generations.items()
            if status is GenerationStatus.ACTIVE
        ]
        if (active_generation is None and active_ids) or (
            active_generation is not None and active_ids != [active_generation]
        ):
            raise ValueError("Invalid active Iceberg checkpoint generation")

        return cls(
            revision=revision,
            table_identifier=value["table_identifier"],
            table_uuid=value["table_uuid"],
            mode=value["mode"],
            active_generation=active_generation,
            generations=generations,
        )

    def to_json(self) -> Dict[str, Any]:
        return {
            "format_version": _FORMAT_VERSION,
            "revision": self.revision,
            "table_identifier": self.table_identifier,
            "table_uuid": self.table_uuid,
            "mode": self.mode,
            "active_generation": self.active_generation,
            "generations": {
                generation_id: status.value
                for generation_id, status in self.generations.items()
            },
        }

    def next_revision(self, revision: int) -> "CheckpointManifest":
        return CheckpointManifest(
            revision=revision,
            table_identifier=self.table_identifier,
            table_uuid=self.table_uuid,
            mode=self.mode,
            active_generation=self.active_generation,
            generations=self.generations.copy(),
        )


def _is_generation_id(value: Any) -> bool:
    return (
        isinstance(value, str)
        and len(value) == 32
        and all(character in "0123456789abcdef" for character in value)
    )


class IcebergCheckpointState:
    """Manage generation manifests for one Iceberg checkpoint namespace.

    Manifest files are immutable and revisioned. A transition writes a new
    revision before compacting history to the newest valid revision and one
    valid fallback. If publication is interrupted, readers ignore malformed
    revisions and continue from the fallback.
    """

    def __init__(
        self,
        filesystem: Any,
        checkpoint_path: str,
        *,
        table_identifier: str,
        table_uuid: str,
        mode: str,
    ):
        if not table_identifier or not table_uuid or not mode:
            raise ValueError("Iceberg checkpoint identity fields must be non-empty")
        self.filesystem = filesystem
        self.paths = CheckpointPaths(_unwrap_protocol(checkpoint_path))
        self.table_identifier = table_identifier
        self.table_uuid = table_uuid
        self.mode = mode
        self._retried_io_errors = DataContext.get_current().retried_io_errors
        self._generation_id: Optional[str] = None

    @property
    def generation_id(self) -> str:
        if self._generation_id is None:
            raise RuntimeError("Iceberg checkpoint generation has not been initialized")
        return self._generation_id

    def initialize(self) -> str:
        """Adopt the active generation or create a fresh one.

        A namespace without a manifest is safe to initialize only when it has
        no committed row checkpoints. Row-only state cannot prove that the
        corresponding destination files are recoverable, so it fails closed.
        """
        self.filesystem.create_dir(self.paths.root, recursive=True)
        manifest = self._load_manifest(required=False)
        if manifest is None:
            self._reject_legacy_row_checkpoints()
            generation_id = uuid.uuid4().hex
            manifest = CheckpointManifest(
                revision=self._next_manifest_revision(),
                table_identifier=self.table_identifier,
                table_uuid=self.table_uuid,
                mode=self.mode,
                active_generation=generation_id,
                generations={generation_id: GenerationStatus.ACTIVE},
            )
            self._write_manifest(manifest)
        else:
            self._validate_identity(manifest)
            generation_id = manifest.active_generation
            if generation_id is None:
                generation_id = uuid.uuid4().hex
                manifest = self._next_manifest(manifest)
                manifest.active_generation = generation_id
                manifest.generations[generation_id] = GenerationStatus.ACTIVE
                self._write_manifest(manifest)

        assert generation_id is not None
        self._generation_id = generation_id
        return generation_id

    def mark_terminal(self) -> None:
        """Close the active generation after its destination commit is known."""
        manifest = self._next_manifest(self._load_active_manifest())
        manifest.generations[self.generation_id] = GenerationStatus.TERMINAL
        manifest.active_generation = None
        self._write_manifest(manifest)

    def remove_empty_generation(self) -> None:
        """Remove an active generation that has not published task artifacts."""
        if self._generation_has_artifacts():
            raise ValueError("Cannot remove a non-empty Iceberg checkpoint generation")
        manifest = self._next_manifest(self._load_active_manifest())
        manifest.generations.pop(self.generation_id)
        manifest.active_generation = None
        self._write_manifest(manifest)
        generation_root = self.paths.generation_root(self.generation_id)
        if self.filesystem.get_file_info(generation_root).type != FileType.NotFound:
            self.filesystem.delete_dir(generation_root)

    def delete_namespace(self) -> None:
        """Delete all row and metadata artifacts in this checkpoint namespace."""
        if self.filesystem.get_file_info(self.paths.root).type != FileType.NotFound:
            self.filesystem.delete_dir(self.paths.root)
        self._generation_id = None

    def load_manifest(self) -> CheckpointManifest:
        """Return the newest valid manifest after validating namespace identity."""
        manifest = self._load_manifest()
        assert manifest is not None
        self._validate_identity(manifest)
        return manifest

    def _read_bytes(self, path: str) -> bytes:
        def _read() -> bytes:
            with self.filesystem.open_input_file(path) as source:
                return source.read()

        return call_with_retry(
            _read,
            description=f"read Iceberg checkpoint artifact {path}",
            match=self._retried_io_errors,
        )

    def _write_bytes(self, path: str, data: bytes) -> None:
        def _publish() -> None:
            self.filesystem.create_dir(posixpath.dirname(path), recursive=True)
            temporary_path = f"{path}.tmp.{uuid.uuid4().hex}"
            with self.filesystem.open_output_stream(temporary_path) as output:
                output.write(data)

            existing = self.filesystem.get_file_info(path)
            if existing.type != FileType.NotFound:
                if self._read_bytes(path) != data:
                    self.filesystem.delete_file(temporary_path)
                    raise RuntimeError(
                        "Conflicting Iceberg checkpoint artifact already exists: "
                        f"{path}"
                    )
                self.filesystem.delete_file(temporary_path)
                return

            try:
                self.filesystem.move(temporary_path, path)
            except Exception:
                final = self.filesystem.get_file_info(path)
                if final.type != FileType.NotFound and self._read_bytes(path) == data:
                    temporary = self.filesystem.get_file_info(temporary_path)
                    if temporary.type != FileType.NotFound:
                        self.filesystem.delete_file(temporary_path)
                    return
                raise
            if self._read_bytes(path) != data:
                raise RuntimeError(
                    f"Failed to publish Iceberg checkpoint artifact: {path}"
                )

        call_with_retry(
            _publish,
            description=f"write Iceberg checkpoint artifact {path}",
            match=self._retried_io_errors,
        )

    def _load_json(self, path: str) -> Dict[str, Any]:
        try:
            value = json.loads(self._read_bytes(path))
        except Exception as exc:
            raise ValueError(
                f"Invalid Iceberg checkpoint JSON artifact: {path}"
            ) from exc
        if not isinstance(value, dict):
            raise ValueError(f"Invalid Iceberg checkpoint JSON artifact: {path}")
        return value

    def _write_json(self, path: str, value: Dict[str, Any]) -> None:
        data = json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
        self._write_bytes(path, data)

    def _manifest_paths(self) -> List[Tuple[int, str]]:
        entries = self.filesystem.get_file_info(
            FileSelector(self.paths.manifest_dir, recursive=False, allow_not_found=True)
        )
        manifests = []
        for entry in entries:
            if entry.type != FileType.File:
                continue
            name = posixpath.basename(entry.path)
            stem, extension = posixpath.splitext(name)
            if extension == ".json" and stem.isdigit():
                manifests.append((int(stem), entry.path))
        return sorted(manifests, reverse=True)

    def _next_manifest_revision(self) -> int:
        paths = self._manifest_paths()
        return paths[0][0] + 1 if paths else 0

    def _next_manifest(self, manifest: CheckpointManifest) -> CheckpointManifest:
        revision = max(manifest.revision + 1, self._next_manifest_revision())
        return manifest.next_revision(revision)

    def _write_manifest(self, manifest: CheckpointManifest) -> None:
        self._write_json(self.paths.manifest(manifest.revision), manifest.to_json())

        valid_manifests = 0
        for revision, path in self._manifest_paths():
            try:
                CheckpointManifest.from_json(self._load_json(path), revision)
                is_valid = True
            except ValueError:
                is_valid = False
            if is_valid and valid_manifests < _MANIFEST_RETENTION:
                valid_manifests += 1
                continue
            try:
                self.filesystem.delete_file(path)
            except Exception:
                logger.warning(
                    "Failed to compact Iceberg checkpoint manifest %s",
                    path,
                    exc_info=True,
                )

    def _load_manifest(self, *, required: bool = True) -> Optional[CheckpointManifest]:
        paths = self._manifest_paths()
        for revision, path in paths:
            try:
                value = self._load_json(path)
            except ValueError:
                logger.warning(
                    "Ignoring incomplete Iceberg checkpoint manifest %s", path
                )
                continue
            if value.get("format_version") != _FORMAT_VERSION:
                raise ValueError(
                    "Unsupported Iceberg checkpoint namespace format version: "
                    f"{value.get('format_version')}"
                )
            try:
                return CheckpointManifest.from_json(value, revision)
            except ValueError:
                logger.warning(
                    "Ignoring incomplete Iceberg checkpoint manifest %s", path
                )
        if paths or required:
            raise ValueError("No valid Iceberg checkpoint namespace manifest found")
        return None

    def _validate_identity(self, manifest: CheckpointManifest) -> None:
        expected = (self.table_identifier, self.table_uuid, self.mode)
        actual = (manifest.table_identifier, manifest.table_uuid, manifest.mode)
        if actual != expected:
            raise ValueError(
                "Checkpoint path belongs to a different Iceberg table or write mode: "
                f"expected {expected}, found {actual}"
            )

    def _load_active_manifest(self) -> CheckpointManifest:
        manifest = self.load_manifest()
        if manifest.active_generation != self.generation_id:
            raise ValueError(
                "Iceberg checkpoint active generation changed concurrently"
            )
        return manifest

    def _reject_legacy_row_checkpoints(self) -> None:
        entries = self.filesystem.get_file_info(
            FileSelector(self.paths.root, recursive=True, allow_not_found=True)
        )
        metadata_prefix = f"{self.paths.metadata_root}/"
        if any(
            entry.type == FileType.File
            and entry.path.endswith(".parquet")
            and not entry.path.endswith(".pending.parquet")
            and not entry.path.startswith(metadata_prefix)
            for entry in entries
        ):
            raise ValueError(
                "Checkpointed Iceberg writes cannot restore a row-only checkpoint "
                "directory because destination file metadata is missing. Use a new "
                "checkpoint path."
            )

    def _generation_has_artifacts(self) -> bool:
        generation_root = self.paths.generation_root(self.generation_id)
        generation_entries = self.filesystem.get_file_info(
            FileSelector(generation_root, recursive=True, allow_not_found=True)
        )
        if any(entry.type == FileType.File for entry in generation_entries):
            return True

        prefix = f"{self.generation_id}-"
        root_entries = self.filesystem.get_file_info(
            FileSelector(self.paths.root, recursive=False, allow_not_found=True)
        )
        return any(
            entry.type == FileType.File
            and posixpath.basename(entry.path).startswith(prefix)
            for entry in root_entries
        )
