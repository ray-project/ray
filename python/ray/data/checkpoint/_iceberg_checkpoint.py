"""Coordinate recoverable Ray Data APPEND writes to Iceberg tables.

Row checkpoints alone can tell Ray which source rows completed, but they can't
recover the ``DataFile`` objects that an Iceberg task wrote before a driver
failure. This module stores both pieces of state and publishes the row
checkpoint last. A committed row checkpoint therefore means that matching,
validated Iceberg task metadata also exists.

Each checkpoint namespace contains one active generation. The generation ID
also becomes an Iceberg snapshot property when the driver commits the append.
On recovery, the snapshot property distinguishes an append that definitely did
not commit from an append whose catalog response was ambiguous. Completed
generations retain row IDs for filtering, but Ray never commits their data-file
metadata again.
"""

import hashlib
import json
import logging
import posixpath
import uuid
from dataclasses import dataclass
from enum import Enum
from typing import TYPE_CHECKING, Any, Dict, Iterable, List, Optional, Tuple

import pyarrow as pa
from pyarrow.fs import FileSelector, FileType

from ray.data._internal.datasource.iceberg_datasink import (
    IcebergDatasink,
    IcebergWriteResult,
)
from ray.data._internal.savemode import SaveMode
from ray.data._internal.util import call_with_retry
from ray.data.block import Block, BlockAccessor
from ray.data.checkpoint._iceberg_checkpoint_serialization import (
    deserialize_write_result,
    serialize_write_result,
)
from ray.data.checkpoint.checkpoint_writer import BatchBasedCheckpointWriter
from ray.data.context import DataContext
from ray.data.datasource.datasink import Datasink, WriteResult
from ray.data.datasource.path_util import _unwrap_protocol

if TYPE_CHECKING:
    from ray.data._internal.execution.interfaces import TaskContext
    from ray.data.checkpoint.interfaces import CheckpointConfig

logger = logging.getLogger(__name__)

_FORMAT_VERSION = 1
_METADATA_DIR = "_iceberg"
_MANIFEST_DIR = "manifests"
_MANIFEST_RETENTION = 2
_GENERATION_PROPERTY = "ray.data.checkpoint.operation-id"


def _data_file_metadata(data_file: Any) -> Tuple[Any, ...]:
    """Return the DataFile metadata that must agree for path deduplication."""
    try:
        spec_id = data_file.spec_id
    except AttributeError:
        spec_id = None
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
        spec_id,
    )


class _GenerationStatus(str, Enum):
    ACTIVE = "active"
    TERMINAL = "terminal"


class _TaskCheckpointState(Enum):
    ABSENT = "absent"
    METADATA_PUBLISHED = "metadata_published"
    COMMITTED = "committed"


@dataclass(frozen=True)
class _CheckpointPaths:
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
class _CheckpointManifest:
    revision: int
    table_identifier: str
    table_uuid: str
    mode: str
    active_generation: Optional[str]
    generations: Dict[str, _GenerationStatus]

    @classmethod
    def from_json(cls, value: Dict[str, Any], revision: int) -> "_CheckpointManifest":
        required = {
            "format_version",
            "revision",
            "table_identifier",
            "table_uuid",
            "mode",
            "active_generation",
            "generations",
        }
        if (
            set(value) != required
            or value.get("format_version") != _FORMAT_VERSION
            or value.get("revision") != revision
            or not isinstance(value.get("table_identifier"), str)
            or not isinstance(value.get("table_uuid"), str)
            or not isinstance(value.get("mode"), str)
            or (
                value.get("active_generation") is not None
                and not isinstance(value.get("active_generation"), str)
            )
            or not isinstance(value.get("generations"), dict)
        ):
            raise ValueError("Invalid Iceberg checkpoint manifest")
        try:
            generations = {
                generation_id: _GenerationStatus(status)
                for generation_id, status in value["generations"].items()
                if isinstance(generation_id, str)
            }
        except ValueError as exc:
            raise ValueError("Invalid Iceberg checkpoint generation status") from exc
        if len(generations) != len(value["generations"]):
            raise ValueError("Invalid Iceberg checkpoint generation ID")
        return cls(
            revision=revision,
            table_identifier=value["table_identifier"],
            table_uuid=value["table_uuid"],
            mode=value["mode"],
            active_generation=value["active_generation"],
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

    def next_revision(self, revision: int) -> "_CheckpointManifest":
        return _CheckpointManifest(
            revision=revision,
            table_identifier=self.table_identifier,
            table_uuid=self.table_uuid,
            mode=self.mode,
            active_generation=self.active_generation,
            generations=self.generations.copy(),
        )


@dataclass(frozen=True)
class _TaskEnvelope:
    generation_id: str
    table_uuid: str
    checkpoint_file: str
    payload_file: str
    payload_sha256: str

    @classmethod
    def from_json(cls, value: Dict[str, Any], artifact_id: str) -> "_TaskEnvelope":
        required = {
            "format_version",
            "generation_id",
            "table_uuid",
            "checkpoint_file",
            "payload_file",
            "payload_sha256",
        }
        string_fields = required - {"format_version"}
        if (
            set(value) != required
            or value.get("format_version") != _FORMAT_VERSION
            or not all(isinstance(value.get(field), str) for field in string_fields)
            or value["checkpoint_file"] != f"{artifact_id}.parquet"
            or value["payload_file"] != f"{artifact_id}.arrow"
            or len(value["payload_sha256"]) != 64
        ):
            raise ValueError("Invalid Iceberg checkpoint task envelope")
        return cls(
            generation_id=value["generation_id"],
            table_uuid=value["table_uuid"],
            checkpoint_file=value["checkpoint_file"],
            payload_file=value["payload_file"],
            payload_sha256=value["payload_sha256"],
        )

    def to_json(self) -> Dict[str, Any]:
        return {"format_version": _FORMAT_VERSION, **self.__dict__}


@dataclass(frozen=True)
class _TaskCheckpoint:
    state: _TaskCheckpointState
    result: Optional[IcebergWriteResult] = None


class IcebergCheckpointCoordinator:
    """Manage driver and task artifacts for one Iceberg checkpoint namespace.

    The namespace has three kinds of durable state:

    * Revisioned manifests identify the table and active generation.
    * Arrow payloads and JSON envelopes preserve each task's ``DataFile`` state.
    * Top-level Parquet files contain row IDs and act as task commit markers.

    Task metadata always precedes its Parquet marker. Recovery only considers
    metadata with a committed marker, which prevents partially published task
    state from filtering source rows.
    """

    def __init__(self, config: "CheckpointConfig", sink: IcebergDatasink):
        self._config = config
        self._filesystem = config.filesystem
        self._paths = _CheckpointPaths(_unwrap_protocol(config.checkpoint_path))
        self._sink = sink
        self._retried_io_errors = DataContext.get_current().retried_io_errors
        self._table_uuid: Optional[str] = None
        self._generation_id: Optional[str] = None

    @property
    def generation_id(self) -> str:
        if self._generation_id is None:
            raise RuntimeError("Iceberg checkpoint generation has not been initialized")
        return self._generation_id

    def _read_bytes(self, path: str) -> bytes:
        def _read() -> bytes:
            with self._filesystem.open_input_file(path) as source:
                return source.read()

        return call_with_retry(
            _read,
            description=f"read Iceberg checkpoint artifact {path}",
            match=self._retried_io_errors,
        )

    def _write_bytes(self, path: str, data: bytes) -> None:
        def _publish() -> None:
            parent = posixpath.dirname(path)
            self._filesystem.create_dir(parent, recursive=True)
            temporary_path = f"{path}.tmp.{uuid.uuid4().hex}"
            with self._filesystem.open_output_stream(temporary_path) as output:
                output.write(data)

            existing = self._filesystem.get_file_info(path)
            if existing.type != FileType.NotFound:
                if self._read_bytes(path) != data:
                    self._filesystem.delete_file(temporary_path)
                    raise RuntimeError(
                        "Conflicting Iceberg checkpoint artifact already exists: "
                        f"{path}"
                    )
                self._filesystem.delete_file(temporary_path)
                return

            try:
                self._filesystem.move(temporary_path, path)
            except Exception:
                final = self._filesystem.get_file_info(path)
                if final.type != FileType.NotFound and self._read_bytes(path) == data:
                    temporary = self._filesystem.get_file_info(temporary_path)
                    if temporary.type != FileType.NotFound:
                        self._filesystem.delete_file(temporary_path)
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

    def initialize(self, table_uuid: str) -> None:
        """Adopt the active generation or create a new generation.

        Initialization validates that an existing namespace belongs to the
        same Iceberg table and APPEND mode. A namespace with no manifest but
        with committed row files is unsafe because it lacks destination-file
        metadata, so initialization rejects it instead of filtering those rows.

        Args:
            table_uuid: Stable UUID from the destination Iceberg table metadata.
        """
        self._table_uuid = table_uuid
        self._filesystem.create_dir(self._paths.root, recursive=True)
        manifest = self._load_manifest(required=False)

        if manifest is None:
            root_files = self._filesystem.get_file_info(
                FileSelector(self._paths.root, recursive=True, allow_not_found=True)
            )
            committed_row_files = [
                entry.path
                for entry in root_files
                if entry.type == FileType.File
                and entry.path.endswith(".parquet")
                and not entry.path.endswith(".pending.parquet")
            ]
            if committed_row_files:
                raise ValueError(
                    "Checkpointed Iceberg writes cannot restore a row-only checkpoint "
                    "directory because destination file metadata is missing. Use a new "
                    "checkpoint path."
                )
            generation_id = uuid.uuid4().hex
            manifest = _CheckpointManifest(
                revision=self._next_manifest_revision(),
                table_identifier=self._sink.table_identifier,
                table_uuid=table_uuid,
                mode=SaveMode.APPEND.value,
                active_generation=generation_id,
                generations={generation_id: _GenerationStatus.ACTIVE},
            )
            self._write_manifest(manifest)
            self._generation_id = generation_id
            return

        self._validate_identity(manifest, table_uuid)
        active = manifest.active_generation
        if active is None:
            active = uuid.uuid4().hex
            manifest = self._next_manifest(manifest)
            manifest.active_generation = active
            manifest.generations[active] = _GenerationStatus.ACTIVE
            self._write_manifest(manifest)
        elif manifest.generations.get(active) is not _GenerationStatus.ACTIVE:
            raise ValueError("Invalid active Iceberg checkpoint generation")
        self._generation_id = active

    def _manifest_paths(self) -> List[Tuple[int, str]]:
        entries = self._filesystem.get_file_info(
            FileSelector(
                self._paths.manifest_dir, recursive=False, allow_not_found=True
            )
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

    def _next_manifest(self, manifest: _CheckpointManifest) -> _CheckpointManifest:
        revision = max(manifest.revision + 1, self._next_manifest_revision())
        return manifest.next_revision(revision)

    def _write_manifest(self, manifest: _CheckpointManifest) -> None:
        """Publish a manifest revision, then retain two valid revisions.

        Publishing a new immutable file preserves the previous valid state if
        the write stops midway. After publication succeeds, compaction keeps the
        current revision and one valid fallback. Invalid interrupted revisions
        never replace that fallback.
        """
        self._write_json(self._paths.manifest(manifest.revision), manifest.to_json())

        # Keep the current manifest and one valid fallback without retaining an
        # ever-growing sequence of full manifest snapshots. Malformed interrupted
        # revisions do not consume the fallback slot.
        valid_manifests = 0
        for revision, path in self._manifest_paths():
            try:
                _CheckpointManifest.from_json(self._load_json(path), revision)
                is_valid = True
            except ValueError:
                is_valid = False
            if is_valid and valid_manifests < _MANIFEST_RETENTION:
                valid_manifests += 1
                continue
            try:
                self._filesystem.delete_file(path)
            except Exception:
                logger.warning(
                    "Failed to compact Iceberg checkpoint manifest %s",
                    path,
                    exc_info=True,
                )

    def _load_manifest(self, *, required: bool = True) -> Optional[_CheckpointManifest]:
        for revision, path in self._manifest_paths():
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
                return _CheckpointManifest.from_json(value, revision)
            except ValueError:
                logger.warning(
                    "Ignoring incomplete Iceberg checkpoint manifest %s", path
                )
        if required:
            raise ValueError("No valid Iceberg checkpoint namespace manifest found")
        return None

    def _validate_identity(
        self, manifest: _CheckpointManifest, table_uuid: str
    ) -> None:
        expected = (self._sink.table_identifier, table_uuid, SaveMode.APPEND.value)
        actual = (manifest.table_identifier, manifest.table_uuid, manifest.mode)
        if actual != expected:
            raise ValueError(
                "Checkpoint path belongs to a different Iceberg table or write mode: "
                f"expected {expected}, found {actual}"
            )

    @staticmethod
    def _validate_artifact_id(artifact_id: str) -> None:
        if not artifact_id or posixpath.basename(artifact_id) != artifact_id:
            raise ValueError(f"Invalid Iceberg checkpoint artifact ID: {artifact_id!r}")

    def persist_task_result(
        self,
        artifact_id: str,
        checkpoint_path: str,
        write_result: IcebergWriteResult,
    ) -> None:
        """Publish destination metadata before committing a row checkpoint.

        The Arrow payload preserves the Iceberg write result. The JSON envelope
        binds that payload to its generation, table, hash, and future Parquet row
        checkpoint. ``write_task_checkpoint`` publishes the Parquet marker only
        after this method succeeds.

        Args:
            artifact_id: Deterministic ID for this generation, write, and task.
            checkpoint_path: Final path of the task's row checkpoint.
            write_result: Iceberg data files and schemas produced by the task.
        """
        if not write_result.data_files:
            raise ValueError(
                "Iceberg write produced rows without recoverable destination files"
            )
        payload = serialize_write_result(write_result)
        payload_hash = hashlib.sha256(payload).hexdigest()
        self._validate_artifact_id(artifact_id)
        assert self._table_uuid is not None
        payload_path = self._paths.task_payload(self.generation_id, artifact_id)
        envelope_path = self._paths.task_envelope(self.generation_id, artifact_id)
        envelope = _TaskEnvelope(
            generation_id=self.generation_id,
            table_uuid=self._table_uuid,
            checkpoint_file=posixpath.basename(checkpoint_path),
            payload_file=posixpath.basename(payload_path),
            payload_sha256=payload_hash,
        )
        self._write_bytes(payload_path, payload)
        self._write_json(envelope_path, envelope.to_json())

    def resolve_task_checkpoint(self, artifact_id: str) -> _TaskCheckpoint:
        """Resolve the explicit publication state for one deterministic task."""
        self._validate_artifact_id(artifact_id)
        checkpoint_path = self._paths.row_checkpoint(artifact_id)
        if self._filesystem.get_file_info(checkpoint_path).type != FileType.NotFound:
            partition_filter = self._config.checkpoint_path_partition_filter
            if partition_filter is None or checkpoint_path in partition_filter(
                [checkpoint_path]
            ):
                return _TaskCheckpoint(
                    _TaskCheckpointState.COMMITTED,
                    self.load_task_result(artifact_id),
                )

        envelope_path = self._paths.task_envelope(self.generation_id, artifact_id)
        if self._filesystem.get_file_info(envelope_path).type != FileType.NotFound:
            return _TaskCheckpoint(
                _TaskCheckpointState.METADATA_PUBLISHED,
                self.load_task_result(artifact_id),
            )

        # A payload without its envelope never completed metadata publication.
        # Remove it so the retry can publish the deterministic task artifact.
        payload_path = self._paths.task_payload(self.generation_id, artifact_id)
        if self._filesystem.get_file_info(payload_path).type != FileType.NotFound:
            self._filesystem.delete_file(payload_path)
        return _TaskCheckpoint(_TaskCheckpointState.ABSENT)

    def load_task_result(self, artifact_id: str) -> IcebergWriteResult:
        envelope = self._load_task_envelope(artifact_id)
        payload_path = self._paths.task_payload(self.generation_id, artifact_id)
        payload = self._read_bytes(payload_path)
        if hashlib.sha256(payload).hexdigest() != envelope.payload_sha256:
            raise ValueError(f"Corrupt Iceberg checkpoint payload: {payload_path}")
        return deserialize_write_result(payload)

    def _selected_row_checkpoint_paths(self) -> List[str]:
        entries = self._filesystem.get_file_info(
            FileSelector(
                self._paths.root,
                recursive=self._config.checkpoint_path_partition_filter is not None,
                allow_not_found=True,
            )
        )
        paths = [
            entry.path
            for entry in entries
            if entry.type == FileType.File
            and entry.path.endswith(".parquet")
            and not entry.path.endswith(".pending.parquet")
            and not entry.path.startswith(f"{self._paths.metadata_root}/")
        ]
        partition_filter = self._config.checkpoint_path_partition_filter
        return partition_filter(paths) if partition_filter is not None else paths

    def _load_task_envelope(self, artifact_id: str) -> _TaskEnvelope:
        envelope_path = self._paths.task_envelope(self.generation_id, artifact_id)
        if self._filesystem.get_file_info(envelope_path).type == FileType.NotFound:
            raise ValueError(
                "Iceberg row checkpoint is missing its destination metadata: "
                f"{artifact_id}.parquet"
            )
        try:
            envelope = _TaskEnvelope.from_json(
                self._load_json(envelope_path), artifact_id
            )
        except ValueError as exc:
            raise ValueError(
                f"Invalid Iceberg checkpoint task envelope: {envelope_path}"
            ) from exc
        if (
            envelope.generation_id != self.generation_id
            or envelope.table_uuid != self._table_uuid
        ):
            raise ValueError(f"Incompatible Iceberg checkpoint task: {envelope_path}")
        return envelope

    def _load_active_manifest(self) -> _CheckpointManifest:
        manifest = self._load_manifest()
        assert manifest is not None
        assert self._table_uuid is not None
        self._validate_identity(manifest, self._table_uuid)
        if manifest.active_generation != self.generation_id:
            raise ValueError(
                "Iceberg checkpoint active generation changed concurrently"
            )
        return manifest

    def load_active_results(self) -> List[IcebergWriteResult]:
        """Load recoverable task results from the current active generation.

        Terminal generations contribute row IDs to generic checkpoint filtering,
        but this method intentionally ignores their destination metadata. This
        separation prevents Ray from appending a completed generation twice.
        Unknown generations fail closed because Ray can't determine whether
        their rows and destination files form a valid recovery unit.
        """
        manifest = self._load_active_manifest()
        selected = self._selected_row_checkpoint_paths()
        known_generations = manifest.generations
        results = []
        for checkpoint_path in selected:
            checkpoint_file = posixpath.basename(checkpoint_path)
            generation_id = checkpoint_file.split("-", 1)[0]
            status = known_generations.get(generation_id)
            if status is _GenerationStatus.TERMINAL:
                continue
            if (
                generation_id != self.generation_id
                or status is not _GenerationStatus.ACTIVE
            ):
                raise ValueError(
                    "Found an Iceberg row checkpoint without a known recoverable "
                    f"generation: {checkpoint_file}"
                )
            artifact_id = checkpoint_file[: -len(".parquet")]
            results.append(self.load_task_result(artifact_id))
        return results

    def _discard_task_checkpoint(
        self, checkpoint_path: str, artifact_id: str
    ) -> None:
        # Delete the row checkpoint first. Once it is gone these IDs cannot be
        # incorrectly filtered, even if metadata cleanup is interrupted.
        self._filesystem.delete_file(checkpoint_path)
        for path in (
            self._paths.task_envelope(self.generation_id, artifact_id),
            self._paths.task_payload(self.generation_id, artifact_id),
        ):
            if self._filesystem.get_file_info(path).type != FileType.NotFound:
                self._filesystem.delete_file(path)

    def discard_uncommitted_results(self, committed_paths: set[str]) -> bool:
        """Discard active task checkpoints whose files aren't in the snapshot.

        Returns whether any complete task checkpoints were discarded. A task
        that only partially appears in the snapshot is unsafe to recover because
        its row checkpoint can't identify which rows belong to each data file.
        """
        discarded = False
        for checkpoint_path in self._selected_row_checkpoint_paths():
            checkpoint_file = posixpath.basename(checkpoint_path)
            if not checkpoint_file.startswith(f"{self.generation_id}-"):
                continue
            artifact_id = checkpoint_file[: -len(".parquet")]
            result = self.load_task_result(artifact_id)
            result_paths = {str(data_file.file_path) for data_file in result.data_files}
            committed = result_paths.intersection(committed_paths)
            if committed == result_paths:
                continue
            if committed:
                raise ValueError(
                    "Iceberg snapshot only contains part of a task checkpoint"
                )
            self._discard_task_checkpoint(checkpoint_path, artifact_id)
            discarded = True
        return discarded

    def mark_terminal(self) -> None:
        """Close the active generation after confirming its Iceberg snapshot."""
        manifest = self._next_manifest(self._load_active_manifest())
        manifest.generations[self.generation_id] = _GenerationStatus.TERMINAL
        manifest.active_generation = None
        self._write_manifest(manifest)

    def discard_empty_generation(self) -> None:
        manifest = self._next_manifest(self._load_active_manifest())
        manifest.generations.pop(self.generation_id, None)
        manifest.active_generation = None
        self._write_manifest(manifest)
        generation_root = self._paths.generation_root(self.generation_id)
        if self._filesystem.get_file_info(generation_root).type != FileType.NotFound:
            self._filesystem.delete_dir(generation_root)

    def delete_namespace(self) -> None:
        if self._filesystem.get_file_info(self._paths.root).type != FileType.NotFound:
            self._filesystem.delete_dir(self._paths.root)


class IcebergCheckpointDatasink(Datasink[IcebergWriteResult]):
    """Add checkpoint recovery and commit detection to an Iceberg datasink.

    Workers still delegate file creation to ``IcebergDatasink``. This wrapper
    coordinates task metadata and changes driver completion into a recovery
    protocol: detect an earlier commit, merge active recovered files with this
    attempt's files, commit them with a generation marker, verify that marker,
    and only then terminalize or delete checkpoint state.
    """

    def __init__(self, sink: IcebergDatasink, config: "CheckpointConfig"):
        self._sink = sink
        self._config = config
        self._coordinator = IcebergCheckpointCoordinator(config, sink)
        self._checkpointing_active = False

    def enable_checkpointing(self) -> None:
        """Validate supported settings and resolve the generation before reads.

        This method runs before checkpoint filtering. If the active generation's
        snapshot has become visible since an ambiguous failure, it discards task
        checkpoints whose files aren't in that snapshot before marking the
        generation terminal and starting a fresh one. Retained terminal row IDs
        then filter rows that the snapshot contains, while discarded rows enter
        the fresh generation.
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
        table_uuid = str(self._sink._table.metadata.table_uuid)
        self._coordinator.initialize(table_uuid)
        snapshot = self._find_generation_snapshot()
        if snapshot is not None:
            committed_paths = self._snapshot_added_file_paths(snapshot)
            self._coordinator.discard_uncommitted_results(committed_paths)
            self._coordinator.mark_terminal()
            self._coordinator.initialize(table_uuid)
        self._checkpointing_active = True

    @property
    def checkpointing_active(self) -> bool:
        return self._checkpointing_active

    @property
    def coordinator(self) -> IcebergCheckpointCoordinator:
        return self._coordinator

    def on_write_start(self, schema: Optional[pa.Schema] = None) -> None:
        self._sink.on_write_start(schema)

    def write(self, blocks: Iterable[Block], ctx: "TaskContext") -> IcebergWriteResult:
        return self._sink.write(blocks, ctx)

    def _find_generation_snapshot(self) -> Optional[Any]:
        """Refresh the table and find this generation's catalog commit marker."""
        self._sink._reload_table()
        for snapshot in self._sink._table.snapshots():
            if (
                snapshot.summary is not None
                and snapshot.summary.get(_GENERATION_PROPERTY)
                == self._coordinator.generation_id
            ):
                return snapshot
        return None

    def _snapshot_added_file_paths(self, snapshot: Any) -> set[str]:
        """Return data-file paths added by the generation's marked snapshot."""
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
        """Merge recovered and current files while detecting path conflicts.

        Task retries can return the same data file more than once. Equal file
        metadata collapses to one append entry, while the same path with
        different metadata fails closed.
        """
        merged = []
        seen: Dict[str, Tuple[Tuple[Any, ...], Tuple[pa.Schema, ...]]] = {}
        for result in [*recovered, *current]:
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
        write_result: WriteResult[IcebergWriteResult], *, require_files: bool = False
    ) -> List[IcebergWriteResult]:
        return [
            result
            for result in write_result.write_returns
            if result is not None and (result.data_files or not require_files)
        ]

    def _handle_visible_snapshot(self) -> bool:
        """Resolve a generation whose snapshot became visible during the write."""
        snapshot = self._find_generation_snapshot()
        if snapshot is None:
            return False

        committed_paths = self._snapshot_added_file_paths(snapshot)
        discarded = self._coordinator.discard_uncommitted_results(committed_paths)
        self._coordinator.mark_terminal()
        if discarded:
            raise RuntimeError(
                "Iceberg checkpoint generation was committed concurrently while "
                "new task results were being written; late task checkpoints were "
                "discarded and will be retried"
            )
        self._cleanup_after_success()
        return True

    def _commit_with_generation_marker(
        self,
        write_result: WriteResult[IcebergWriteResult],
        merged: List[IcebergWriteResult],
    ) -> None:
        original_properties = self._sink._snapshot_properties
        self._sink._snapshot_properties = {
            **original_properties,
            _GENERATION_PROPERTY: self._coordinator.generation_id,
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

        merged = self._merge_results(
            self._coordinator.load_active_results(),
            self._current_results(write_result),
        )
        if not merged:
            self._coordinator.discard_empty_generation()
            self._cleanup_after_success()
            return

        self._commit_with_generation_marker(write_result, merged)
        self._coordinator.mark_terminal()
        self._cleanup_after_success()

    def _cleanup_after_success(self) -> None:
        if not self._config.delete_checkpoint_on_success:
            return
        try:
            self._coordinator.delete_namespace()
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


def wrap_iceberg_datasink(
    datasink: Datasink, config: Optional["CheckpointConfig"]
) -> Datasink:
    """Wrap an Iceberg datasink when the DataContext enables checkpointing."""
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
    """Publish one write task's recoverable metadata and row checkpoint.

    The task ID stays stable across task retries. First reuse a committed result
    or metadata from an interrupted marker commit. For a new result, write the
    pending row IDs, publish the Iceberg metadata, and finally rename the row
    checkpoint to its committed path. The final rename makes the task visible
    to both row filtering and driver-side Iceberg recovery.

    Args:
        datasink: Checkpoint-aware Iceberg datasink for this write.
        checkpoint_writer: Writer for pending and committed row-ID Parquet files.
        block: Input rows handled by this task.
        ctx: Task context containing the write UUID and Iceberg write result.
    """
    write_result = ctx.kwargs.get("_datasink_write_return")
    if not isinstance(write_result, IcebergWriteResult):
        raise TypeError("Iceberg write task did not return IcebergWriteResult")

    write_uuid = ctx.kwargs.get("write_uuid")
    if not write_uuid:
        raise RuntimeError("Iceberg checkpoint write is missing the write UUID")
    artifact_id = f"{datasink.coordinator.generation_id}-{write_uuid}-{ctx.task_idx}"
    task_checkpoint = datasink.coordinator.resolve_task_checkpoint(artifact_id)
    if task_checkpoint.state is _TaskCheckpointState.COMMITTED:
        ctx.kwargs["_datasink_write_return"] = task_checkpoint.result
        return
    if task_checkpoint.state is _TaskCheckpointState.METADATA_PUBLISHED:
        assert task_checkpoint.result is not None
        write_result = task_checkpoint.result
        ctx.kwargs["_datasink_write_return"] = write_result

    id_column_data = BlockAccessor.for_block(
        block.select(columns=[datasink._config.id_column])
    ).to_arrow()[datasink._config.id_column]
    pending = checkpoint_writer.write_pending_checkpoint(
        id_column_data, checkpoint_id=artifact_id
    )
    if pending is None:
        return
    if task_checkpoint.state is _TaskCheckpointState.ABSENT:
        datasink.coordinator.persist_task_result(
            artifact_id, pending.committed_path, write_result
        )
    checkpoint_writer.commit_checkpoint(pending)
