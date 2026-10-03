import json
import posixpath
import re
import uuid
from dataclasses import asdict, dataclass
from typing import Dict, List, Optional

from pyarrow.fs import FileInfo, FileSelector, FileType

from ray.data._internal.util import call_with_retry
from ray.data.checkpoint.checkpoint_writer import PENDING_CHECKPOINT_SUFFIX
from ray.data.context import DataContext
from ray.data.datasource.path_util import _unwrap_protocol

_FORMAT_VERSION = 1
_METADATA_DIRECTORY = "_iceberg"
_NAMESPACE_FILENAME = "namespace.json"
_APPEND_MODE = "append"
_HEX_ID_PATTERN = re.compile(r"^[0-9a-f]{32}$")
_PENDING_CHECKPOINT_PATTERN = re.compile(
    r"^(?P<operation_id>[0-9a-f]{32})-"
    r"(?P<write_id>[0-9a-f]{32})-"
    r"(?P<task_index>[0-9]{6,})\.pending\.parquet$"
)
_LEGACY_COMPONENTS = frozenset({"generations", "manifests", "tasks"})


@dataclass(frozen=True)
class IcebergCheckpointNamespace:
    """Identity of the Iceberg destination that owns a checkpoint path."""

    format_version: int
    table_identifier: str
    table_uuid: str
    mode: str


@dataclass(frozen=True)
class PendingOperationCheckpoint:
    """Paths and identity parsed from a pending row checkpoint."""

    operation_id: str
    checkpoint_id: str
    pending_path: str
    committed_path: str


def validate_operation_id(operation_id: str) -> None:
    """Validate an operation ID used in checkpoint and snapshot metadata."""
    if not _HEX_ID_PATTERN.fullmatch(operation_id):
        raise ValueError(
            "Iceberg checkpoint operation IDs must be 32 lower-case hexadecimal "
            f"characters, got {operation_id!r}."
        )


def build_task_checkpoint_id(operation_id: str, write_id: str, task_index: int) -> str:
    """Build the deterministic row-checkpoint ID for one write task."""
    validate_operation_id(operation_id)
    if not _HEX_ID_PATTERN.fullmatch(write_id):
        raise ValueError(
            "Iceberg checkpoint write IDs must be 32 lower-case hexadecimal "
            f"characters, got {write_id!r}."
        )
    if (
        not isinstance(task_index, int)
        or isinstance(task_index, bool)
        or task_index < 0
    ):
        raise ValueError(
            f"Iceberg checkpoint task indexes must be non-negative integers, got "
            f"{task_index!r}."
        )
    return f"{operation_id}-{write_id}-{task_index:06d}"


def parse_pending_checkpoint_name(
    filename: str, checkpoint_directory: str
) -> Optional[PendingOperationCheckpoint]:
    """Parse a valid pending checkpoint basename, or return ``None``."""
    if filename != posixpath.basename(filename):
        return None
    match = _PENDING_CHECKPOINT_PATTERN.fullmatch(filename)
    if match is None:
        return None
    task_index = match.group("task_index")
    if task_index != f"{int(task_index):06d}":
        return None

    checkpoint_id = filename[: -len(f"{PENDING_CHECKPOINT_SUFFIX}.parquet")]
    return PendingOperationCheckpoint(
        operation_id=match.group("operation_id"),
        checkpoint_id=checkpoint_id,
        pending_path=posixpath.join(checkpoint_directory, filename),
        committed_path=posixpath.join(checkpoint_directory, f"{checkpoint_id}.parquet"),
    )


class IcebergCheckpointState:
    """Manage operation-level row-checkpoint state for one Iceberg table.

    Pending row checkpoints are grouped by the operation ID encoded in their
    filenames. Catalog recovery decides whether a complete operation should be
    promoted or discarded; this class only applies that decision idempotently.
    """

    def __init__(self, checkpoint_path: str, filesystem) -> None:
        self._filesystem = filesystem
        self.checkpoint_path = _unwrap_protocol(checkpoint_path)
        self._metadata_path = posixpath.join(self.checkpoint_path, _METADATA_DIRECTORY)
        self._namespace_path = posixpath.join(self._metadata_path, _NAMESPACE_FILENAME)

    def ensure_namespace(
        self, table_identifier: str, table_uuid: str, mode: str = _APPEND_MODE
    ) -> IcebergCheckpointNamespace:
        """Create the namespace identity or validate the existing identity."""
        expected = self._expected_namespace(table_identifier, table_uuid, mode)
        self._reject_legacy_layout()
        self._filesystem.create_dir(self._metadata_path, recursive=True)

        namespace_info = self._filesystem.get_file_info(self._namespace_path)
        if namespace_info.type != FileType.NotFound:
            actual = self._read_namespace(namespace_info)
            self._validate_namespace(actual, expected)
            return actual

        self._reject_unowned_committed_checkpoints()
        payload = self._encode_namespace(expected)
        temporary_path = f"{self._namespace_path}.tmp-{uuid.uuid4().hex}"

        def publish() -> None:
            final_info = self._filesystem.get_file_info(self._namespace_path)
            if final_info.type != FileType.NotFound:
                self._validate_namespace(self._read_namespace(final_info), expected)
                return

            with self._filesystem.open_output_stream(temporary_path) as stream:
                stream.write(payload)

            # A filesystem move may be implemented as copy-and-delete on object
            # storage. Final verification, rather than rename atomicity, is the
            # publication contract under the single-writer requirement.
            final_info = self._filesystem.get_file_info(self._namespace_path)
            if final_info.type == FileType.NotFound:
                self._filesystem.move(temporary_path, self._namespace_path)
                actual = self._read_namespace()
            else:
                actual = self._read_namespace(final_info)

            self._validate_namespace(actual, expected)

        try:
            self._retry_io(publish, "publish the Iceberg checkpoint namespace")
        finally:
            self._retry_io(
                lambda: self._delete_file_if_present(temporary_path),
                "clean up the temporary Iceberg checkpoint namespace",
            )

        return expected

    def list_pending_operations(self) -> Dict[str, List[PendingOperationCheckpoint]]:
        """Return valid pending row checkpoints grouped by operation ID."""
        entries = self._filesystem.get_file_info(
            FileSelector(self.checkpoint_path, recursive=False, allow_not_found=True)
        )
        grouped: Dict[str, List[PendingOperationCheckpoint]] = {}
        for entry in entries:
            if entry.type != FileType.File:
                continue
            checkpoint = parse_pending_checkpoint_name(
                posixpath.basename(entry.path), self.checkpoint_path
            )
            if checkpoint is None:
                continue
            grouped.setdefault(checkpoint.operation_id, []).append(checkpoint)

        for checkpoints in grouped.values():
            checkpoints.sort(key=lambda checkpoint: checkpoint.checkpoint_id)
        return dict(sorted(grouped.items()))

    def promote_operation(self, operation_id: str) -> None:
        """Idempotently promote every pending checkpoint for an operation."""
        validate_operation_id(operation_id)
        checkpoints = self.list_pending_operations().get(operation_id, [])
        for checkpoint in checkpoints:
            self._retry_io(
                lambda checkpoint=checkpoint: self._promote_checkpoint(checkpoint),
                f"promote Iceberg row checkpoint {checkpoint.checkpoint_id}",
            )

    def discard_operation(self, operation_id: str) -> None:
        """Delete only pending row checkpoints for an uncommitted operation."""
        validate_operation_id(operation_id)
        checkpoints = self.list_pending_operations().get(operation_id, [])
        for checkpoint in checkpoints:
            self._retry_io(
                lambda checkpoint=checkpoint: self._delete_file_if_present(
                    checkpoint.pending_path
                ),
                f"discard Iceberg row checkpoint {checkpoint.checkpoint_id}",
            )

    def delete(self) -> None:
        """Delete the complete checkpoint namespace if it exists."""

        def delete_directory() -> None:
            info = self._filesystem.get_file_info(self.checkpoint_path)
            if info.type == FileType.NotFound:
                return
            if info.type != FileType.Directory:
                raise ValueError(
                    f"Iceberg checkpoint path {self.checkpoint_path!r} is not a "
                    "directory."
                )
            self._filesystem.delete_dir(self.checkpoint_path)

        self._retry_io(delete_directory, "delete the Iceberg checkpoint namespace")

    def _promote_checkpoint(self, checkpoint: PendingOperationCheckpoint) -> None:
        pending_info, committed_info = self._filesystem.get_file_info(
            [checkpoint.pending_path, checkpoint.committed_path]
        )
        pending_type = pending_info.type
        committed_type = committed_info.type
        self._validate_checkpoint_file_type(checkpoint.pending_path, pending_type)
        self._validate_checkpoint_file_type(checkpoint.committed_path, committed_type)

        if committed_type == FileType.File:
            if pending_type == FileType.File:
                self._filesystem.delete_file(checkpoint.pending_path)
            return
        if pending_type == FileType.NotFound:
            return
        self._filesystem.move(checkpoint.pending_path, checkpoint.committed_path)

    def _reject_legacy_layout(self) -> None:
        entries = self._filesystem.get_file_info(
            FileSelector(self._metadata_path, recursive=True, allow_not_found=True)
        )
        metadata_prefix = self._metadata_path.rstrip("/") + "/"
        for entry in entries:
            entry_path = entry.path
            if not entry_path.startswith(metadata_prefix):
                continue
            relative_path = entry_path[len(metadata_prefix) :]
            components = set(relative_path.split("/"))
            if components.intersection(_LEGACY_COMPONENTS) or entry_path.endswith(
                ".arrow"
            ):
                raise ValueError(
                    "The checkpoint path contains an unsupported unreleased "
                    "Iceberg checkpoint layout. Use a new checkpoint path."
                )

    def _reject_unowned_committed_checkpoints(self) -> None:
        entries = self._filesystem.get_file_info(
            FileSelector(self.checkpoint_path, recursive=True, allow_not_found=True)
        )
        for entry in entries:
            if (
                entry.type == FileType.File
                and entry.path.endswith(".parquet")
                and not entry.path.endswith(f"{PENDING_CHECKPOINT_SUFFIX}.parquet")
            ):
                raise ValueError(
                    "The checkpoint path contains committed row checkpoints but "
                    "has no Iceberg namespace identity. Use a new checkpoint path."
                )

    def _read_namespace(
        self, info: Optional[FileInfo] = None
    ) -> IcebergCheckpointNamespace:
        if info is None:
            info = self._filesystem.get_file_info(self._namespace_path)
        if info.type != FileType.File:
            raise ValueError(
                f"Iceberg checkpoint namespace {self._namespace_path!r} is not a file."
            )
        try:
            with self._filesystem.open_input_file(self._namespace_path) as stream:
                data = json.loads(stream.read().decode("utf-8"))
        except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ValueError(
                f"Invalid Iceberg checkpoint namespace {self._namespace_path!r}."
            ) from exc

        expected_fields = {
            "format_version",
            "table_identifier",
            "table_uuid",
            "mode",
        }
        if not isinstance(data, dict) or set(data) != expected_fields:
            raise ValueError(
                f"Invalid Iceberg checkpoint namespace {self._namespace_path!r}."
            )
        if (
            not isinstance(data["format_version"], int)
            or isinstance(data["format_version"], bool)
            or not isinstance(data["table_identifier"], str)
            or not data["table_identifier"]
            or not isinstance(data["table_uuid"], str)
            or not data["table_uuid"]
            or not isinstance(data["mode"], str)
        ):
            raise ValueError(
                f"Invalid Iceberg checkpoint namespace {self._namespace_path!r}."
            )
        return IcebergCheckpointNamespace(**data)

    @staticmethod
    def _expected_namespace(
        table_identifier: str, table_uuid: str, mode: str
    ) -> IcebergCheckpointNamespace:
        if not isinstance(table_identifier, str) or not table_identifier:
            raise ValueError("Iceberg table identifier must be a non-empty string.")
        if not isinstance(table_uuid, str) or not table_uuid:
            raise ValueError("Iceberg table UUID must be a non-empty string.")
        if mode != _APPEND_MODE:
            raise ValueError(
                f"Iceberg checkpointing only supports append mode, got {mode!r}."
            )
        return IcebergCheckpointNamespace(
            format_version=_FORMAT_VERSION,
            table_identifier=table_identifier,
            table_uuid=table_uuid,
            mode=mode,
        )

    def _validate_namespace(
        self,
        actual: IcebergCheckpointNamespace,
        expected: IcebergCheckpointNamespace,
    ) -> None:
        if actual == expected:
            return
        mismatches = [
            field
            for field in asdict(expected)
            if getattr(actual, field) != getattr(expected, field)
        ]
        raise ValueError(
            f"Iceberg checkpoint namespace {self._namespace_path!r} does not match "
            f"the destination ({', '.join(mismatches)} mismatch). Use a different "
            "checkpoint path."
        )

    @staticmethod
    def _validate_checkpoint_file_type(path: str, file_type: FileType) -> None:
        if file_type not in (FileType.File, FileType.NotFound):
            raise ValueError(f"Iceberg row checkpoint {path!r} is not a file.")

    def _delete_file_if_present(self, path: str) -> None:
        info = self._filesystem.get_file_info(path)
        if info.type == FileType.File:
            self._filesystem.delete_file(path)
        elif info.type != FileType.NotFound:
            raise ValueError(f"Iceberg checkpoint artifact {path!r} is not a file.")

    @staticmethod
    def _encode_namespace(namespace: IcebergCheckpointNamespace) -> bytes:
        return (
            json.dumps(asdict(namespace), sort_keys=True, separators=(",", ":")) + "\n"
        ).encode("utf-8")

    @staticmethod
    def _retry_io(function, description: str):
        return call_with_retry(
            function,
            description=description,
            match=DataContext.get_current().retried_io_errors,
        )
