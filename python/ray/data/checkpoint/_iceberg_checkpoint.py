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
from typing import TYPE_CHECKING, Any, Dict, Iterable, List, Optional, Tuple

import pyarrow as pa
from pyarrow.fs import FileSelector, FileType

from ray.data._internal.datasource.iceberg_datasink import (
    IcebergDatasink,
    IcebergWriteResult,
)
from ray.data._internal.object_extensions.arrow import raise_on_pickle_object_columns
from ray.data._internal.savemode import SaveMode
from ray.data._internal.util import call_with_retry
from ray.data.block import Block, BlockAccessor
from ray.data.checkpoint.checkpoint_writer import BatchBasedCheckpointWriter
from ray.data.context import DataContext
from ray.data.datasource.datasink import Datasink, WriteResult
from ray.data.datasource.path_util import _unwrap_protocol

if TYPE_CHECKING:
    from pyiceberg.manifest import DataFile

    from ray.data._internal.execution.interfaces import TaskContext
    from ray.data.checkpoint.interfaces import CheckpointConfig

logger = logging.getLogger(__name__)

_FORMAT_VERSION = 1
_METADATA_DIR = "_iceberg"
_MANIFEST_DIR = "manifests"
_MANIFEST_RETENTION = 2
_OPERATION_PROPERTY = "ray.data.checkpoint.operation-id"

_PAYLOAD_SCHEMA = pa.schema(
    [
        pa.field("record_type", pa.string(), nullable=False),
        pa.field("content", pa.int32()),
        pa.field("file_path", pa.string()),
        pa.field("file_format", pa.string()),
        pa.field("partition_values", pa.list_(pa.binary())),
        pa.field("record_count", pa.int64()),
        pa.field("file_size_in_bytes", pa.int64()),
        pa.field("column_sizes", pa.map_(pa.int32(), pa.int64())),
        pa.field("value_counts", pa.map_(pa.int32(), pa.int64())),
        pa.field("null_value_counts", pa.map_(pa.int32(), pa.int64())),
        pa.field("nan_value_counts", pa.map_(pa.int32(), pa.int64())),
        pa.field("lower_bounds", pa.map_(pa.int32(), pa.binary())),
        pa.field("upper_bounds", pa.map_(pa.int32(), pa.binary())),
        pa.field("key_metadata", pa.binary()),
        pa.field("split_offsets", pa.list_(pa.int64())),
        pa.field("equality_ids", pa.list_(pa.int64())),
        pa.field("sort_order_id", pa.int32()),
        pa.field("spec_id", pa.int32()),
        pa.field("payload", pa.binary()),
    ]
)


def _enum_value(value: Any) -> Any:
    return value.value if hasattr(value, "value") else value


def _map_value(value: Optional[Dict[int, Any]]) -> Optional[List[Tuple[int, Any]]]:
    if value is None:
        return None
    return list(value.items())


def _serialize_arrow_table(table: pa.Table) -> bytes:
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, table.schema) as writer:
        writer.write_table(table)
    return sink.getvalue().to_pybytes()


def _deserialize_arrow_table(data: bytes) -> pa.Table:
    with pa.ipc.open_stream(pa.py_buffer(data)) as reader:
        table = reader.read_all()
    raise_on_pickle_object_columns(table)
    return table


def _serialize_scalar(value: Any) -> bytes:
    return _serialize_arrow_table(pa.table({"value": [value]}))


def _deserialize_scalar(data: bytes) -> Any:
    table = _deserialize_arrow_table(data)
    if table.column_names != ["value"] or table.num_rows != 1:
        raise ValueError("Invalid Iceberg partition value in checkpoint payload")
    return table["value"][0].as_py()


def _serialize_schema(schema: pa.Schema) -> bytes:
    return _serialize_arrow_table(pa.Table.from_batches([], schema=schema))


def _deserialize_schema(data: bytes) -> pa.Schema:
    return _deserialize_arrow_table(data).schema


def _data_file_to_row(data_file: "DataFile") -> Dict[str, Any]:
    try:
        spec_id = data_file.spec_id
    except AttributeError:
        # PyIceberg 0.11's Arrow writer doesn't attach a spec ID to newly
        # produced DataFiles. Preserve that state rather than inventing one.
        spec_id = None
    return {
        "record_type": "data_file",
        "content": int(_enum_value(data_file.content)),
        "file_path": str(data_file.file_path),
        "file_format": str(_enum_value(data_file.file_format)),
        "partition_values": [_serialize_scalar(value) for value in data_file.partition],
        "record_count": data_file.record_count,
        "file_size_in_bytes": data_file.file_size_in_bytes,
        "column_sizes": _map_value(data_file.column_sizes),
        "value_counts": _map_value(data_file.value_counts),
        "null_value_counts": _map_value(data_file.null_value_counts),
        "nan_value_counts": _map_value(data_file.nan_value_counts),
        "lower_bounds": _map_value(data_file.lower_bounds),
        "upper_bounds": _map_value(data_file.upper_bounds),
        "key_metadata": data_file.key_metadata,
        "split_offsets": data_file.split_offsets,
        "equality_ids": data_file.equality_ids,
        "sort_order_id": data_file.sort_order_id,
        "spec_id": spec_id,
        "payload": None,
    }


def _serialize_write_result(write_result: IcebergWriteResult) -> bytes:
    """Serialize recoverable Iceberg task output without using pickle.

    The payload contains every ``DataFile`` field needed by PyIceberg's append
    transaction and the Arrow schemas needed by Iceberg schema reconciliation.
    Deserialization validates the exact Arrow schema before constructing any
    PyIceberg objects.
    """
    if write_result.upsert_keys is not None:
        raise ValueError("Checkpointed Iceberg UPSERT is not supported")
    rows = [_data_file_to_row(data_file) for data_file in write_result.data_files]
    rows.extend(
        {
            "record_type": "schema",
            "payload": _serialize_schema(schema),
        }
        for schema in write_result.schemas
    )
    table = pa.Table.from_pylist(rows, schema=_PAYLOAD_SCHEMA)
    return _serialize_arrow_table(table)


def _pairs_to_dict(value: Any) -> Optional[Dict[int, Any]]:
    return dict(value) if value is not None else None


def _row_to_data_file(row: Dict[str, Any]) -> "DataFile":
    from pyiceberg.manifest import DataFile
    from pyiceberg.typedef import Record

    data_file = DataFile.from_args(
        content=row["content"],
        file_path=row["file_path"],
        file_format=row["file_format"],
        partition=Record(
            *[_deserialize_scalar(value) for value in row["partition_values"]]
        ),
        record_count=row["record_count"],
        file_size_in_bytes=row["file_size_in_bytes"],
        column_sizes=_pairs_to_dict(row["column_sizes"]),
        value_counts=_pairs_to_dict(row["value_counts"]),
        null_value_counts=_pairs_to_dict(row["null_value_counts"]),
        nan_value_counts=_pairs_to_dict(row["nan_value_counts"]),
        lower_bounds=_pairs_to_dict(row["lower_bounds"]),
        upper_bounds=_pairs_to_dict(row["upper_bounds"]),
        key_metadata=row["key_metadata"],
        split_offsets=row["split_offsets"],
        equality_ids=row["equality_ids"],
        sort_order_id=row["sort_order_id"],
    )
    if row["spec_id"] is not None:
        data_file.spec_id = row["spec_id"]
    return data_file


def _deserialize_write_result(data: bytes) -> IcebergWriteResult:
    table = _deserialize_arrow_table(data)
    if not table.schema.equals(_PAYLOAD_SCHEMA):
        raise ValueError("Unsupported Iceberg checkpoint Arrow schema")

    data_files = []
    schemas = []
    for row in table.to_pylist():
        if row["record_type"] == "data_file":
            data_files.append(_row_to_data_file(row))
        elif row["record_type"] == "schema" and row["payload"] is not None:
            schemas.append(_deserialize_schema(row["payload"]))
        else:
            raise ValueError("Invalid record in Iceberg checkpoint Arrow payload")
    return IcebergWriteResult(data_files=data_files, schemas=schemas)


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
        self._root = _unwrap_protocol(config.checkpoint_path)
        self._metadata_root = posixpath.join(self._root, _METADATA_DIR)
        self._sink = sink
        self._retried_io_errors = DataContext.get_current().retried_io_errors
        self._table_uuid: Optional[str] = None
        self._operation_id: Optional[str] = None

    @property
    def operation_id(self) -> str:
        if self._operation_id is None:
            raise RuntimeError("Iceberg checkpoint generation has not been initialized")
        return self._operation_id

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

    def _manifest_dir(self) -> str:
        return posixpath.join(self._metadata_root, _MANIFEST_DIR)

    def _manifest_path(self, revision: int) -> str:
        return posixpath.join(self._manifest_dir(), f"{revision:020d}.json")

    def _generation_root(self, operation_id: Optional[str] = None) -> str:
        return posixpath.join(
            self._metadata_root, "generations", operation_id or self.operation_id
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
        self._filesystem.create_dir(self._root, recursive=True)
        manifest = self._load_manifest(required=False)

        root_files = self._filesystem.get_file_info(
            FileSelector(self._root, recursive=True, allow_not_found=True)
        )
        committed_row_files = [
            entry.path
            for entry in root_files
            if entry.type == FileType.File
            and entry.path.endswith(".parquet")
            and not entry.path.endswith(".pending.parquet")
        ]

        if manifest is None:
            if committed_row_files:
                raise ValueError(
                    "Checkpointed Iceberg writes cannot restore a row-only checkpoint "
                    "directory because destination file metadata is missing. Use a new "
                    "checkpoint path."
                )
            operation_id = uuid.uuid4().hex
            manifest = {
                "format_version": _FORMAT_VERSION,
                "revision": self._next_manifest_revision(),
                "table_identifier": self._sink.table_identifier,
                "table_uuid": table_uuid,
                "mode": SaveMode.APPEND.value,
                "active_generation": operation_id,
                "generations": {operation_id: "active"},
            }
            self._write_manifest(manifest)
            self._operation_id = operation_id
            return

        self._validate_identity(manifest, table_uuid)
        active = manifest["active_generation"]
        if active is None:
            active = uuid.uuid4().hex
            manifest = self._next_manifest(manifest)
            manifest["active_generation"] = active
            manifest["generations"][active] = "active"
            self._write_manifest(manifest)
        elif manifest["generations"].get(active) != "active":
            raise ValueError("Invalid active Iceberg checkpoint generation")
        self._operation_id = active

    def _manifest_paths(self) -> List[Tuple[int, str]]:
        entries = self._filesystem.get_file_info(
            FileSelector(self._manifest_dir(), recursive=False, allow_not_found=True)
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

    def _next_manifest(self, manifest: Dict[str, Any]) -> Dict[str, Any]:
        return {
            **manifest,
            "revision": max(manifest["revision"] + 1, self._next_manifest_revision()),
            "generations": manifest["generations"].copy(),
        }

    @staticmethod
    def _is_complete_manifest(manifest: Dict[str, Any], revision: int) -> bool:
        required_fields = {
            "format_version",
            "revision",
            "table_identifier",
            "table_uuid",
            "mode",
            "active_generation",
            "generations",
        }
        return (
            set(manifest) == required_fields
            and manifest["format_version"] == _FORMAT_VERSION
            and manifest["revision"] == revision
            and isinstance(manifest["table_identifier"], str)
            and isinstance(manifest["table_uuid"], str)
            and isinstance(manifest["mode"], str)
            and isinstance(manifest["generations"], dict)
            and all(
                isinstance(key, str) and value in {"active", "terminal"}
                for key, value in manifest["generations"].items()
            )
            and (
                manifest["active_generation"] is None
                or isinstance(manifest["active_generation"], str)
            )
        )

    def _write_manifest(self, manifest: Dict[str, Any]) -> None:
        """Publish a manifest revision, then retain two valid revisions.

        Publishing a new immutable file preserves the previous valid state if
        the write stops midway. After publication succeeds, compaction keeps the
        current revision and one valid fallback. Invalid interrupted revisions
        never replace that fallback.
        """
        self._write_json(self._manifest_path(manifest["revision"]), manifest)

        # Keep the current manifest and one valid fallback without retaining an
        # ever-growing sequence of full manifest snapshots. Malformed interrupted
        # revisions do not consume the fallback slot.
        valid_manifests = 0
        for revision, path in self._manifest_paths():
            try:
                candidate = self._load_json(path)
                is_valid = self._is_complete_manifest(candidate, revision)
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

    def _load_manifest(self, *, required: bool = True) -> Optional[Dict[str, Any]]:
        for revision, path in self._manifest_paths():
            try:
                manifest = self._load_json(path)
            except ValueError:
                logger.warning(
                    "Ignoring incomplete Iceberg checkpoint manifest %s", path
                )
                continue
            if manifest.get("format_version") != _FORMAT_VERSION:
                raise ValueError(
                    "Unsupported Iceberg checkpoint namespace format version: "
                    f"{manifest.get('format_version')}"
                )
            if not self._is_complete_manifest(manifest, revision):
                logger.warning(
                    "Ignoring incomplete Iceberg checkpoint manifest %s", path
                )
                continue
            return manifest
        if required:
            raise ValueError("No valid Iceberg checkpoint namespace manifest found")
        return None

    def _validate_identity(self, manifest: Dict[str, Any], table_uuid: str) -> None:
        expected = (self._sink.table_identifier, table_uuid, SaveMode.APPEND.value)
        actual = (
            manifest["table_identifier"],
            manifest["table_uuid"],
            manifest["mode"],
        )
        if actual != expected:
            raise ValueError(
                "Checkpoint path belongs to a different Iceberg table or write mode: "
                f"expected {expected}, found {actual}"
            )

    def task_artifact_id(self, task_id: str, write_result: IcebergWriteResult) -> str:
        del write_result
        if not task_id or posixpath.basename(task_id) != task_id:
            raise ValueError(f"Invalid Iceberg checkpoint task ID: {task_id!r}")
        return task_id

    def persist_task_result(
        self,
        artifact_id: str,
        checkpoint_path: str,
        write_result: IcebergWriteResult,
    ) -> None:
        """Publish destination metadata before committing a row checkpoint.

        The Arrow payload preserves the Iceberg write result. The JSON envelope
        binds that payload to its operation, table, hash, and future Parquet row
        checkpoint. ``write_task_checkpoint`` publishes the Parquet marker only
        after this method succeeds.

        Args:
            artifact_id: Deterministic ID for this operation, write, and task.
            checkpoint_path: Final path of the task's row checkpoint.
            write_result: Iceberg data files and schemas produced by the task.
        """
        if not write_result.data_files:
            raise ValueError(
                "Iceberg write produced rows without recoverable destination files"
            )
        payload = _serialize_write_result(write_result)
        payload_hash = hashlib.sha256(payload).hexdigest()
        if not artifact_id or posixpath.basename(artifact_id) != artifact_id:
            raise ValueError(f"Invalid Iceberg checkpoint artifact ID: {artifact_id!r}")
        task_root = posixpath.join(self._generation_root(), "tasks")
        payload_path = posixpath.join(task_root, f"{artifact_id}.arrow")
        envelope_path = posixpath.join(task_root, f"{artifact_id}.json")
        self._write_bytes(payload_path, payload)
        self._write_json(
            envelope_path,
            {
                "format_version": _FORMAT_VERSION,
                "operation_id": self.operation_id,
                "table_uuid": self._table_uuid,
                "checkpoint_file": posixpath.basename(checkpoint_path),
                "payload_file": posixpath.basename(payload_path),
                "payload_sha256": payload_hash,
            },
        )

    def find_committed_task_checkpoint(self, task_id: str) -> Optional[str]:
        """Return a task's committed row marker using one exact metadata lookup.

        Deterministic task IDs avoid a namespace-wide object-store listing for
        every write task. The partition filter still controls whether recovery
        selects the exact checkpoint.
        """
        if not task_id or posixpath.basename(task_id) != task_id:
            raise ValueError(f"Invalid Iceberg checkpoint task ID: {task_id!r}")
        path = posixpath.join(self._root, f"{task_id}.parquet")
        if self._filesystem.get_file_info(path).type == FileType.NotFound:
            return None
        partition_filter = self._config.checkpoint_path_partition_filter
        if partition_filter is not None and path not in partition_filter([path]):
            return None
        return path

    def load_task_result_if_present(
        self, artifact_id: str
    ) -> Optional[IcebergWriteResult]:
        """Load metadata published before an interrupted row-marker commit.

        A task retry can reuse the original destination files and finish the
        row checkpoint instead of producing a second recoverable task result.
        A payload without an envelope never completed metadata publication, so
        the retry removes that partial payload and starts again.
        """
        envelope_path = posixpath.join(
            self._generation_root(), "tasks", f"{artifact_id}.json"
        )
        if self._filesystem.get_file_info(envelope_path).type == FileType.NotFound:
            # A payload without its envelope was never published. Remove it so a
            # retry can safely publish the deterministic task artifact.
            payload_path = posixpath.join(
                self._generation_root(), "tasks", f"{artifact_id}.arrow"
            )
            if self._filesystem.get_file_info(payload_path).type != FileType.NotFound:
                self._filesystem.delete_file(payload_path)
            return None
        return self.load_task_result(
            posixpath.join(self._root, f"{artifact_id}.parquet")
        )

    def load_task_result(self, checkpoint_path: str) -> IcebergWriteResult:
        checkpoint_file = posixpath.basename(checkpoint_path)
        envelope = self._load_task_envelope(checkpoint_file)
        payload_path = posixpath.join(
            self._generation_root(), "tasks", envelope["payload_file"]
        )
        payload = self._read_bytes(payload_path)
        if hashlib.sha256(payload).hexdigest() != envelope["payload_sha256"]:
            raise ValueError(f"Corrupt Iceberg checkpoint payload: {payload_path}")
        return _deserialize_write_result(payload)

    def _selected_row_checkpoint_paths(self) -> List[str]:
        entries = self._filesystem.get_file_info(
            FileSelector(
                self._root,
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
            and not entry.path.startswith(f"{self._metadata_root}/")
        ]
        partition_filter = self._config.checkpoint_path_partition_filter
        return partition_filter(paths) if partition_filter is not None else paths

    def _load_task_envelope(self, checkpoint_file: str) -> Dict[str, Any]:
        artifact_id = checkpoint_file[: -len(".parquet")]
        envelope_path = posixpath.join(
            self._generation_root(), "tasks", f"{artifact_id}.json"
        )
        if self._filesystem.get_file_info(envelope_path).type == FileType.NotFound:
            raise ValueError(
                "Iceberg row checkpoint is missing its destination metadata: "
                f"{checkpoint_file}"
            )
        envelope = self._load_json(envelope_path)
        required = {
            "format_version",
            "operation_id",
            "table_uuid",
            "checkpoint_file",
            "payload_file",
            "payload_sha256",
        }
        if (
            set(envelope) != required
            or not all(
                isinstance(envelope[key], str)
                for key in (
                    "operation_id",
                    "table_uuid",
                    "checkpoint_file",
                    "payload_file",
                    "payload_sha256",
                )
            )
            or envelope["checkpoint_file"] != checkpoint_file
            or posixpath.basename(envelope["payload_file"]) != envelope["payload_file"]
            or envelope["payload_file"] != f"{artifact_id}.arrow"
            or len(envelope["payload_sha256"]) != 64
        ):
            raise ValueError(
                f"Invalid Iceberg checkpoint task envelope: {envelope_path}"
            )
        if (
            envelope["format_version"] != _FORMAT_VERSION
            or envelope["operation_id"] != self.operation_id
            or envelope["table_uuid"] != self._table_uuid
        ):
            raise ValueError(f"Incompatible Iceberg checkpoint task: {envelope_path}")
        return envelope

    def load_active_results(self) -> List[IcebergWriteResult]:
        """Load recoverable task results from the current active generation.

        Terminal generations contribute row IDs to generic checkpoint filtering,
        but this method intentionally ignores their destination metadata. This
        separation prevents Ray from appending a completed generation twice.
        Unknown generations fail closed because Ray can't determine whether
        their rows and destination files form a valid recovery unit.
        """
        manifest = self._load_manifest()
        assert manifest is not None
        assert self._table_uuid is not None
        self._validate_identity(manifest, self._table_uuid)
        if manifest["active_generation"] != self.operation_id:
            raise ValueError(
                "Iceberg checkpoint active generation changed concurrently"
            )

        selected = self._selected_row_checkpoint_paths()
        known_generations = manifest["generations"]
        results = []
        for checkpoint_path in selected:
            checkpoint_file = posixpath.basename(checkpoint_path)
            generation_id = checkpoint_file.split("-", 1)[0]
            status = known_generations.get(generation_id)
            if status == "terminal":
                continue
            if generation_id != self.operation_id or status != "active":
                raise ValueError(
                    "Found an Iceberg row checkpoint without a known recoverable "
                    f"generation: {checkpoint_file}"
                )
            results.append(self.load_task_result(checkpoint_path))
        return results

    def discard_task_results(self, results: List[IcebergWriteResult]) -> None:
        """Remove task checkpoints that a newly visible snapshot didn't commit.

        An ambiguous earlier append can become visible after the current tasks
        finish. Before terminalizing that earlier generation, remove checkpoints
        for the current task outputs so their row IDs remain eligible on retry.
        Delete each row marker before its metadata to preserve that safety rule
        if cleanup stops midway. Fail closed unless every current data file maps
        to a checkpoint.
        """
        current_paths = {
            str(data_file.file_path)
            for result in results
            for data_file in result.data_files
        }
        discarded_paths = set()
        for checkpoint_path in self._selected_row_checkpoint_paths():
            checkpoint_file = posixpath.basename(checkpoint_path)
            if not checkpoint_file.startswith(f"{self.operation_id}-"):
                continue
            result = self.load_task_result(checkpoint_path)
            result_paths = {str(data_file.file_path) for data_file in result.data_files}
            if not result_paths.intersection(current_paths):
                continue
            if not result_paths.issubset(current_paths):
                raise ValueError(
                    "Iceberg task checkpoint only partially matches late task results"
                )

            # Delete the row checkpoint first. Once it is gone these IDs cannot be
            # incorrectly filtered, even if metadata cleanup is interrupted.
            self._filesystem.delete_file(checkpoint_path)
            artifact_id = checkpoint_file[: -len(".parquet")]
            task_root = posixpath.join(self._generation_root(), "tasks")
            for extension in (".json", ".arrow"):
                path = posixpath.join(task_root, f"{artifact_id}{extension}")
                if self._filesystem.get_file_info(path).type != FileType.NotFound:
                    self._filesystem.delete_file(path)
            discarded_paths.update(result_paths)

        if discarded_paths != current_paths:
            missing = sorted(current_paths - discarded_paths)
            raise ValueError(
                "Could not identify row checkpoints for late Iceberg task results: "
                f"{missing}"
            )

    def mark_terminal(self) -> None:
        """Close the active generation after confirming its Iceberg snapshot."""
        manifest = self._load_manifest()
        assert manifest is not None
        if manifest["active_generation"] != self.operation_id:
            raise ValueError(
                "Iceberg checkpoint active generation changed concurrently"
            )
        manifest = self._next_manifest(manifest)
        manifest["generations"][self.operation_id] = "terminal"
        manifest["active_generation"] = None
        self._write_manifest(manifest)

    def discard_empty_generation(self) -> None:
        manifest = self._load_manifest()
        assert manifest is not None
        if manifest["active_generation"] != self.operation_id:
            raise ValueError(
                "Iceberg checkpoint active generation changed concurrently"
            )
        manifest = self._next_manifest(manifest)
        manifest["generations"].pop(self.operation_id, None)
        manifest["active_generation"] = None
        self._write_manifest(manifest)
        generation_root = self._generation_root()
        if self._filesystem.get_file_info(generation_root).type != FileType.NotFound:
            self._filesystem.delete_dir(generation_root)

    def delete_namespace(self) -> None:
        if self._filesystem.get_file_info(self._root).type != FileType.NotFound:
            self._filesystem.delete_dir(self._root)


class IcebergCheckpointDatasink(Datasink[IcebergWriteResult]):
    """Add checkpoint recovery and commit detection to an Iceberg datasink.

    Workers still delegate file creation to ``IcebergDatasink``. This wrapper
    coordinates task metadata and changes driver completion into a recovery
    protocol: detect an earlier commit, merge active recovered files with this
    attempt's files, commit them with an operation marker, verify that marker,
    and only then terminalize or delete checkpoint state.
    """

    def __init__(self, sink: IcebergDatasink, config: "CheckpointConfig"):
        self._sink = sink
        self._config = config
        self._coordinator = IcebergCheckpointCoordinator(config, sink)
        self._checkpointing_active = False

    def enable_checkpointing(self) -> None:
        """Validate supported settings and resolve the generation before reads.

        This method runs before checkpoint filtering. If the active operation's
        snapshot has become visible since an ambiguous failure, it marks that
        generation terminal and starts a fresh one. The retained terminal row
        IDs then filter rows that the snapshot already contains, while new rows
        enter the fresh generation.
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
        if _OPERATION_PROPERTY in self._sink._snapshot_properties:
            raise ValueError(
                f"Snapshot property {_OPERATION_PROPERTY!r} is reserved by Ray Data "
                "checkpointing"
            )
        self._sink._reload_table()
        table_uuid = str(self._sink._table.metadata.table_uuid)
        self._coordinator.initialize(table_uuid)
        if self._find_operation_snapshot() is not None:
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

    def _find_operation_snapshot(self) -> Optional[Any]:
        """Refresh the table and find this generation's catalog commit marker."""
        self._sink._reload_table()
        for snapshot in self._sink._table.snapshots():
            if (
                snapshot.summary is not None
                and snapshot.summary.get(_OPERATION_PROPERTY)
                == self._coordinator.operation_id
            ):
                return snapshot
        return None

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
        seen: Dict[str, bytes] = {}
        for result in [*recovered, *current]:
            unique_files = []
            for data_file in result.data_files:
                path = str(data_file.file_path)
                fingerprint = hashlib.sha256(
                    _serialize_write_result(
                        IcebergWriteResult(
                            data_files=[data_file], schemas=result.schemas
                        )
                    )
                ).digest()
                previous = seen.get(path)
                if previous is not None:
                    if previous != fingerprint:
                        raise ValueError(
                            "Conflicting Iceberg checkpoint metadata for data file "
                            f"{path}"
                        )
                    continue
                seen[path] = fingerprint
                unique_files.append(data_file)
            if unique_files:
                merged.append(
                    IcebergWriteResult(data_files=unique_files, schemas=result.schemas)
                )
        return merged

    def on_write_complete(self, write_result: WriteResult[IcebergWriteResult]) -> None:
        """Recover task files, commit once, and finalize checkpoint state.

        The snapshot marker acts as the catalog-side commit record. If the
        marker appears before this method starts, an earlier attempt committed.
        If current tasks also produced files, remove their row checkpoints and
        retry them under a new generation. Otherwise merge active task metadata,
        append the unique data files, and verify the marker before treating the
        generation as complete.
        """
        if not self._checkpointing_active:
            self._sink.on_write_complete(write_result)
            return

        if self._find_operation_snapshot() is not None:
            current = [
                result
                for result in write_result.write_returns
                if result is not None and result.data_files
            ]
            if current:
                # The visible snapshot belongs to the earlier attempt. These task
                # outputs were produced after it and must not remain as row
                # checkpoints in the generation that is about to become terminal.
                self._coordinator.discard_task_results(current)
                self._coordinator.mark_terminal()
                raise RuntimeError(
                    "Iceberg checkpoint generation was committed concurrently while "
                    "new task results were being written; late task checkpoints were "
                    "discarded and will be retried"
                )
            self._coordinator.mark_terminal()
            self._cleanup_after_success()
            return

        recovered = self._coordinator.load_active_results()
        current = [
            result for result in write_result.write_returns if result is not None
        ]
        merged = self._merge_results(recovered, current)
        if not merged:
            self._coordinator.discard_empty_generation()
            self._cleanup_after_success()
            return

        original_properties = self._sink._snapshot_properties
        self._sink._snapshot_properties = {
            **original_properties,
            _OPERATION_PROPERTY: self._coordinator.operation_id,
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

        if self._find_operation_snapshot() is None:
            raise RuntimeError(
                "Iceberg commit succeeded without the expected Ray checkpoint marker"
            )
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
    task_id = f"{datasink.coordinator.operation_id}-{write_uuid}-{ctx.task_idx}"
    committed_path = datasink.coordinator.find_committed_task_checkpoint(task_id)
    if committed_path is not None:
        ctx.kwargs["_datasink_write_return"] = datasink.coordinator.load_task_result(
            committed_path
        )
        return

    artifact_id = datasink.coordinator.task_artifact_id(task_id, write_result)
    recovered_result = datasink.coordinator.load_task_result_if_present(artifact_id)
    if recovered_result is not None:
        write_result = recovered_result
        ctx.kwargs["_datasink_write_return"] = recovered_result

    id_column_data = BlockAccessor.for_block(
        block.select(columns=[datasink._config.id_column])
    ).to_arrow()[datasink._config.id_column]
    pending = checkpoint_writer.write_pending_checkpoint(
        id_column_data, checkpoint_id=artifact_id
    )
    if pending is None:
        return
    if recovered_result is None:
        datasink.coordinator.persist_task_result(
            artifact_id, pending.committed_path, write_result
        )
    checkpoint_writer.commit_checkpoint(pending)
