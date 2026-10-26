"""Ordered, bounded schema discovery shared by file datasources."""

import logging
import math
from array import array
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from threading import Lock
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

import pyarrow as pa
from pyarrow.fs import FileSystem

import ray
from ray import ObjectRef
from ray.data._internal.datasource_v2.interfaces.file_indexer import FileInfo
from ray.data._internal.datasource_v2.interfaces.file_manifest import (
    FILE_METADATA_TYPE,
    FILE_MTIME_COLUMN_NAME,
    FileManifest,
    FileMetadata,
)
from ray.data._internal.remote_fn import cached_remote_fn
from ray.data._internal.util import unify_schemas_with_validation
from ray.data.context import DataContext
from ray.util.scheduling_strategies import (
    NodeAffinitySchedulingStrategy,
    PlacementGroupSchedulingStrategy,
)

# Bound submitted reads, not just executing threads, for full inference.
_SCHEMA_READ_BATCH_SIZE = 256
_SCHEMA_READ_MIN_FILES = 2048
_SCHEMA_READ_MAX_TASKS = 16
_SCHEMA_METADATA_BUDGET = 32 * 1024 * 1024
_SCHEMA_TASK_METADATA_BUDGET = 1024 * 1024
# Conservative fixed-record allowance; footer bytes are budgeted separately.
_FILE_METADATA_RECORD_BYTES = 32

logger = logging.getLogger(__name__)


@dataclass
class FullSchemaInferenceResult:
    """Planned schema and optional, ordinary Arrow metadata for the same files."""

    schema: pa.Schema
    metadata: Optional[pa.ChunkedArray] = None


@dataclass
class _EncodedSchemas:
    schemas: List[bytes]
    indices: array

    def decode(self) -> List[pa.Schema]:
        schemas = [pa.ipc.read_schema(pa.BufferReader(value)) for value in self.schemas]
        return [schemas[index] for index in self.indices]


@dataclass
class _SchemaBatchSummary:
    schema: Optional[bytes]
    metadata: Optional[pa.Array]
    footer_bytes: int


class _FooterBudget:
    def __init__(self, limit: int):
        self.limit = limit
        self.remaining = limit
        self.lock = Lock()

    def encode(self, metadata) -> Optional[bytes]:
        # A metadata-only file adds the header, footer length and trailing magic.
        reserved = metadata.serialized_size + 12
        with self.lock:
            if reserved > self.remaining:
                return None
            self.remaining -= reserved
        with pa.BufferOutputStream() as output:
            metadata.write_metadata_file(output)
            payload = output.getvalue().to_pybytes()
        with self.lock:
            extra = len(payload) - reserved
            if extra > self.remaining:
                self.remaining += reserved
                return None
            self.remaining -= extra
        return payload


def _is_flat_schema(schema: pa.Schema) -> bool:
    if len(set(schema.names)) != len(schema):
        return False
    for field in schema:
        metadata = field.metadata or {}
        if any(key.startswith(b"ARROW:extension:") for key in metadata):
            return False
        dtype = field.type
        if not any(
            predicate(dtype)
            for predicate in (
                pa.types.is_null,
                pa.types.is_boolean,
                pa.types.is_integer,
                pa.types.is_floating,
                pa.types.is_decimal,
                pa.types.is_string,
                pa.types.is_large_string,
                pa.types.is_binary,
                pa.types.is_large_binary,
                pa.types.is_fixed_size_binary,
                pa.types.is_temporal,
            )
        ):
            return False
    return True


def _merge_same_types(
    left: Optional[pa.Schema], right: pa.Schema
) -> Optional[pa.Schema]:
    """Merge only flat fields whose types stay unchanged across all inputs."""
    if not _is_flat_schema(right):
        return None
    if left is None:
        return right
    types = {field.name: field.type for field in left}
    if any(field.name in types and types[field.name] != field.type for field in right):
        return None
    return unify_schemas_with_validation([left, right]) or left


def _encode_schemas(
    schemas: List[pa.Schema],
) -> Tuple[Optional[bytes], _EncodedSchemas]:
    unique: List[bytes] = []
    positions: Dict[bytes, int] = {}
    indices = array("I")
    merged = None
    eligible = True
    for schema in schemas:
        value = schema.serialize().to_pybytes()
        if value not in positions:
            positions[value] = len(unique)
            unique.append(value)
        indices.append(positions[value])
        if eligible:
            merged = _merge_same_types(merged, schema)
            eligible = merged is not None
    partial = merged.serialize().to_pybytes() if merged is not None else None
    return partial, _EncodedSchemas(unique, indices)


def _read_schema_batch(
    files: List[FileInfo],
    filesystem: FileSystem,
    format_name: str,
    row_records: int,
    footer_credit: int,
    context: DataContext,
    io_concurrency: int = 4,
) -> Tuple[_SchemaBatchSummary, _EncodedSchemas]:
    DataContext._set_current(context)
    budget = _FooterBudget(footer_credit)

    def read_file(item: Tuple[int, FileInfo]):
        index, file = item
        try:
            with filesystem.open_input_file(file.path) as source:
                if format_name == "Parquet":
                    import pyarrow.parquet as pq

                    reader = pq.ParquetFile(source)
                    # This is pq.read_schema's conversion, not schema_arrow.
                    schema = reader.schema.to_arrow_schema()
                    num_rows = reader.metadata.num_rows
                    footer = (
                        budget.encode(reader.metadata)
                        if footer_credit and index < row_records
                        else None
                    )
                elif format_name == "ORC":
                    import pyarrow.orc as orc

                    reader = orc.ORCFile(source)
                    schema = reader.schema
                    num_rows = reader.nrows
                    footer = None
                else:
                    raise ValueError(f"Unsupported schema format: {format_name}")
        except (OSError, pa.ArrowInvalid, pa.ArrowTypeError) as error:
            raise type(error)(
                f"Failed to read {format_name} schema from {file.path!r}: {error}"
            ) from error
        metadata = (
            FileMetadata(num_rows, footer, file.mtime_ns)
            if index < row_records
            else FileMetadata()
        )
        return schema, metadata

    with ThreadPoolExecutor(max_workers=min(io_concurrency, len(files))) as executor:
        results = list(executor.map(read_file, enumerate(files)))
    partial, encoded = _encode_schemas([schema for schema, _ in results])
    metadata = None
    if row_records:
        metadata = pa.array(
            [value.to_metadata() for _, value in results], type=FILE_METADATA_TYPE
        )
    return (
        _SchemaBatchSummary(partial, metadata, budget.limit - budget.remaining),
        encoded,
    )


def _schema_task_options(remote_args: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    scheduling_strategy = remote_args.get("scheduling_strategy", "DEFAULT")
    # Metadata tasks reserve 0.5 CPU independently of read tasks and may not fit
    # the placement group's bundles. Explicit None can inherit the caller's PG.
    if scheduling_strategy is None or isinstance(
        scheduling_strategy, PlacementGroupSchedulingStrategy
    ):
        logger.debug("Using driver schema reads for placement group scheduling")
        return None
    if (
        isinstance(scheduling_strategy, NodeAffinitySchedulingStrategy)
        and not scheduling_strategy.soft
    ):
        # The target node may fit read tasks but not the metadata task's CPU
        # reservation, even when other nodes have enough cluster resources.
        logger.debug("Using driver schema reads for hard node affinity")
        return None
    supported = {
        "runtime_env",
        "label_selector",
        "fallback_strategy",
        "scheduling_strategy",
    }
    ignored = {"num_cpus", "num_gpus", "memory", "max_calls", "accelerator_type"}
    if remote_args.get("resources") or set(remote_args) - supported - ignored - {
        "resources"
    }:
        logger.debug("Using driver schema reads for unsupported metadata task options")
        return None
    return {key: value for key, value in remote_args.items() if key in supported}


def infer_full_schema_from_files(
    sample: FileManifest,
    filesystem: FileSystem,
    *,
    format_name: str,
    ray_remote_args: Dict[str, Any],
    supports_distributed_reads: bool,
) -> FullSchemaInferenceResult:
    """Read every candidate with bounded tasks and retain a lossless merge path.

    Partial schemas are used only when every field stays in the same flat type.
    All other inputs replay their original ordered schema sequence to Ray's
    existing one-shot unifier. Metadata is an optional, budgeted optimization.
    """
    if not len(sample):
        return FullSchemaInferenceResult(pa.schema([]))
    block = sample.as_block()
    assert isinstance(block, pa.Table)
    mtimes = (
        block[FILE_MTIME_COLUMN_NAME].to_pylist()
        if FILE_MTIME_COLUMN_NAME in block.column_names
        else [None] * len(sample)
    )
    paths = sample.paths
    sizes = sample.file_sizes
    options = _schema_task_options(ray_remote_args)
    distributed = (
        supports_distributed_reads
        and options is not None
        and len(sample) > _SCHEMA_READ_MIN_FILES
    )
    window = (
        min(
            _SCHEMA_READ_MAX_TASKS,
            max(1, math.ceil(ray.cluster_resources().get("CPU", 1) * 2)),
        )
        if distributed
        else 1
    )
    # Four I/O threads per task should not reduce the driver's 16-way I/O.
    distributed = distributed and window >= 4
    if not distributed:
        window = 1
    read_remote = (
        cached_remote_fn(
            _read_schema_batch, num_cpus=0.5, max_retries=2, num_returns=2
        ).options(**(options or {}))
        if distributed
        else None
    )
    context = DataContext.get_current()
    row_limit = min(len(sample), _SCHEMA_METADATA_BUDGET // _FILE_METADATA_RECORD_BYTES)
    footer_available = (
        _SCHEMA_METADATA_BUDGET - row_limit * _FILE_METADATA_RECORD_BYTES
        if format_name == "Parquet"
        else 0
    )
    pending = deque()
    originals: List[Union[ObjectRef[_EncodedSchemas], _EncodedSchemas]] = []
    metadata_chunks = []
    merged = None
    eligible = True
    succeeded = False
    start = 0
    try:
        while start < len(sample) or pending:
            while start < len(sample) and len(pending) < window:
                stop = min(start + _SCHEMA_READ_BATCH_SIZE, len(sample))
                files = [
                    FileInfo(str(paths[i]), int(sizes[i]), mtime_ns=mtimes[i])
                    for i in range(start, stop)
                ]
                row_records = max(0, min(stop, row_limit) - start)
                footer_credit = min(
                    footer_available,
                    max(
                        0,
                        _SCHEMA_TASK_METADATA_BUDGET
                        - row_records * _FILE_METADATA_RECORD_BYTES,
                    ),
                )
                footer_available -= footer_credit
                args = (
                    files,
                    filesystem,
                    format_name,
                    row_records,
                    footer_credit,
                    context,
                )
                if read_remote is not None:
                    summary, original = read_remote.remote(*args)
                else:
                    summary, original = _read_schema_batch(*args, io_concurrency=16)
                pending.append((summary, footer_credit, stop - start))
                originals.append(original)
                start = stop
            summary, footer_credit, count = pending[0]
            if read_remote is not None:
                ray.wait([summary], num_returns=1)
                summary = ray.get(summary)
            pending.popleft()
            footer_available += footer_credit - summary.footer_bytes
            metadata_chunks.append(
                summary.metadata
                if summary.metadata is not None
                else pa.nulls(count, type=FILE_METADATA_TYPE)
            )
            if eligible:
                if summary.schema is None:
                    eligible = False
                else:
                    partial = pa.ipc.read_schema(pa.BufferReader(summary.schema))
                    merged = _merge_same_types(merged, partial)
                    eligible = merged is not None
        if not eligible:
            schemas = []
            for original in originals:
                encoded = (
                    original
                    if isinstance(original, _EncodedSchemas)
                    else ray.get(original)
                )
                schemas.extend(encoded.decode())
            merged = _merge_file_schemas(list(map(str, paths)), schemas, format_name)
        assert merged is not None
        metadata = (
            pa.chunked_array(metadata_chunks, type=FILE_METADATA_TYPE)
            if row_limit
            else None
        )
        succeeded = True
        return FullSchemaInferenceResult(merged, metadata)
    finally:
        if not succeeded and read_remote is not None:
            for summary, _, _ in pending:
                try:
                    ray.cancel(summary, force=False)
                except Exception:
                    logger.debug("Could not cancel schema task", exc_info=True)
        pending.clear()
        originals.clear()


def infer_schema_from_files(
    paths: List[str],
    read_schema: Callable[[str], pa.Schema],
    *,
    format_name: str,
) -> pa.Schema:
    """Read file schemas in input order and apply Ray's existing unifier.

    All schemas are unified together to preserve field order, metadata and type
    promotion. Pairwise merging is used only to diagnose a failed unification;
    it does not read any file again or replace the successful-path semantics.
    """
    if not paths:
        return pa.schema([])

    per_file_schemas: List[pa.Schema] = []
    with ThreadPoolExecutor(max_workers=min(len(paths), 16)) as executor:
        for start in range(0, len(paths), _SCHEMA_READ_BATCH_SIZE):
            batch = paths[start : start + _SCHEMA_READ_BATCH_SIZE]
            per_file_schemas.extend(executor.map(read_schema, batch))
    return _merge_file_schemas(paths, per_file_schemas, format_name)


def _merge_file_schemas(
    paths: List[str], per_file_schemas: List[pa.Schema], format_name: str
) -> pa.Schema:
    try:
        schema = unify_schemas_with_validation(per_file_schemas) or per_file_schemas[0]
    except (pa.ArrowTypeError, pa.ArrowInvalid):
        merged = per_file_schemas[0]
        for path, file_schema in zip(paths[1:], per_file_schemas[1:]):
            try:
                merged = unify_schemas_with_validation([merged, file_schema]) or merged
            except (pa.ArrowTypeError, pa.ArrowInvalid) as cause:
                raise type(cause)(
                    f"Failed to merge {format_name} schema from {path!r}: {cause}"
                ) from cause
        raise
    assert isinstance(schema, pa.Schema)
    return schema
