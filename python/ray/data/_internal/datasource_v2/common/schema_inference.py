"""Ordered, bounded schema and metadata discovery shared by file datasources."""

import logging
import math
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from threading import Lock
from typing import Any, Callable, Dict, List, Optional, Tuple

import pyarrow as pa
from pyarrow.fs import FileSystem

import ray
from ray import ObjectRef
from ray._common.utils import env_float, env_integer
from ray.data._internal.datasource_v2.interfaces.file_manifest import (
    FILE_METADATA_TYPE,
    FILE_MTIME_COLUMN_NAME,
    FileManifest,
    FileMetadata,
)
from ray.data._internal.execution.util import merge_label_selector
from ray.data._internal.remote_fn import cached_remote_fn
from ray.data._internal.util import (
    call_with_retry,
    filesystem_for_sized_opens,
    unify_schemas_with_validation,
)
from ray.data.context import DataContext
from ray.util.scheduling_strategies import (
    NodeAffinitySchedulingStrategy,
    PlacementGroupSchedulingStrategy,
)

_SCHEMA_READ_BATCH_SIZE = 256
_FULL_SCHEMA_INFERENCE_MIN_FILES = env_integer(
    "RAY_DATA_FULL_SCHEMA_INFERENCE_MIN_FILES", 2048
)
_FULL_SCHEMA_INFERENCE_MAX_TASKS = env_integer(
    "RAY_DATA_FULL_SCHEMA_INFERENCE_MAX_TASKS", 16
)
_FULL_SCHEMA_INFERENCE_NUM_CPUS = env_float(
    "RAY_DATA_FULL_SCHEMA_INFERENCE_NUM_CPUS", 0.5
)
_FULL_SCHEMA_INFERENCE_BATCH_SIZE = env_integer(
    "RAY_DATA_FULL_SCHEMA_INFERENCE_BATCH_SIZE", 256
)
_FULL_SCHEMA_INFERENCE_IO_CONCURRENCY = env_integer(
    "RAY_DATA_FULL_SCHEMA_INFERENCE_IO_CONCURRENCY", 4
)
_FULL_SCHEMA_INFERENCE_FIRST_RESULT_TIMEOUT_S = 30
# Cached metadata payload budgets, internal constants rather than public
# DataContext config. Zero disables caching. Index scaffolding, schemas and
# transient copies are excluded from these byte budgets.
_FULL_SCHEMA_INFERENCE_METADATA_BUDGET_BYTES = env_integer(
    "RAY_DATA_FULL_SCHEMA_INFERENCE_METADATA_BUDGET_BYTES", 32 * 1024 * 1024
)
_FULL_SCHEMA_INFERENCE_TASK_METADATA_BUDGET_BYTES = env_integer(
    "RAY_DATA_FULL_SCHEMA_INFERENCE_TASK_METADATA_BUDGET_BYTES", 1024 * 1024
)
_FILE_METADATA_RECORD_BYTES = 32
logger = logging.getLogger(__name__)


@dataclass
class FullSchemaInferenceResult:
    """Schema and optional ordinary Arrow values for the same candidates."""

    schema: pa.Schema
    metadata: Optional[pa.ChunkedArray] = None


@dataclass
class _SchemaBatchResult:
    schemas: List[Tuple[int, bytes, bool]]
    metadata: Optional[pa.Array]
    footer_bytes: int


class _FooterBudget:
    def __init__(self, limit: int):
        self.limit = limit
        self.remaining = limit
        self.lock = Lock()

    def encode(self, metadata) -> Optional[bytes]:
        # A metadata-only file adds its header, footer length and trailing magic.
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


def _add_unique_schema(
    unique: Dict[bytes, Tuple[int, bytes, bool]],
    index: int,
    schema: pa.Schema,
    value: bytes,
) -> None:
    if value in unique:
        return
    decoded = pa.ipc.read_schema(pa.BufferReader(value))
    if schema.equals(decoded, check_metadata=True):
        unique[value] = (index, value, False)
        return
    import pyarrow.parquet as pq

    output = pa.BufferOutputStream()
    pq.write_metadata(schema, output)
    encoded = output.getvalue().to_pybytes()
    restored = pq.read_schema(pa.BufferReader(encoded))
    if not schema.equals(restored, check_metadata=True):
        raise pa.ArrowInvalid(
            "Unable to preserve schema through IPC or Parquet metadata"
        )
    unique[value] = (index, encoded, True)


def _decode_schema(value: bytes, parquet_encoded: bool) -> pa.Schema:
    if parquet_encoded:
        import pyarrow.parquet as pq

        return pq.read_schema(pa.BufferReader(value))
    return pa.ipc.read_schema(pa.BufferReader(value))


def _read_schema_batch(
    paths: List[str],
    sizes: List[Optional[int]],
    filesystem: FileSystem,
    read_schema: Callable,
    format_name: str,
    context: DataContext,
    io_concurrency: int,
) -> List[Tuple[int, bytes, bool]]:
    DataContext._set_current(context)

    def read_file(item: Tuple[str, Optional[int]]) -> Tuple[pa.Schema, bytes]:
        path, size = item
        try:
            schema = read_schema(path, filesystem, size)
            return schema, schema.serialize().to_pybytes()
        except (OSError, pa.ArrowInvalid, pa.ArrowTypeError) as error:
            raise type(error)(
                f"Failed to read {format_name} schema from {path!r}: {error}"
            ) from error

    with ThreadPoolExecutor(max_workers=min(io_concurrency, len(paths))) as executor:
        values = executor.map(read_file, zip(paths, sizes))
        unique: Dict[bytes, Tuple[int, bytes, bool]] = {}
        for index, (schema, value) in enumerate(values):
            _add_unique_schema(unique, index, schema, value)
    return list(unique.values())


def _read_schema_metadata_batch(
    paths: List[str],
    sizes: Optional[List[Optional[int]]],
    filesystem: FileSystem,
    format_name: str,
    context: DataContext,
    row_records: int = 0,
    footer_credit: int = 0,
    mtimes: Optional[List[Optional[int]]] = None,
    *,
    io_concurrency: Optional[int] = None,
) -> _SchemaBatchResult:
    """Read every schema once and, within budget, file metadata in the same open.

    Schemas are batch-deduplicated and their first positions restored on the
    driver (using the schema-only path's lossless encoding). Metadata stays
    file-specific and aligned with the input
    order, so deduplication never reorders or drops per-file records. Parquet
    reads use the listed file size when available to spare the extra size
    request, matching the schema-only path.
    """
    DataContext._set_current(context)
    if format_name != "Parquet":
        raise ValueError(f"Unsupported schema format: {format_name}")
    budget = _FooterBudget(footer_credit)

    def read_file(item: Tuple[int, str]):
        index, path = item
        size = sizes[index] if sizes is not None else None
        metadata: Optional[FileMetadata] = None
        try:
            import pyarrow.dataset as pds

            fragment = pds.ParquetFileFormat().make_fragment(
                path,
                filesystem=filesystem_for_sized_opens(filesystem),
                file_size=size,
            )
            parquet_metadata = call_with_retry(
                lambda: fragment.metadata,
                description=f"read Parquet footer for {path}",
                match=context.retried_io_errors,
            )
            # This is pq.read_schema's conversion, not schema_arrow.
            schema = parquet_metadata.schema.to_arrow_schema()
            if index < row_records:
                metadata = FileMetadata(
                    parquet_metadata.num_rows,
                    budget.encode(parquet_metadata) if footer_credit else None,
                    mtimes[index] if mtimes is not None else None,
                )
            value = schema.serialize().to_pybytes()
            return schema, value, metadata
        except (OSError, pa.ArrowInvalid, pa.ArrowTypeError) as error:
            raise type(error)(
                f"Failed to read {format_name} schema from {path!r}: {error}"
            ) from error

    concurrency = (
        io_concurrency
        if io_concurrency is not None
        else _FULL_SCHEMA_INFERENCE_IO_CONCURRENCY
    )
    unique: Dict[bytes, Tuple[int, bytes, bool]] = {}
    metadata_values: List[Optional[FileMetadata]] = []
    with ThreadPoolExecutor(max_workers=min(concurrency, len(paths))) as executor:
        for index, (schema, value, file_metadata) in enumerate(
            executor.map(read_file, enumerate(paths))
        ):
            metadata_values.append(file_metadata)
            _add_unique_schema(unique, index, schema, value)
    metadata = (
        pa.array(
            [
                value.to_metadata() if value is not None else None
                for value in metadata_values
            ],
            type=FILE_METADATA_TYPE,
        )
        if row_records
        else None
    )
    return _SchemaBatchResult(
        list(unique.values()), metadata, budget.limit - budget.remaining
    )


def _schema_task_options(
    remote_args: Dict[str, Any], num_cpus: float
) -> Optional[Dict[str, Any]]:
    read_num_cpus = remote_args.get("num_cpus", 1)
    if read_num_cpus is None:
        # Unlike an absent key, explicit None adds no CPU resource in Ray Core.
        read_num_cpus = 0
    if read_num_cpus < num_cpus:
        # Do not require more CPU for planning than the requested read can use
        # on its eligible nodes, including label-selected zero-CPU nodes.
        logger.debug("Using driver schema reads for a low read CPU request")
        return None
    scheduling_strategy = remote_args.get("scheduling_strategy", "DEFAULT")
    # Schema tasks reserve CPU independently of read tasks and may not fit
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
    read_schema: Callable,
    ray_remote_args: Dict[str, Any],
    supports_distributed_reads: bool,
) -> Optional[pa.Schema]:
    """Read schemas on workers and merge once in candidate order on the driver.

    Return None when the caller should use its existing driver inference.
    Each task returns distinct schemas and their first positions. Schemas that
    cannot round-trip through IPC use a metadata-only Parquet representation.
    The driver restores candidate order before the one-shot merge. The first
    batch is a single file so progress lands early; if the first distributed
    result does not arrive within ``_FULL_SCHEMA_INFERENCE_FIRST_RESULT_TIMEOUT_S``
    seconds, pending tasks are cancelled and the caller falls back to driver
    schema reads.
    """
    context = DataContext.get_current()
    paths: List[str] = sample.paths.tolist()
    if not paths:
        return pa.schema([])
    if not supports_distributed_reads or len(paths) <= _FULL_SCHEMA_INFERENCE_MIN_FILES:
        return None
    options = _schema_task_options(
        merge_label_selector(ray_remote_args, context.execution_options.label_selector),
        _FULL_SCHEMA_INFERENCE_NUM_CPUS,
    )
    if options is None:
        return None
    batch_size = _FULL_SCHEMA_INFERENCE_BATCH_SIZE
    window = min(
        _FULL_SCHEMA_INFERENCE_MAX_TASKS,
        max(
            1,
            math.ceil(
                ray.cluster_resources().get("CPU", 1) / _FULL_SCHEMA_INFERENCE_NUM_CPUS
            ),
        ),
    )
    if window * min(_FULL_SCHEMA_INFERENCE_IO_CONCURRENCY, batch_size) < min(
        16, batch_size
    ):
        return None
    read_remote = cached_remote_fn(_read_schema_batch, max_retries=2).options(
        num_cpus=_FULL_SCHEMA_INFERENCE_NUM_CPUS, **options
    )
    sizes = sample.known_file_sizes
    schemas: List[Tuple[int, pa.Schema]] = []
    pending: Dict[ObjectRef[List[Tuple[int, bytes, bool]]], int] = {}
    start = 0
    first_result = True
    try:
        while start < len(paths) or pending:
            while start < len(paths) and len(pending) < window:
                # Let the first task report progress without reading a full batch.
                stop = min(start + (1 if start == 0 else batch_size), len(paths))
                ref = read_remote.remote(
                    paths[start:stop],
                    sizes[start:stop],
                    filesystem,
                    read_schema,
                    format_name,
                    context,
                    _FULL_SCHEMA_INFERENCE_IO_CONCURRENCY,
                )
                pending[ref] = start
                start = stop
            ready, _ = ray.wait(
                list(pending),
                num_returns=1,
                timeout=(
                    _FULL_SCHEMA_INFERENCE_FIRST_RESULT_TIMEOUT_S
                    if first_result
                    else None
                ),
            )
            if not ready:
                logger.warning(
                    "Timed out waiting for the first distributed %s schema read; "
                    "falling back to driver schema reads.",
                    format_name,
                )
                return None
            first_result = False
            ref = ready[0]
            offset = pending.pop(ref)
            for index, value, parquet_encoded in ray.get(ref):
                position = offset + index
                schemas.append((position, _decode_schema(value, parquet_encoded)))
        schemas.sort(key=lambda item: item[0])
        return _merge_file_schemas(
            [paths[index] for index, _ in schemas],
            [schema for _, schema in schemas],
            format_name,
        )
    finally:
        for ref in pending:
            try:
                ray.cancel(ref, force=False)
            except Exception:
                logger.debug("Could not cancel schema task", exc_info=True)
        pending.clear()


def _driver_read_all_metadata(
    paths: List[str],
    sizes: Optional[List[Optional[int]]],
    filesystem: FileSystem,
    format_name: str,
    budget: int,
    task_budget: int,
    mtimes: Optional[List[Optional[int]]],
) -> Optional[FullSchemaInferenceResult]:
    context = DataContext.get_current()
    batch_size = _FULL_SCHEMA_INFERENCE_BATCH_SIZE
    row_slots = min(batch_size, task_budget // _FILE_METADATA_RECORD_BYTES)
    full_batches, tail = divmod(len(paths), batch_size)
    row_limit = min(
        budget // _FILE_METADATA_RECORD_BYTES,
        full_batches * row_slots + min(tail, row_slots),
    )
    row_remaining = row_limit
    footer_available = (
        budget - row_limit * _FILE_METADATA_RECORD_BYTES
        if format_name == "Parquet"
        else 0
    )
    schemas: List[Tuple[int, pa.Schema]] = []
    metadata_chunks: Dict[int, pa.Array] = {}
    for start in range(0, len(paths), batch_size):
        stop = min(start + batch_size, len(paths))
        row_records = min(row_slots, stop - start, row_remaining)
        row_remaining -= row_records
        credit = (
            min(
                footer_available,
                task_budget - row_records * _FILE_METADATA_RECORD_BYTES,
            )
            if row_records
            else 0
        )
        footer_available -= credit
        result = _read_schema_metadata_batch(
            paths[start:stop],
            sizes[start:stop] if sizes is not None else None,
            filesystem,
            format_name,
            context,
            row_records,
            credit,
            mtimes[start:stop] if mtimes is not None else None,
            io_concurrency=16,
        )
        footer_available += credit - result.footer_bytes
        metadata_chunks[start] = (
            result.metadata
            if result.metadata is not None
            else pa.nulls(stop - start, type=FILE_METADATA_TYPE)
        )
        for first_pos, value, parquet_encoded in result.schemas:
            schemas.append((start + first_pos, _decode_schema(value, parquet_encoded)))
    schemas.sort(key=lambda item: item[0])
    return FullSchemaInferenceResult(
        _merge_file_schemas(
            [paths[index] for index, _ in schemas],
            [schema for _, schema in schemas],
            format_name,
        ),
        (
            pa.chunked_array(
                [metadata_chunks[offset] for offset in sorted(metadata_chunks)],
                type=FILE_METADATA_TYPE,
            )
            if row_limit
            else None
        ),
    )


def infer_full_schema_with_metadata_from_files(
    sample: FileManifest,
    filesystem: FileSystem,
    *,
    format_name: str,
    ray_remote_args: Dict[str, Any],
    supports_distributed_reads: bool,
) -> Optional[FullSchemaInferenceResult]:
    """Read every schema once and optionally retain budgeted file metadata.

    Schemas are deduplicated per batch; metadata remains file-specific.
    Scheduling follows the schema-only path: the first batch is a
    single file so progress lands early, and the first distributed result has
    a timeout after which this function cancels pending tasks and finishes
    with a driver-side read, so the caller is not pushed into a second
    distributed planning round.

    Returns None only when no metadata can be retained for the caller's
    candidate set -- the caller then falls back to its existing schema-only
    planning.
    """
    context = DataContext.get_current()
    budget = _FULL_SCHEMA_INFERENCE_METADATA_BUDGET_BYTES
    task_budget = _FULL_SCHEMA_INFERENCE_TASK_METADATA_BUDGET_BYTES
    if min(budget, task_budget) < _FILE_METADATA_RECORD_BYTES:
        return None
    paths: List[str] = sample.paths.tolist()
    if not paths:
        return FullSchemaInferenceResult(pa.schema([]))
    sizes = sample.known_file_sizes
    block = sample.as_block()
    mtimes = (
        block[FILE_MTIME_COLUMN_NAME].to_pylist()
        if isinstance(block, pa.Table) and FILE_MTIME_COLUMN_NAME in block.column_names
        else None
    )
    options = _schema_task_options(
        merge_label_selector(ray_remote_args, context.execution_options.label_selector),
        _FULL_SCHEMA_INFERENCE_NUM_CPUS,
    )
    batch_size = _FULL_SCHEMA_INFERENCE_BATCH_SIZE
    distributed = (
        supports_distributed_reads
        and options is not None
        and len(paths) > _FULL_SCHEMA_INFERENCE_MIN_FILES
    )
    window = (
        min(
            _FULL_SCHEMA_INFERENCE_MAX_TASKS,
            max(
                1,
                math.ceil(
                    ray.cluster_resources().get("CPU", 1)
                    / _FULL_SCHEMA_INFERENCE_NUM_CPUS
                ),
            ),
        )
        if distributed
        else 1
    )
    distributed = distributed and window * min(
        _FULL_SCHEMA_INFERENCE_IO_CONCURRENCY, batch_size
    ) >= min(16, batch_size)
    if not distributed:
        return _driver_read_all_metadata(
            paths, sizes, filesystem, format_name, budget, task_budget, mtimes
        )
    read_remote = cached_remote_fn(_read_schema_metadata_batch, max_retries=2).options(
        num_cpus=_FULL_SCHEMA_INFERENCE_NUM_CPUS, **options
    )
    # Reserve count records first, respecting the task cap even for large batches.
    row_slots = min(batch_size, task_budget // _FILE_METADATA_RECORD_BYTES)
    full_batches, tail = divmod(len(paths) - 1, batch_size)
    row_limit = min(
        budget // _FILE_METADATA_RECORD_BYTES,
        1 + full_batches * row_slots + min(tail, row_slots),
    )
    row_remaining = row_limit
    footer_available = (
        budget - row_limit * _FILE_METADATA_RECORD_BYTES
        if format_name == "Parquet"
        else 0
    )
    schemas: List[Tuple[int, pa.Schema]] = []
    metadata_chunks: Dict[int, pa.Array] = {}
    pending: Dict[ObjectRef[Any], Tuple[int, int, int]] = {}

    def consume(
        result: _SchemaBatchResult,
        offset: int,
        count: int,
        credit: int,
    ) -> None:
        nonlocal footer_available
        footer_available += credit - result.footer_bytes
        metadata_chunks[offset] = (
            result.metadata
            if result.metadata is not None
            else pa.nulls(count, type=FILE_METADATA_TYPE)
        )
        for first_pos, value, parquet_encoded in result.schemas:
            schemas.append((offset + first_pos, _decode_schema(value, parquet_encoded)))

    start = 0
    first_result = True
    timed_out = False
    try:
        while start < len(paths) or pending:
            while start < len(paths) and len(pending) < window:
                # Let the first task report progress without reading a full batch.
                stop = min(start + (1 if start == 0 else batch_size), len(paths))
                row_records = min(row_slots, stop - start, row_remaining)
                row_remaining -= row_records
                credit = (
                    min(
                        footer_available,
                        task_budget - row_records * _FILE_METADATA_RECORD_BYTES,
                    )
                    if row_records
                    else 0
                )
                footer_available -= credit
                ref = read_remote.remote(
                    paths[start:stop],
                    sizes[start:stop] if sizes is not None else None,
                    filesystem,
                    format_name,
                    context,
                    row_records,
                    credit,
                    mtimes[start:stop] if mtimes is not None else None,
                    io_concurrency=_FULL_SCHEMA_INFERENCE_IO_CONCURRENCY,
                )
                pending[ref] = (start, stop - start, credit)
                start = stop
            if not pending:
                break
            ready, _ = ray.wait(
                list(pending),
                num_returns=1,
                timeout=(
                    _FULL_SCHEMA_INFERENCE_FIRST_RESULT_TIMEOUT_S
                    if first_result
                    else None
                ),
            )
            if not ready:
                logger.warning(
                    "Timed out waiting for the first distributed %s schema read; "
                    "falling back to driver schema reads.",
                    format_name,
                )
                timed_out = True
                break
            first_result = False
            ref = ready[0]
            offset, count, credit = pending.pop(ref)
            consume(ray.get(ref), offset, count, credit)
    finally:
        for ref in pending:
            try:
                ray.cancel(ref, force=False)
            except Exception:
                logger.debug("Could not cancel schema task", exc_info=True)
        pending.clear()
    if timed_out:
        return _driver_read_all_metadata(
            paths, sizes, filesystem, format_name, budget, task_budget, mtimes
        )
    schemas.sort(key=lambda item: item[0])
    return FullSchemaInferenceResult(
        _merge_file_schemas(
            [paths[index] for index, _ in schemas],
            [schema for _, schema in schemas],
            format_name,
        ),
        (
            pa.chunked_array(
                [metadata_chunks[offset] for offset in sorted(metadata_chunks)],
                type=FILE_METADATA_TYPE,
            )
            if row_limit
            else None
        ),
    )


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
