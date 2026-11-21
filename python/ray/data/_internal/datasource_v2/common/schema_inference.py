"""Ordered, bounded schema and metadata discovery shared by file datasources."""

import logging
import math
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from functools import partial
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
from ray.data._internal.util import unify_schemas_with_validation
from ray.data.context import DataContext
from ray.util.scheduling_strategies import (
    NodeAffinitySchedulingStrategy,
    PlacementGroupSchedulingStrategy,
)

_FULL_SCHEMA_INFERENCE_WARNING_FILE_COUNT = 10_000

# Bound submitted reads, not just executing threads, for full inference.
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
# Estimated row-count metadata budgets, internal rather than public config.
# Zero disables caching. Index scaffolding, schemas and temporary copies are
# excluded from these budgets.
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


def _add_unique_schema(
    unique: Dict[bytes, Tuple[int, bytes, bool]],
    index: int,
    schema: pa.Schema,
    value: bytes,
    read_original_metadata: Optional[Callable[[], bytes]] = None,
) -> None:
    if value in unique:
        return
    decoded = pa.ipc.read_schema(pa.BufferReader(value))
    if schema.equals(decoded, check_metadata=True):
        unique[value] = (index, value, False)
        return
    import pyarrow.parquet as pq

    try:
        output = pa.BufferOutputStream()
        pq.write_metadata(schema, output)
        encoded = output.getvalue().to_pybytes()
        restored = pq.read_schema(pa.BufferReader(encoded))
        if not schema.equals(restored, check_metadata=True):
            raise pa.ArrowInvalid(
                "Unable to preserve schema through IPC or Parquet metadata"
            )
    except (pa.ArrowInvalid, pa.ArrowTypeError, pa.ArrowNotImplementedError):
        if read_original_metadata is None:
            raise
        encoded = read_original_metadata()
        restored = pq.read_schema(pa.BufferReader(encoded))
        if not schema.equals(restored, check_metadata=True):
            raise pa.ArrowInvalid(
                "Unable to preserve schema through original Parquet metadata"
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
    read_schema_metadata: Optional[Callable] = None,
) -> List[Tuple[int, bytes, bool]]:
    """Read distinct schemas and encode them without losing metadata.

    Use IPC when lossless; otherwise try generated or original Parquet metadata.
    """
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
            read_original = (
                partial(read_schema_metadata, paths[index], filesystem, sizes[index])
                if read_schema_metadata is not None
                else None
            )
            _add_unique_schema(unique, index, schema, value, read_original)
    return list(unique.values())


def _read_schema_metadata_batch(
    paths: List[str],
    sizes: Optional[List[Optional[int]]],
    filesystem: FileSystem,
    format_name: str,
    context: DataContext,
    row_records: int = 0,
    mtimes: Optional[List[Optional[int]]] = None,
    *,
    read_metadata: Callable,
    io_concurrency: Optional[int] = None,
) -> _SchemaBatchResult:
    """Read schemas and bounded row counts from the same Parquet footers.

    Schemas are deduplicated; row counts stay aligned with the input files.
    """
    DataContext._set_current(context)
    if format_name != "Parquet":
        raise ValueError(f"Unsupported schema format: {format_name}")

    def read_file(item: Tuple[int, str]):
        index, path = item
        size = sizes[index] if sizes is not None else None
        metadata: Optional[FileMetadata] = None
        try:
            parquet_metadata = read_metadata(
                path, filesystem, size, retried_io_errors=context.retried_io_errors
            )
            # This is pq.read_schema's conversion, not schema_arrow.
            schema = parquet_metadata.schema.to_arrow_schema()
            if index < row_records:
                metadata = FileMetadata(
                    num_rows=parquet_metadata.num_rows,
                    mtime_ns=mtimes[index] if mtimes is not None else None,
                )
            value = schema.serialize().to_pybytes()
            return schema, value, metadata, parquet_metadata
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
        for index, (schema, value, file_metadata, parquet_metadata) in enumerate(
            executor.map(read_file, enumerate(paths))
        ):
            metadata_values.append(file_metadata)

            def read_original_metadata() -> bytes:
                output = pa.BufferOutputStream()
                parquet_metadata.write_metadata_file(output)
                return output.getvalue().to_pybytes()

            _add_unique_schema(unique, index, schema, value, read_original_metadata)
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
    return _SchemaBatchResult(list(unique.values()), metadata)


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
    read_schema_metadata: Optional[Callable] = None,
) -> Optional[pa.Schema]:
    """Read schemas on workers and merge once in candidate order on the driver.

    Return None when the caller should use its existing driver inference.
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
                    read_schema_metadata,
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
    *,
    read_metadata: Callable,
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
    schemas: List[Tuple[int, pa.Schema]] = []
    metadata_chunks: Dict[int, pa.Array] = {}
    for start in range(0, len(paths), batch_size):
        stop = min(start + batch_size, len(paths))
        row_records = min(row_slots, stop - start, row_remaining)
        row_remaining -= row_records
        result = _read_schema_metadata_batch(
            paths[start:stop],
            sizes[start:stop] if sizes is not None else None,
            filesystem,
            format_name,
            context,
            row_records,
            mtimes[start:stop] if mtimes is not None else None,
            read_metadata=read_metadata,
            io_concurrency=16,
        )
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
    read_metadata: Callable,
    ray_remote_args: Dict[str, Any],
    supports_distributed_reads: bool,
) -> Optional[FullSchemaInferenceResult]:
    """Read schemas and retain row counts within the metadata budget.

    Return None when caching is disabled. A first-result timeout cancels
    pending tasks and falls back directly to driver reads.
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
            paths,
            sizes,
            filesystem,
            format_name,
            budget,
            task_budget,
            mtimes,
            read_metadata=read_metadata,
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
    schemas: List[Tuple[int, pa.Schema]] = []
    metadata_chunks: Dict[int, pa.Array] = {}
    pending: Dict[ObjectRef[Any], Tuple[int, int]] = {}

    def consume(
        result: _SchemaBatchResult,
        offset: int,
        count: int,
    ) -> None:
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
                ref = read_remote.remote(
                    paths[start:stop],
                    sizes[start:stop] if sizes is not None else None,
                    filesystem,
                    format_name,
                    context,
                    row_records,
                    mtimes[start:stop] if mtimes is not None else None,
                    read_metadata=read_metadata,
                    io_concurrency=_FULL_SCHEMA_INFERENCE_IO_CONCURRENCY,
                )
                pending[ref] = (start, stop - start)
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
            offset, count = pending.pop(ref)
            consume(ray.get(ref), offset, count)
    finally:
        for ref in pending:
            try:
                ray.cancel(ref, force=False)
            except Exception:
                logger.debug("Could not cancel schema task", exc_info=True)
        pending.clear()
    if timed_out:
        return _driver_read_all_metadata(
            paths,
            sizes,
            filesystem,
            format_name,
            budget,
            task_budget,
            mtimes,
            read_metadata=read_metadata,
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
