"""Ordered, bounded schema discovery shared by file datasources."""

import logging
import math
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from threading import Lock
from typing import Any, Callable, Dict, List, Optional, Tuple, Union, cast

import pyarrow as pa
from pyarrow.fs import FileSystem

import ray
from ray import ObjectRef
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

_SCHEMA_READ_BATCH_SIZE = 256
logger = logging.getLogger(__name__)
_FILE_METADATA_RECORD_BYTES = 32


@dataclass
class FullSchemaInferenceResult:
    """Schema and optional ordinary Arrow values for the same candidates."""

    schema: pa.Schema
    metadata: Optional[pa.ChunkedArray] = None


@dataclass
class _SchemaBatchResult:
    schemas: List[bytes]
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


def _read_schema_batch(
    paths: List[str], filesystem: FileSystem, format_name: str, context: DataContext
) -> List[bytes]:
    return _read_schema_metadata_batch(paths, filesystem, format_name, context).schemas


def _read_schema_metadata_batch(
    paths: List[str],
    filesystem: FileSystem,
    format_name: str,
    context: DataContext,
    row_records: int = 0,
    footer_credit: int = 0,
    mtimes: Optional[List[Optional[int]]] = None,
    *,
    io_concurrency: Optional[int] = None,
) -> _SchemaBatchResult:
    DataContext._set_current(context)
    if format_name not in {"Parquet", "ORC"}:
        raise ValueError(f"Unsupported schema format: {format_name}")
    budget = _FooterBudget(footer_credit)

    def read_file(item: Tuple[int, str]):
        index, path = item
        metadata: Optional[FileMetadata] = None
        try:
            with filesystem.open_input_file(path) as source:
                if format_name == "Parquet":
                    import pyarrow.parquet as pq

                    parquet_reader = pq.ParquetFile(source)
                    # This is pq.read_schema's conversion, not schema_arrow.
                    schema = parquet_reader.schema.to_arrow_schema()
                    if index < row_records:
                        metadata = FileMetadata(
                            parquet_reader.metadata.num_rows,
                            budget.encode(parquet_reader.metadata)
                            if footer_credit
                            else None,
                            mtimes[index] if mtimes is not None else None,
                        )
                else:
                    import pyarrow.orc as orc

                    orc_reader = orc.ORCFile(source)
                    schema = orc_reader.schema
                    if index < row_records:
                        metadata = FileMetadata(
                            num_rows=orc_reader.nrows,
                            mtime_ns=mtimes[index] if mtimes is not None else None,
                        )
                return schema.serialize().to_pybytes(), metadata
        except (OSError, pa.ArrowInvalid, pa.ArrowTypeError) as error:
            raise type(error)(
                f"Failed to read {format_name} schema from {path!r}: {error}"
            ) from error

    concurrency = (
        io_concurrency
        if io_concurrency is not None
        else context.full_schema_inference_io_concurrency
    )
    with ThreadPoolExecutor(max_workers=min(concurrency, len(paths))) as executor:
        results = list(executor.map(read_file, enumerate(paths)))
    metadata = (
        pa.array(
            [
                value.to_metadata() if value is not None else None
                for _, value in results
            ],
            type=FILE_METADATA_TYPE,
        )
        if row_records
        else None
    )
    return _SchemaBatchResult(
        [schema for schema, _ in results], metadata, budget.limit - budget.remaining
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


def _validate_full_schema_config(context: DataContext) -> None:
    for suffix, minimum in (
        ("min_files", 0),
        ("max_tasks", 1),
        ("batch_size", 1),
        ("io_concurrency", 1),
    ):
        name = f"full_schema_inference_{suffix}"
        value = getattr(context, name)
        if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
            raise ValueError(f"{name} must be an integer >= {minimum}, got {value!r}")
    num_cpus = context.full_schema_inference_num_cpus
    if (
        isinstance(num_cpus, bool)
        or not isinstance(num_cpus, (int, float))
        or not math.isfinite(num_cpus)
        or num_cpus <= 0
    ):
        raise ValueError("full_schema_inference_num_cpus must be finite and positive")


def infer_full_schema_from_files(
    sample: FileManifest,
    filesystem: FileSystem,
    *,
    format_name: str,
    ray_remote_args: Dict[str, Any],
    supports_distributed_reads: bool,
) -> Optional[pa.Schema]:
    """Keep the schema-only planning contract, including its driver fallback."""
    result = infer_full_schema_with_metadata_from_files(
        sample,
        filesystem,
        format_name=format_name,
        ray_remote_args=ray_remote_args,
        supports_distributed_reads=supports_distributed_reads,
        reuse_metadata=False,
    )
    return result.schema if result is not None else None


def infer_full_schema_with_metadata_from_files(
    sample: FileManifest,
    filesystem: FileSystem,
    *,
    format_name: str,
    ray_remote_args: Dict[str, Any],
    supports_distributed_reads: bool,
    reuse_metadata: bool = True,
) -> Optional[FullSchemaInferenceResult]:
    """Read every schema once and optionally retain budgeted file metadata.

    Consecutive identical IPC payloads share a decoded schema within this call;
    metadata remains file-specific and candidate references remain O(files).
    """
    context = DataContext.get_current()
    _validate_full_schema_config(context)
    budget = task_budget = 0
    if reuse_metadata:
        for suffix in ("metadata_budget_bytes", "task_metadata_budget_bytes"):
            name = f"full_schema_inference_{suffix}"
            value = getattr(context, name)
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise ValueError(f"{name} must be a nonnegative integer")
        budget = context.full_schema_inference_metadata_budget_bytes
        task_budget = context.full_schema_inference_task_metadata_budget_bytes
        if min(budget, task_budget) < _FILE_METADATA_RECORD_BYTES:
            return None
    paths: List[str] = sample.paths.tolist()
    if not paths:
        return FullSchemaInferenceResult(pa.schema([]))
    options = _schema_task_options(
        merge_label_selector(ray_remote_args, context.execution_options.label_selector),
        context.full_schema_inference_num_cpus,
    )
    batch_size = context.full_schema_inference_batch_size
    distributed = (
        supports_distributed_reads
        and options is not None
        and len(paths) > context.full_schema_inference_min_files
    )
    window = (
        min(
            context.full_schema_inference_max_tasks,
            max(
                1,
                math.ceil(
                    ray.cluster_resources().get("CPU", 1)
                    / context.full_schema_inference_num_cpus
                ),
            ),
        )
        if distributed
        else 1
    )
    distributed = distributed and window * min(
        context.full_schema_inference_io_concurrency, batch_size
    ) >= min(16, batch_size)
    if not distributed and not reuse_metadata:
        return None
    if not distributed:
        window = 1
    read_batch = _read_schema_metadata_batch if reuse_metadata else _read_schema_batch
    read_remote = (
        cached_remote_fn(read_batch, max_retries=2).options(
            num_cpus=context.full_schema_inference_num_cpus, **(options or {})
        )
        if distributed
        else None
    )
    # Reserve count records first, respecting the task cap even for large batches.
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
    block = sample.as_block()
    mtimes = (
        block[FILE_MTIME_COLUMN_NAME].to_pylist()
        if reuse_metadata
        and isinstance(block, pa.Table)
        and FILE_MTIME_COLUMN_NAME in block.column_names
        else None
    )
    schemas: List[Optional[pa.Schema]] = [None] * len(paths)
    metadata_chunks: Dict[int, pa.Array] = {}
    pending: Dict[ObjectRef[Any], Tuple[int, int, int]] = {}
    last_payload: Optional[bytes] = None
    last_schema: Optional[pa.Schema] = None

    def consume(
        result: Union[List[bytes], _SchemaBatchResult],
        offset: int,
        count: int,
        credit: int,
    ) -> None:
        nonlocal footer_available, last_payload, last_schema
        if isinstance(result, _SchemaBatchResult):
            footer_available += credit - result.footer_bytes
            metadata_chunks[offset] = (
                result.metadata
                if result.metadata is not None
                else pa.nulls(count, type=FILE_METADATA_TYPE)
            )
            values = result.schemas
        else:
            values = result
        for index, value in enumerate(values):
            if value != last_payload:
                last_schema = pa.ipc.read_schema(pa.BufferReader(value))
                last_payload = value
            assert last_schema is not None
            schemas[offset + index] = last_schema

    start = 0
    try:
        while start < len(paths) or pending:
            while start < len(paths) and len(pending) < window:
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
                args: Tuple[Any, ...] = (
                    paths[start:stop],
                    filesystem,
                    format_name,
                    context,
                )
                if reuse_metadata:
                    args += (
                        row_records,
                        credit,
                        mtimes[start:stop] if mtimes is not None else None,
                    )
                if read_remote is not None:
                    ref = read_remote.remote(*args)
                    pending[ref] = (start, stop - start, credit)
                else:
                    consume(
                        _read_schema_metadata_batch(
                            paths[start:stop],
                            filesystem,
                            format_name,
                            context,
                            row_records,
                            credit,
                            mtimes[start:stop] if mtimes is not None else None,
                            io_concurrency=16,
                        ),
                        start,
                        stop - start,
                        credit,
                    )
                start = stop
            if not pending:
                break
            ready, _ = ray.wait(list(pending), num_returns=1)
            ref = ready[0]
            offset, count, credit = pending.pop(ref)
            consume(ray.get(ref), offset, count, credit)
        assert all(schema is not None for schema in schemas)
        return FullSchemaInferenceResult(
            _merge_file_schemas(paths, cast(List[pa.Schema], schemas), format_name),
            pa.chunked_array(
                [metadata_chunks[offset] for offset in sorted(metadata_chunks)],
                type=FILE_METADATA_TYPE,
            )
            if row_limit
            else None,
        )
    finally:
        for ref in pending:
            try:
                ray.cancel(ref, force=False)
            except Exception:
                logger.debug("Could not cancel schema task", exc_info=True)
        pending.clear()


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
