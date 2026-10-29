"""Ordered, bounded schema discovery shared by file datasources."""

import logging
import math
from array import array
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

import pyarrow as pa
from pyarrow.fs import FileSystem

import ray
from ray import ObjectRef
from ray.data._internal.datasource_v2.interfaces.file_indexer import FileInfo
from ray.data._internal.datasource_v2.interfaces.file_manifest import FileManifest
from ray.data._internal.execution.util import merge_label_selector
from ray.data._internal.remote_fn import cached_remote_fn
from ray.data._internal.util import unify_schemas_with_validation
from ray.data.context import DataContext
from ray.util.scheduling_strategies import (
    NodeAffinitySchedulingStrategy,
    PlacementGroupSchedulingStrategy,
)

# Bound submitted reads, not just executing threads, for full inference.
_SCHEMA_READ_BATCH_SIZE = 256

logger = logging.getLogger(__name__)


@dataclass
class _EncodedSchemas:
    schemas: List[bytes]
    indices: array

    def decode(self) -> List[pa.Schema]:
        schemas = [pa.ipc.read_schema(pa.BufferReader(value)) for value in self.schemas]
        return [schemas[index] for index in self.indices]


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
    context: DataContext,
    *,
    io_concurrency: int,
) -> Tuple[Optional[bytes], _EncodedSchemas]:
    DataContext._set_current(context)

    def read_file(file: FileInfo) -> pa.Schema:
        try:
            with filesystem.open_input_file(file.path) as source:
                if format_name == "Parquet":
                    import pyarrow.parquet as pq

                    reader = pq.ParquetFile(source)
                    # This is pq.read_schema's conversion, not schema_arrow.
                    return reader.schema.to_arrow_schema()
                elif format_name == "ORC":
                    import pyarrow.orc as orc

                    return orc.ORCFile(source).schema
                else:
                    raise ValueError(f"Unsupported schema format: {format_name}")
        except (OSError, pa.ArrowInvalid, pa.ArrowTypeError) as error:
            raise type(error)(
                f"Failed to read {format_name} schema from {file.path!r}: {error}"
            ) from error

    with ThreadPoolExecutor(max_workers=min(io_concurrency, len(files))) as executor:
        schemas = list(executor.map(read_file, files))
    return _encode_schemas(schemas)


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
) -> pa.Schema:
    """Read all candidates, using safe partials or exact ordered schema replay.

    The task window bounds unfinished reads. Completed results and lossless
    schemas can grow with the number of batches until the final merge.
    """
    context = DataContext.get_current()
    _validate_full_schema_config(context)
    if not len(sample):
        return pa.schema([])
    paths = sample.paths
    sizes = sample.file_sizes
    num_cpus = context.full_schema_inference_num_cpus
    read_remote_args = merge_label_selector(
        ray_remote_args, context.execution_options.label_selector
    )
    options = _schema_task_options(read_remote_args, num_cpus)
    batch_size = context.full_schema_inference_batch_size
    io_concurrency = context.full_schema_inference_io_concurrency
    distributed = (
        supports_distributed_reads
        and options is not None
        and len(sample) > context.full_schema_inference_min_files
    )
    window = (
        min(
            context.full_schema_inference_max_tasks,
            max(1, math.ceil(ray.cluster_resources().get("CPU", 1) / num_cpus)),
        )
        if distributed
        else 1
    )
    # Keep at least the driver's 16-way I/O when selecting the worker path.
    distributed = distributed and window * min(io_concurrency, batch_size) >= min(
        16, batch_size
    )
    if not distributed:
        window = 1
    read_remote = (
        cached_remote_fn(
            _read_schema_batch,
            num_cpus=num_cpus,
            max_retries=2,
            num_returns=2,
        ).options(**(options or {}))
        if distributed
        else None
    )
    pending = deque()
    inflight = set()
    ready = set()
    originals: List[Union[ObjectRef[_EncodedSchemas], _EncodedSchemas]] = []
    merged = None
    eligible = True
    succeeded = False
    start = 0
    try:
        while start < len(sample) or pending:
            while (
                start < len(sample)
                and (len(inflight) if read_remote is not None else len(pending))
                < window
            ):
                stop = min(start + batch_size, len(sample))
                files = [
                    FileInfo(str(paths[i]), int(sizes[i])) for i in range(start, stop)
                ]
                args = (files, filesystem, format_name, context)
                if read_remote is not None:
                    partial, original = read_remote.remote(
                        *args, io_concurrency=io_concurrency
                    )
                    inflight.add(partial)
                else:
                    partial, original = _read_schema_batch(*args, io_concurrency=16)
                pending.append(partial)
                originals.append(original)
                start = stop
            if read_remote is not None and inflight:
                completed, _ = ray.wait(list(inflight), num_returns=1)
                inflight.difference_update(completed)
                ready.update(completed)
            # Completion frees submission slots; consume only the ready prefix
            # so field order and metadata still follow candidate order.
            while pending:
                partial = pending[0]
                if read_remote is not None:
                    if partial not in ready:
                        break
                    partial_ref = partial
                    partial = ray.get(partial_ref)
                    ready.remove(partial_ref)
                pending.popleft()
                if eligible:
                    if partial is None:
                        eligible = False
                    else:
                        schema = pa.ipc.read_schema(pa.BufferReader(partial))
                        merged = _merge_same_types(merged, schema)
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
        succeeded = True
        return merged
    finally:
        if not succeeded and read_remote is not None:
            for partial in pending:
                try:
                    ray.cancel(partial, force=False)
                except Exception:
                    logger.debug("Could not cancel schema task", exc_info=True)
        pending.clear()
        originals.clear()
        inflight.clear()
        ready.clear()


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
