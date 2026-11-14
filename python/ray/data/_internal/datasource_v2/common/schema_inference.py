"""Ordered, bounded schema discovery shared by file datasources."""

import logging
import math
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Callable, Dict, List, Optional, Tuple

import pyarrow as pa
from pyarrow.fs import FileSystem

import ray
from ray import ObjectRef
from ray._common.utils import env_float, env_integer
from ray.data._internal.datasource_v2.interfaces.file_manifest import FileManifest
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
logger = logging.getLogger(__name__)


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
            if value not in unique:
                decoded = pa.ipc.read_schema(pa.BufferReader(value))
                if schema.equals(decoded, check_metadata=True):
                    unique[value] = (index, value, False)
                else:
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
    return list(unique.values())


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
    The driver restores candidate order before the one-shot merge.
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
                if parquet_encoded:
                    import pyarrow.parquet as pq

                    schema = pq.read_schema(pa.BufferReader(value))
                else:
                    schema = pa.ipc.read_schema(pa.BufferReader(value))
                schemas.append((position, schema))
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
