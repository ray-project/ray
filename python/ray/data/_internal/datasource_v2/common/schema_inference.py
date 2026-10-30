"""Ordered, bounded schema discovery shared by file datasources."""

import logging
import math
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Callable, Dict, List, Optional, cast

import pyarrow as pa
from pyarrow.fs import FileSystem

import ray
from ray import ObjectRef
from ray.data._internal.datasource_v2.interfaces.file_manifest import FileManifest
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


def _read_schema_batch(
    paths: List[str], filesystem: FileSystem, format_name: str, context: DataContext
) -> List[bytes]:
    DataContext._set_current(context)
    read_schema: Callable[[Any], pa.Schema]
    if format_name == "Parquet":
        import pyarrow.parquet as pq

        read_schema = pq.read_schema
    elif format_name == "ORC":
        import pyarrow.orc as orc

        def read_orc_schema(source: Any) -> pa.Schema:
            return orc.ORCFile(source).schema

        read_schema = read_orc_schema
    else:
        raise ValueError(f"Unsupported schema format: {format_name}")

    def read_file(path: str) -> bytes:
        try:
            with filesystem.open_input_file(path) as source:
                return read_schema(source).serialize().to_pybytes()
        except (OSError, pa.ArrowInvalid, pa.ArrowTypeError) as error:
            raise type(error)(
                f"Failed to read {format_name} schema from {path!r}: {error}"
            ) from error

    with ThreadPoolExecutor(
        max_workers=min(context.full_schema_inference_io_concurrency, len(paths))
    ) as executor:
        return list(executor.map(read_file, paths))


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
    """Read schemas on workers and merge once in candidate order on the driver.

    Return None when the caller should use its existing driver inference.
    The task window bounds submitted batches. Consecutive identical IPC payloads
    share a decoded schema within this call; candidate references remain O(files).
    """
    context = DataContext.get_current()
    _validate_full_schema_config(context)
    paths: List[str] = sample.paths.tolist()
    if not paths:
        return pa.schema([])
    if (
        not supports_distributed_reads
        or len(paths) <= context.full_schema_inference_min_files
    ):
        return None
    options = _schema_task_options(
        merge_label_selector(ray_remote_args, context.execution_options.label_selector),
        context.full_schema_inference_num_cpus,
    )
    if options is None:
        return None
    batch_size = context.full_schema_inference_batch_size
    window = min(
        context.full_schema_inference_max_tasks,
        max(
            1,
            math.ceil(
                ray.cluster_resources().get("CPU", 1)
                / context.full_schema_inference_num_cpus
            ),
        ),
    )
    if window * min(context.full_schema_inference_io_concurrency, batch_size) < min(
        16, batch_size
    ):
        return None
    read_remote = cached_remote_fn(_read_schema_batch, max_retries=2).options(
        num_cpus=context.full_schema_inference_num_cpus, **options
    )
    schemas: List[Optional[pa.Schema]] = [None] * len(paths)
    pending: Dict[ObjectRef[List[bytes]], int] = {}
    last_payload: Optional[bytes] = None
    last_schema: Optional[pa.Schema] = None
    start = 0
    try:
        while start < len(paths) or pending:
            while start < len(paths) and len(pending) < window:
                stop = min(start + batch_size, len(paths))
                ref = read_remote.remote(
                    paths[start:stop], filesystem, format_name, context
                )
                pending[ref] = start
                start = stop
            ready, _ = ray.wait(list(pending), num_returns=1)
            ref = ready[0]
            offset = pending.pop(ref)
            for index, value in enumerate(ray.get(ref)):
                if value != last_payload:
                    last_schema = pa.ipc.read_schema(pa.BufferReader(value))
                    last_payload = value
                assert last_schema is not None
                schemas[offset + index] = last_schema
        assert all(schema is not None for schema in schemas)
        return _merge_file_schemas(paths, cast(List[pa.Schema], schemas), format_name)
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
