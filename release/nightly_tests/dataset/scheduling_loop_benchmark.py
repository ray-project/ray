"""Benchmark for the Ray Data scheduling loop under a production-shape parquet workload.

Reproduces the scheduling-loop bottleneck observed in a production ML feature
transform: ``read_parquet`` -> ``map_batches`` over a wide mixed schema
(scalar + variable-length list columns), with a high task ``concurrency`` and
``batch_format="pyarrow"``.

Workload profile (defaults mirror production):

  - Schema: 140 columns (80 scalar float32, 20 scalar int64,
            40 list<float32> with inner sizes 32/64/128/200)
  - ``override_num_blocks`` = ``--num-files``
  - ``map_batches(batch_size=1000, concurrency=400, num_cpus=1,
    batch_format="pyarrow", zero_copy_batch=True)``

The input parquet files are generated (untimed) into a per-run S3 prefix so
every node in the cluster can read them; only the read + map pipeline is
timed. Key metric to compare across wheels is the map operator's
``average_task_scheduling_time_s`` (lower is better).

NOTE: the V2 parquet reader bin-packs files into ~128 MiB read units and only
treats ``override_num_blocks`` as a hint, so run with
``RAY_DATA_PARQUET_BIN_PACKING_BYTES=1`` (as the release test does) to get
one read task per file like production.

Profiling is gated by env vars consumed by ``profiling.coordinator.Profiling``
(``PYSPY_ENABLED=1``, ``PERF_PROFILING_ENABLED=1`` etc.). When none are set,
the coordinator is a no-op aside from printing its configuration.
"""

import argparse
import os
import uuid
from typing import Any, Dict, List, Optional

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import ray
from benchmark import (
    Benchmark,
    RuntimeEnvSetupTracker,
    benchmark_py_modules,
    collect_dataset_stats,
)
from profiling.coordinator import Profiling

JOB_ID = os.environ.get("ANYSCALE_JOB_ID", f"local-{uuid.uuid4().hex[:8]}")
# Override for local runs where /mnt/shared_storage doesn't exist.
SHARED_OUTDIR = os.environ.get(
    "PROFILING_OUTDIR", f"/mnt/shared_storage/scheduling_loop/{JOB_ID}"
)
# Per-run prefix so concurrent runs don't collide.
DEFAULT_DATA_PATH = f"s3://ray-data-write-benchmark/scheduling_loop/{uuid.uuid4().hex}"

# Production schema breakdown. Wide schemas with list types make Arrow schema
# (de)serialization dominate the scheduler thread.
NUM_F32_COLS = 80
NUM_I64_COLS = 20
# (inner list size, number of columns)
LIST_COL_SPECS = [(32, 13), (64, 2), (128, 5), (200, 20)]

# Per-operator task metrics surfaced in the result JSON.
TASK_METRIC_KEYS = [
    "num_tasks_finished",
    "task_scheduling_time_s",
    "average_task_scheduling_time_s",
    "average_total_task_completion_time_s",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--num-files",
        type=int,
        default=2000,
        help="Number of parquet files to generate; also used as override_num_blocks.",
    )
    parser.add_argument(
        "--rows-per-file",
        type=int,
        default=1200,
        help="Rows per parquet file (~average rows output per read task in prod).",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=1000,
        help="map_batches batch_size.",
    )
    parser.add_argument(
        "--concurrency",
        type=int,
        default=400,
        help="map_batches concurrency (max in-flight tasks).",
    )
    parser.add_argument(
        "--num-cpus",
        type=float,
        default=1,
        help="num_cpus per map_batches task.",
    )
    parser.add_argument(
        "--memory-per-read-task",
        type=int,
        default=0,
        help=(
            "Memory (bytes) requested per read task. Production used this to "
            "cap read tasks per worker. 0 disables the label."
        ),
    )
    parser.add_argument(
        "--data-path",
        type=str,
        default=DEFAULT_DATA_PATH,
        help="Where to write the generated parquet input (S3 prefix or shared FS).",
    )
    args = parser.parse_args()
    if args.num_files < 1:
        parser.error("--num-files must be >= 1.")
    if args.rows_per_file < 1:
        parser.error("--rows-per-file must be >= 1.")
    if args.concurrency < 1:
        parser.error("--concurrency must be >= 1.")
    return args


def make_schema() -> pa.Schema:
    fields: List[pa.Field] = []
    for i in range(NUM_F32_COLS):
        fields.append(pa.field(f"f32_{i}", pa.float32()))
    for i in range(NUM_I64_COLS):
        fields.append(pa.field(f"i64_{i}", pa.int64()))
    for inner_size, count in LIST_COL_SPECS:
        for i in range(count):
            fields.append(pa.field(f"list_{inner_size}_{i}", pa.list_(pa.float32())))
    return pa.schema(fields)


def make_table(schema: pa.Schema, n_rows: int, seed: int) -> pa.Table:
    rng = np.random.default_rng(seed)
    arrays: List[pa.Array] = []
    for field in schema:
        t = field.type
        if t == pa.float32():
            arrays.append(pa.array(rng.standard_normal(n_rows).astype(np.float32)))
        elif t == pa.int64():
            arrays.append(pa.array(rng.integers(0, 100_000, n_rows, dtype=np.int64)))
        else:
            # "list_32_0" -> 32
            inner_size = int(field.name.split("_")[1])
            offsets = np.arange(
                0, (n_rows + 1) * inner_size, inner_size, dtype=np.int32
            )
            values = rng.standard_normal(n_rows * inner_size).astype(np.float32)
            arrays.append(pa.ListArray.from_arrays(offsets, values))
    return pa.Table.from_arrays(arrays, schema=schema)


def generate_input(args: argparse.Namespace, schema: pa.Schema) -> None:
    """Write ``--num-files`` parquet files to ``--data-path`` in parallel.

    One ``range`` block per file -> one ``map_batches`` task per file -> one
    parquet file per block on write, so the input has exactly ``num_files``
    files and the timed ``read_parquet`` can use ``override_num_blocks`` to
    get one read task per file.
    """
    rows_per_file = args.rows_per_file

    def gen(batch: Dict[str, np.ndarray]) -> pa.Table:
        # ``range`` emits a single "id" row per block; use it as the seed so
        # every file has distinct (but reproducible) content.
        seed = int(batch["id"][0])
        return make_table(schema, rows_per_file, seed)

    (
        ray.data.range(args.num_files, override_num_blocks=args.num_files)
        .map_batches(gen, batch_size=None)
        .write_parquet(args.data_path)
    )


def identity_transform(batch: pa.Table) -> pa.Table:
    """Stand-in for the production transform.

    Reads every column (forcing schema deserialization) and applies a trivial
    float op on the scalar float columns so the work can't be elided. List
    columns pass through untouched; the scheduling-loop bottleneck is schema
    overhead, not compute.
    """
    for i, name in enumerate(batch.column_names):
        col = batch.column(i)
        if pa.types.is_floating(col.type):
            batch = batch.set_column(
                i, name, pc.add(col, pa.scalar(0.0, type=col.type))
            )
    return batch


def _collect_operator_task_metrics(ds: "ray.data.Dataset") -> List[Dict[str, Any]]:
    """Per-operator task scheduling metrics, root-most operator first.

    Each ``DatasetStatsSummary`` in the ``parents`` chain carries the
    ``OpRuntimeMetrics`` dict of one physical operator in ``extra_metrics``.
    """
    chain: List[Dict[str, Any]] = []
    summary = ds.get_stats_summary(detail=True)
    while summary is not None:
        if summary.extra_metrics:
            entry = {"operator_name": summary.base_name}
            for key in TASK_METRIC_KEYS:
                entry[key] = summary.extra_metrics.get(key)
            chain.append(entry)
        summary = summary.parents[0] if summary.parents else None
    chain.reverse()
    return chain


def _find_operator_metric(
    op_metrics: List[Dict[str, Any]], name_substr: str, key: str
) -> Optional[float]:
    for entry in op_metrics:
        if name_substr in (entry["operator_name"] or ""):
            return entry.get(key)
    return None


def main(args: argparse.Namespace) -> None:
    benchmark = Benchmark()
    schema = make_schema()
    print(
        f"Schema: {len(schema)} columns "
        f"({NUM_F32_COLS} scalar-f32, {NUM_I64_COLS} scalar-i64, "
        f"{sum(c for _, c in LIST_COL_SPECS)} list)"
    )

    print(
        f"Generating {args.num_files} parquet files x {args.rows_per_file} rows "
        f"at {args.data_path} ..."
    )
    generate_input(args, schema)

    read_remote_args: Dict[str, Any] = {"num_cpus": 1}
    if args.memory_per_read_task > 0:
        read_remote_args["memory"] = args.memory_per_read_task

    ds_holder = {}

    def benchmark_fn():
        ds = ray.data.read_parquet(
            args.data_path,
            override_num_blocks=args.num_files,
            ray_remote_args=read_remote_args,
        )
        ds = ds.map_batches(
            identity_transform,
            batch_size=args.batch_size,
            zero_copy_batch=True,
            batch_format="pyarrow",
            concurrency=args.concurrency,
            num_cpus=args.num_cpus,
        )
        ds_holder["ds"] = ds.materialize()

    benchmark.run_fn("scheduling_loop", benchmark_fn)

    ds = ds_holder["ds"]
    metrics = collect_dataset_stats(ds)
    op_metrics = _collect_operator_task_metrics(ds)
    metrics["operator_task_metrics"] = op_metrics
    # Headline number: how long map tasks sit between submission and start.
    metrics["map_average_task_scheduling_time_s"] = _find_operator_metric(
        op_metrics, "MapBatches(", "average_task_scheduling_time_s"
    )
    metrics["runtime_env_setup"] = RuntimeEnvSetupTracker.collect()
    metrics["num_files"] = args.num_files
    metrics["rows_per_file"] = args.rows_per_file
    metrics["num_rows"] = ds.count()
    metrics["num_columns"] = len(schema)
    metrics["batch_size"] = args.batch_size
    metrics["concurrency"] = args.concurrency
    metrics["num_cpus"] = args.num_cpus
    benchmark.result["scheduling_loop"].update(metrics)

    print("Per-operator task metrics:")
    for entry in op_metrics:
        print(f"  {entry}")

    benchmark.write_result()


if __name__ == "__main__":
    # ``Profiling.start()`` spawns ``_UDFPySpyProfiler`` actors on worker
    # nodes, which need to import ``profiling.pyspy`` from this script's
    # ``profiling/`` sibling. Ship it alongside ``benchmark.py``.
    import profiling as _profiling_pkg

    _profiling_dir = os.path.dirname(os.path.abspath(_profiling_pkg.__file__))
    ray.init(runtime_env={"py_modules": benchmark_py_modules() + [_profiling_dir]})
    args = parse_args()

    profiling = Profiling(outdir=SHARED_OUTDIR, num_gpu_nodes=0)
    profiling.start(
        extra_config={
            "RAY_COMMIT": ray.__commit__,
            "NUM_FILES": args.num_files,
            "ROWS_PER_FILE": args.rows_per_file,
            "BATCH_SIZE": args.batch_size,
            "CONCURRENCY": args.concurrency,
            "NUM_CPUS": args.num_cpus,
        }
    )
    try:
        main(args)
    finally:
        profiling.stop(s3_prefix=f"scheduling-loop/{JOB_ID}")
