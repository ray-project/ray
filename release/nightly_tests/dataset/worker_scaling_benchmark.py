"""Benchmark for measuring worker scaling overhead under a production-shape schema.

Measures how long it takes to spin up workers, process data, and tear down
across a range(N) -> map_batches(N workers) -> consume pipeline, with each
map output block carrying a wide mixed-type schema:

  - ``--num-scalar-cols`` scalar float32 columns
  - ``--num-array-cols`` float32[32] array columns

Stresses the per-block schema propagation path (``ray.get(meta_ref)`` +
schema deserialization in ``on_data_ready``), which dominates large-schema
production workloads.

Profiling is gated by env vars consumed by ``profiling.coordinator.Profiling``
(``PYSPY_ENABLED=1``, ``PERF_PROFILING_ENABLED=1`` etc.). When none are set,
the coordinator is a no-op aside from printing its configuration.
"""

import argparse
import os
import pickle
import uuid
from typing import Dict, List

import numpy as np
import ray
from benchmark import (
    Benchmark,
    RuntimeEnvSetupTracker,
    benchmark_py_modules,
    collect_dataset_stats,
)
from profiling.coordinator import Profiling

JOB_ID = os.environ.get("ANYSCALE_JOB_ID", f"local-{uuid.uuid4().hex[:8]}")
SHARED_OUTDIR = f"/mnt/shared_storage/worker_scaling/{JOB_ID}"

BLOCKS_PER_WORKER: int = 10
# Cap output block size to avoid OOM under wide schemas.
TARGET_BLOCK_SIZE_BYTES: int = 16 * 1024 * 1024  # 16 MiB
ARRAY_LEN: int = 32


def _bytes_per_row(num_scalar: int, num_array: int) -> int:
    floats = num_scalar + num_array * ARRAY_LEN
    return 4 * floats  # float32


def _rows_per_block(num_scalar: int, num_array: int) -> int:
    bpr = _bytes_per_row(num_scalar, num_array)
    return max(TARGET_BLOCK_SIZE_BYTES // bpr, 1)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--num-workers",
        type=int,
        required=True,
        help="Number of actors/tasks to use for map_batches.",
    )
    parser.add_argument(
        "--worker-type",
        type=str,
        choices=["actors", "tasks"],
        default="actors",
        help="Whether to use actors or regular tasks for map_batches.",
    )
    parser.add_argument(
        "--num-scalar-cols",
        type=int,
        required=True,
        help="Number of scalar float32 columns to emit per row.",
    )
    parser.add_argument(
        "--num-array-cols",
        type=int,
        required=True,
        help=f"Number of float32[{ARRAY_LEN}] array columns per row.",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=42,
        help="Seed used to pre-roll template values once per UDF instance.",
    )
    parser.add_argument(
        "--num-operators",
        type=int,
        default=1,
        help=(
            "Number of chained map_batches operators in the pipeline. "
            "The total worker pool (--num-workers) is split evenly across "
            "operators (each gets num_workers // num_operators workers). "
            "Useful for stressing the per-iteration update_usages / "
            "_update_allocated_budgets work that scales with N_ops."
        ),
    )
    args = parser.parse_args()
    if args.num_scalar_cols + args.num_array_cols <= 0:
        parser.error(
            "At least one of --num-scalar-cols / --num-array-cols must be > 0."
        )
    if args.num_operators < 1:
        parser.error("--num-operators must be >= 1.")
    if args.num_workers < args.num_operators:
        parser.error(
            f"--num-workers ({args.num_workers}) must be >= --num-operators "
            f"({args.num_operators}) so each operator gets at least one worker."
        )
    return args


class RealisticSchemaUDF:
    """Expands each input batch into a mixed-type wide-schema table."""

    def __init__(
        self,
        seed: int = 42,
        num_scalar_cols: int = 0,
        num_array_cols: int = 0,
    ):
        self._scalar_cols: List[str] = [
            f"scalar_col_{i}" for i in range(num_scalar_cols)
        ]
        self._array_cols: List[str] = [f"array_col_{i}" for i in range(num_array_cols)]

        rng = np.random.default_rng(seed)
        self._scalar_template: np.ndarray = rng.uniform(
            0.0, 1.0, size=num_scalar_cols
        ).astype(np.float32)
        self._array_templates: np.ndarray = rng.uniform(
            0.0, 100.0, size=(num_array_cols, ARRAY_LEN)
        ).astype(np.float32)

    def __call__(self, batch) -> Dict[str, object]:
        # Derive the row count from any input column rather than hardcoding
        # "id": when operators are chained, the UDF output (scalar/array cols
        # only) becomes the next operator's input and no longer contains "id".
        n_rows = len(next(iter(batch.values())))
        out: Dict[str, object] = {}

        for i, col in enumerate(self._scalar_cols):
            out[col] = np.full(n_rows, self._scalar_template[i], dtype=np.float32)

        for i, col in enumerate(self._array_cols):
            out[col] = [self._array_templates[i]] * n_rows

        return out


def make_realistic_schema_udf(
    seed: int = 42,
    num_scalar_cols: int = 0,
    num_array_cols: int = 0,
):
    """Functional variant of ``RealisticSchemaUDF`` for the task-based path."""
    udf = RealisticSchemaUDF(
        seed=seed,
        num_scalar_cols=num_scalar_cols,
        num_array_cols=num_array_cols,
    )
    return udf.__call__


def _disable_operator_fusion() -> None:
    """Stop Ray Data from fusing the chained map_batches into one operator.

    Ray Data's optimizer fuses linear chains of compatible map operators
    (same compute + remote args) into a single physical operator. With
    identical map_batches that collapses the whole --num-operators chain into
    one fused operator, so the scheduling topology has 1 operator no matter
    what --num-operators is set to — defeating the purpose of this variant,
    which exists to measure scheduling-loop cost as a function of the number
    of operators. There's no public toggle (and batch_size/UDF differences
    don't block map->map fusion), so remove the rule from the DeveloperAPI
    physical ruleset.
    """
    from ray.data._internal.logical.optimizers import get_physical_ruleset
    from ray.data._internal.logical.rules import FuseOperators

    ruleset = get_physical_ruleset()
    try:
        ruleset.remove(FuseOperators)
    except ValueError:
        pass  # Already removed.


def main(args: argparse.Namespace):
    # Keep the chained operators separate so the topology actually has
    # --num-operators operators (see the function docstring).
    if args.num_operators > 1:
        _disable_operator_fusion()

    benchmark = Benchmark()

    def benchmark_fn():
        num_blocks = BLOCKS_PER_WORKER * args.num_workers
        rows_per_block = _rows_per_block(
            args.num_scalar_cols,
            args.num_array_cols,
        )
        num_rows = num_blocks * rows_per_block
        ds = ray.data.range(num_rows, override_num_blocks=num_blocks)

        # Split the total worker pool evenly across the chained operators so the
        # cluster footprint stays the same regardless of --num-operators. With
        # 5000 workers and 15 operators each operator gets ~333 workers, which
        # mirrors production pipelines that pay the per-iteration cost of many
        # ops with a moderately sized pool per op.
        workers_per_operator = args.num_workers // args.num_operators

        map_kwargs = {"num_cpus": 0.5}
        if args.worker_type == "actors":
            map_kwargs["compute"] = ray.data.ActorPoolStrategy(
                size=workers_per_operator
            )
            udf = RealisticSchemaUDF
            map_kwargs["fn_constructor_kwargs"] = {
                "seed": args.seed,
                "num_scalar_cols": args.num_scalar_cols,
                "num_array_cols": args.num_array_cols,
            }
        else:
            # ``concurrency`` caps in-flight tasks per operator. Without this
            # cap, all tasks of a single operator can fan out across the entire
            # cluster and the next operator in the chain starves — but the goal
            # here is N_operators sharing the pool, so each gets
            # ``workers_per_operator`` task slots.
            map_kwargs["concurrency"] = workers_per_operator
            udf = make_realistic_schema_udf(
                args.seed,
                args.num_scalar_cols,
                args.num_array_cols,
            )

        for _ in range(args.num_operators):
            ds = ds.map_batches(udf, **map_kwargs)

        ds = ds.materialize()
        metrics = collect_dataset_stats(ds)
        metrics["runtime_env_setup"] = RuntimeEnvSetupTracker.collect()
        metrics["num_blocks"] = num_blocks
        metrics["num_rows"] = num_rows
        metrics["num_scalar_cols"] = args.num_scalar_cols
        metrics["num_array_cols"] = args.num_array_cols
        metrics["rows_per_block"] = rows_per_block
        metrics["bytes_per_row"] = _bytes_per_row(
            args.num_scalar_cols,
            args.num_array_cols,
        )
        metrics["schema_pickled_bytes"] = len(pickle.dumps(ds.schema()))
        metrics["num_operators"] = args.num_operators
        metrics["workers_per_operator"] = workers_per_operator
        return metrics

    benchmark.run_fn("worker_scaling", benchmark_fn)
    benchmark.write_result()


if __name__ == "__main__":
    # ``Profiling.start()`` spawns ``_UDFPySpyProfiler`` actors on worker
    # nodes. To deserialize that actor class, the worker has to import
    # ``profiling.pyspy`` — which lives at this script's ``profiling/``
    # sibling and isn't on the worker's Python path by default. Ship the
    # directory alongside ``benchmark.py`` so workers can resolve the
    # import.
    import profiling as _profiling_pkg

    _profiling_dir = os.path.dirname(os.path.abspath(_profiling_pkg.__file__))
    ray.init(runtime_env={"py_modules": benchmark_py_modules() + [_profiling_dir]})
    args = parse_args()

    profiling = Profiling(outdir=SHARED_OUTDIR, num_gpu_nodes=0)
    profiling.start(
        extra_config={
            "RAY_COMMIT": ray.__commit__,
            "NUM_WORKERS": args.num_workers,
            "WORKER_TYPE": args.worker_type,
            "NUM_SCALAR_COLS": args.num_scalar_cols,
            "NUM_ARRAY_COLS": args.num_array_cols,
        }
    )
    try:
        main(args)
    finally:
        profiling.stop(s3_prefix=f"worker-scaling/{JOB_ID}")
