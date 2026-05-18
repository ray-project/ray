"""Benchmark for measuring worker scaling overhead under a production-shape schema.

Measures how long it takes to spin up workers, process data, and tear down
across a range(N) -> map_batches(N workers) -> consume pipeline, with each
map output block carrying a wide mixed-type schema:

  - ``--num-scalar-cols`` scalar float32 columns
  - ``--num-array64-cols`` float32[64] array columns
  - ``--num-array32-cols`` float32[32] array columns

Stresses the per-block schema propagation path (``ray.get(meta_ref)`` +
schema deserialization in ``on_data_ready``), which dominates large-schema
production workloads.
"""

import argparse
from typing import Dict, List

import numpy as np
import ray
from benchmark import (
    Benchmark,
    RuntimeEnvSetupTracker,
    benchmark_py_modules,
    collect_dataset_stats,
)

BLOCKS_PER_WORKER: int = 10
# Cap output block size to avoid OOM under wide schemas.
TARGET_BLOCK_SIZE_BYTES: int = 16 * 1024 * 1024  # 16 MiB
ARRAY_64_LEN: int = 64
ARRAY_32_LEN: int = 32


def _bytes_per_row(num_scalar: int, num_array_64: int, num_array_32: int) -> int:
    floats = num_scalar + num_array_64 * ARRAY_64_LEN + num_array_32 * ARRAY_32_LEN
    return 4 * floats  # float32


def _rows_per_block(num_scalar: int, num_array_64: int, num_array_32: int) -> int:
    bpr = _bytes_per_row(num_scalar, num_array_64, num_array_32)
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
        "--num-array64-cols",
        type=int,
        required=True,
        help=f"Number of float32[{ARRAY_64_LEN}] array columns per row.",
    )
    parser.add_argument(
        "--num-array32-cols",
        type=int,
        required=True,
        help=f"Number of float32[{ARRAY_32_LEN}] array columns per row.",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=42,
        help="Seed used to pre-roll template values once per UDF instance.",
    )
    return parser.parse_args()


class RealisticSchemaUDF:
    """Expands each input batch into a mixed-type wide-schema table."""

    def __init__(
        self,
        seed: int = 42,
        num_scalar_cols: int = 0,
        num_array_64_cols: int = 0,
        num_array_32_cols: int = 0,
    ):
        self._scalar_cols: List[str] = [
            f"scalar_col_{i}" for i in range(num_scalar_cols)
        ]
        self._array_64_cols: List[str] = [
            f"seq64_col_{i}" for i in range(num_array_64_cols)
        ]
        self._array_32_cols: List[str] = [
            f"target32_col_{i}" for i in range(num_array_32_cols)
        ]

        rng = np.random.default_rng(seed)
        self._scalar_template: np.ndarray = rng.uniform(
            0.0, 1.0, size=num_scalar_cols
        ).astype(np.float32)
        self._arr64_templates: np.ndarray = rng.uniform(
            0.0, 1.0, size=(num_array_64_cols, ARRAY_64_LEN)
        ).astype(np.float32)
        self._arr32_templates: np.ndarray = rng.uniform(
            0.0, 100.0, size=(num_array_32_cols, ARRAY_32_LEN)
        ).astype(np.float32)

    def __call__(self, batch) -> Dict[str, object]:
        n_rows = len(batch["id"])
        out: Dict[str, object] = {}

        for i, col in enumerate(self._scalar_cols):
            out[col] = np.full(n_rows, self._scalar_template[i], dtype=np.float32)

        for i, col in enumerate(self._array_64_cols):
            out[col] = [self._arr64_templates[i]] * n_rows

        for i, col in enumerate(self._array_32_cols):
            out[col] = [self._arr32_templates[i]] * n_rows

        return out


def make_realistic_schema_udf(
    seed: int = 42,
    num_scalar_cols: int = 0,
    num_array_64_cols: int = 0,
    num_array_32_cols: int = 0,
):
    """Functional variant of ``RealisticSchemaUDF`` for the task-based path."""
    udf = RealisticSchemaUDF(
        seed=seed,
        num_scalar_cols=num_scalar_cols,
        num_array_64_cols=num_array_64_cols,
        num_array_32_cols=num_array_32_cols,
    )
    return udf.__call__


def main(args: argparse.Namespace):
    benchmark = Benchmark()

    def benchmark_fn():
        num_blocks = BLOCKS_PER_WORKER * args.num_workers
        rows_per_block = _rows_per_block(
            args.num_scalar_cols,
            args.num_array64_cols,
            args.num_array32_cols,
        )
        num_rows = num_blocks * rows_per_block
        ds = ray.data.range(num_rows, override_num_blocks=num_blocks)

        map_kwargs = {"num_cpus": 1}
        if args.worker_type == "actors":
            map_kwargs["compute"] = ray.data.ActorPoolStrategy(size=args.num_workers)
            udf = RealisticSchemaUDF
            map_kwargs["fn_constructor_kwargs"] = {
                "seed": args.seed,
                "num_scalar_cols": args.num_scalar_cols,
                "num_array_64_cols": args.num_array64_cols,
                "num_array_32_cols": args.num_array32_cols,
            }
        else:
            udf = make_realistic_schema_udf(
                args.seed,
                args.num_scalar_cols,
                args.num_array64_cols,
                args.num_array32_cols,
            )

        ds = ds.map_batches(udf, **map_kwargs)

        ds = ds.materialize()
        metrics = collect_dataset_stats(ds)
        metrics["runtime_env_setup"] = RuntimeEnvSetupTracker.collect()
        metrics["num_blocks"] = num_blocks
        metrics["num_rows"] = num_rows
        metrics["num_scalar_cols"] = args.num_scalar_cols
        metrics["num_array64_cols"] = args.num_array64_cols
        metrics["num_array32_cols"] = args.num_array32_cols
        metrics["rows_per_block"] = rows_per_block
        metrics["bytes_per_row"] = _bytes_per_row(
            args.num_scalar_cols,
            args.num_array64_cols,
            args.num_array32_cols,
        )
        return metrics

    benchmark.run_fn("worker_scaling", benchmark_fn)
    benchmark.write_result()


if __name__ == "__main__":
    ray.init(runtime_env={"py_modules": benchmark_py_modules()})
    args = parse_args()
    main(args)
