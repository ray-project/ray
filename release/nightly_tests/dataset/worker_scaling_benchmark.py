"""Benchmark for measuring actor scaling overhead with 1000 actors.

Measures how long it takes to spin up actors, process data, and tear down
across a range(N) -> map_batches(1000 actors) -> consume pipeline.

Optionally produces a wide, mixed-type output schema modeled after
production reference data. When ``--num-scalar-cols``,
``--num-array64-cols``, and/or ``--num-array32-cols`` are non-zero, the
default no-op UDF is replaced by a ``RealisticSchemaUDF`` that expands
each input batch into the specified number of:

  - scalar float32 columns
  - float32[64] array columns (sequence / embedding-shaped)
  - float32[32] array columns (target-level shape)

This shape is useful for stress-testing the per-block schema
propagation path (``ray.get(meta_ref)`` + schema deserialization in
``on_data_ready``), which dominates large-schema production workloads.
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
TARGET_BLOCK_SIZE_BYTES: int = 128 * 1024 * 1024  # 128 MiB
BYTES_PER_ROW: int = 8  # ray.data.range produces one int64 per row
ROWS_PER_BLOCK: int = TARGET_BLOCK_SIZE_BYTES // BYTES_PER_ROW

# When the wide-schema path is active, cap the output block size at
# ~16 MiB by adjusting rows-per-block proportionally to the per-row
# byte budget. Without this, wide schemas at the original 128 MiB
# sizing exceed worker memory.
WIDE_SCHEMA_TARGET_BLOCK_SIZE_BYTES: int = 16 * 1024 * 1024  # 16 MiB
ARRAY_64_LEN: int = 64
ARRAY_32_LEN: int = 32


def _bytes_per_row(num_scalar: int, num_array_64: int, num_array_32: int) -> int:
    floats = num_scalar + num_array_64 * ARRAY_64_LEN + num_array_32 * ARRAY_32_LEN
    return 4 * floats  # float32


def _rows_per_block_for_wide_schema(
    num_scalar: int, num_array_64: int, num_array_32: int
) -> int:
    bpr = _bytes_per_row(num_scalar, num_array_64, num_array_32)
    return max(WIDE_SCHEMA_TARGET_BLOCK_SIZE_BYTES // bpr, 1) if bpr else ROWS_PER_BLOCK


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
        default=0,
        help=(
            "Number of scalar float32 columns to emit per row. "
            "Default 0 (use the no-op UDF and preserve historical behavior)."
        ),
    )
    parser.add_argument(
        "--num-array64-cols",
        type=int,
        default=0,
        help=(
            f"Number of float32[{ARRAY_64_LEN}] array columns per row. " "Default 0."
        ),
    )
    parser.add_argument(
        "--num-array32-cols",
        type=int,
        default=0,
        help=(
            f"Number of float32[{ARRAY_32_LEN}] array columns per row. " "Default 0."
        ),
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=42,
        help="Seed used to pre-roll template values once per UDF instance.",
    )
    return parser.parse_args()


class NoOpUDF:
    def __call__(self, batch):
        return batch


def no_op_udf(batch):
    return batch


class RealisticSchemaUDF:
    """Expands each input batch into a mixed-type wide-schema table.

    For producer-side efficiency, one example value per column is
    pre-rolled in ``__init__`` and tiled across rows. The output schema
    is genuinely mixed-type (scalar float32 + ``list<float32>`` of two
    different lengths), so the per-block ``BlockMetadataWithSchema``
    reflects realistic deserialization cost without paying the data
    generation cost on every call.
    """

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

        # Array columns: each row gets a reference to the same template
        # array. The producer-side memory footprint is one buffer per
        # column type, but PyArrow serializes each row's array
        # independently so the per-block payload reflects realistic
        # ``list<float32>`` storage.
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


def _wide_schema_enabled(args: argparse.Namespace) -> bool:
    return (args.num_scalar_cols + args.num_array64_cols + args.num_array32_cols) > 0


def main(args: argparse.Namespace):
    benchmark = Benchmark()
    wide = _wide_schema_enabled(args)

    def benchmark_fn():
        num_blocks = BLOCKS_PER_WORKER * args.num_workers
        if wide:
            rows_per_block = _rows_per_block_for_wide_schema(
                args.num_scalar_cols,
                args.num_array64_cols,
                args.num_array32_cols,
            )
        else:
            rows_per_block = ROWS_PER_BLOCK
        num_rows = num_blocks * rows_per_block
        ds = ray.data.range(num_rows, override_num_blocks=num_blocks)

        if args.worker_type == "actors":
            if wide:
                ds = ds.map_batches(
                    RealisticSchemaUDF,
                    fn_constructor_kwargs={
                        "seed": args.seed,
                        "num_scalar_cols": args.num_scalar_cols,
                        "num_array_64_cols": args.num_array64_cols,
                        "num_array_32_cols": args.num_array32_cols,
                    },
                    num_cpus=1,
                    compute=ray.data.ActorPoolStrategy(size=args.num_workers),
                )
            else:
                ds = ds.map_batches(
                    NoOpUDF,
                    num_cpus=1,
                    compute=ray.data.ActorPoolStrategy(size=args.num_workers),
                )
        else:
            if wide:
                ds = ds.map_batches(
                    make_realistic_schema_udf(
                        args.seed,
                        args.num_scalar_cols,
                        args.num_array64_cols,
                        args.num_array32_cols,
                    ),
                    num_cpus=1,
                )
            else:
                ds = ds.map_batches(no_op_udf, num_cpus=1)

        ds = ds.materialize()
        metrics = collect_dataset_stats(ds)
        metrics["runtime_env_setup"] = RuntimeEnvSetupTracker.collect()
        metrics["num_blocks"] = num_blocks
        metrics["num_rows"] = num_rows
        if wide:
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
