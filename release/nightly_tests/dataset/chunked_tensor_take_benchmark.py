"""Measure local-shuffle tensor ingestion through complete Ray Data pipelines.

Synthetic float32 payloads represent decoded images and the (2000, 1697) tensor
shape from issue #64960. Every measured iteration generates blocks with
map_batches and consumes them through iter_batches with local shuffle; inputs
are not materialized or retained between iterations. Model computation and
image decoding are outside this data-ingestion benchmark.

Set RAY_DATA_ENABLE_CHUNKED_TENSOR_TAKE before starting the process to compare
implementations. Both release entries use the same workload and measurement
code, with no internal function patches or manually constructed Arrow buffers.
"""

import argparse
import math
import os
import statistics
import time
from dataclasses import dataclass
from typing import Any, Dict, Tuple

import numpy as np
from benchmark import Benchmark

import ray


@dataclass(frozen=True)
class Workload:
    name: str
    shape: Tuple[int, ...]
    rows: int
    blocks: int
    batch_size: int
    shuffle_buffer_rows: int


WORKLOADS = (
    Workload("image_tensor_ingest", (224, 224, 3), 1024, 32, 32, 256),
    Workload("wide_tensor_ingest", (2000, 1697), 128, 32, 8, 64),
)


def make_tensors(
    batch: Dict[str, np.ndarray], *, shape: Tuple[int, ...]
) -> Dict[str, np.ndarray]:
    """Create fixed-shape tensors through the normal NumPy batch interface."""
    row_ids = batch["id"]
    values = np.empty((len(row_ids), *shape), dtype=np.float32)
    values.reshape(len(row_ids), -1)[:] = row_ids[:, None]
    return {"id": row_ids, "tensor": values}


def run_pipeline(workload: Workload, *, validate_payload: bool) -> np.ndarray:
    """Generate and shuffle every row, returning the consumed row order.

    Full payload validation is done in an untimed warmup. Every timed iteration
    still checks shapes, types, row coverage, and alignment of IDs with tensors.
    """
    dataset = ray.data.range(
        workload.rows, override_num_blocks=workload.blocks
    ).map_batches(
        make_tensors, batch_format="numpy", fn_kwargs={"shape": workload.shape}
    )
    row_ids = []
    for batch in dataset.iter_batches(
        batch_size=workload.batch_size,
        batch_format="numpy",
        local_shuffle_buffer_size=workload.shuffle_buffer_rows,
        local_shuffle_seed=42,
        prefetch_batches=1,
    ):
        ids, values = batch["id"], batch["tensor"]
        assert values.shape == (len(ids), *workload.shape)
        assert values.dtype == np.float32
        flat_values = values.reshape(len(ids), -1)
        np.testing.assert_array_equal(flat_values[:, 0], ids)
        if validate_payload:
            np.testing.assert_array_equal(flat_values.min(axis=1), ids)
            np.testing.assert_array_equal(flat_values.max(axis=1), ids)
        row_ids.append(ids)
    actual = np.concatenate(row_ids)
    np.testing.assert_array_equal(np.sort(actual), np.arange(workload.rows))
    return actual


def measure_pipeline(
    workload: Workload, iterations: int, expected_order: np.ndarray
) -> Dict[str, Any]:
    """Measure repeated complete pipeline executions after a warmup."""
    durations = []
    for _ in range(iterations):
        start = time.perf_counter()
        actual_order = run_pipeline(workload, validate_payload=False)
        durations.append(time.perf_counter() - start)
        np.testing.assert_array_equal(actual_order, expected_order)
    median = statistics.median(durations)
    return {
        "fast_path_enabled": os.environ.get("RAY_DATA_ENABLE_CHUNKED_TENSOR_TAKE", "1"),
        "shape": workload.shape,
        "rows_per_iteration": workload.rows,
        "source_blocks": workload.blocks,
        "batch_size": workload.batch_size,
        "shuffle_buffer_rows": workload.shuffle_buffer_rows,
        "iterations": iterations,
        "iteration_times_s": durations,
        "median_iteration_s": median,
        "rows_per_s": workload.rows / median,
        "payload_gib_per_s": workload.rows
        * math.prod(workload.shape)
        * 4
        / median
        / 1024**3,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--iterations", type=int, default=3)
    parser.add_argument("--case", choices=[w.name for w in WORKLOADS])
    parser.add_argument("--num-cpus", type=int, help="CPU count for local Ray runs.")
    parser.add_argument(
        "--object-store-memory", type=int, help="Object store bytes for local Ray runs."
    )
    args = parser.parse_args()
    if args.iterations < 1:
        parser.error("--iterations must be positive")

    ray.init(num_cpus=args.num_cpus, object_store_memory=args.object_store_memory)
    context = ray.data.DataContext.get_current()
    context.execution_options.preserve_order = True
    benchmark = Benchmark()
    try:
        for workload in WORKLOADS:
            if args.case is not None and args.case != workload.name:
                continue
            expected_order = run_pipeline(workload, validate_payload=True)
            benchmark.run_fn(
                workload.name,
                measure_pipeline,
                workload,
                args.iterations,
                expected_order,
            )
    finally:
        benchmark.write_result()
        ray.shutdown()


if __name__ == "__main__":
    main()
