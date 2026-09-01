"""Checks that the autoscaler scales up both worker groups when a pipeline is
compute bound on its CPU stage.

`produce` is a slow CPU stage and `consume` is a fast GPU stage holding one GPU
each. Saturating the GPU group isn't enough to keep the GPUs busy, so a healthy
autoscaler also has to add CPU nodes. The test asserts that the peak worker node
counts are exactly EXPECTED_GPU_NODES GPU nodes and at least MIN_CPU_NODES CPU
nodes.
"""

import argparse
import math
import time
from typing import Any, Dict, Iterator

import numpy as np
import pyarrow as pa

import ray

from benchmark import Benchmark
from cluster_resource_monitor import ClusterResourceMonitor

# With 1000 inputs this takes ~30 minutes, long enough that node provisioning
# speed doesn't make the test flaky.
NUM_INPUTS = 1000
BLOCKS_PER_INPUT = 4
PRODUCE_SLEEP_S = 5
CONSUME_SLEEP_S = 1
BLOCK_SHAPE = (128, 1024, 1024)
ROWS_PER_BLOCK = BLOCK_SHAPE[0]
CONSUME_BATCH_SIZE = 2 * ROWS_PER_BLOCK

# From the compute config.
MAX_GPU_NODES = 10
CPUS_PER_NODE = 8

EXPECTED_GPU_NODES = MAX_GPU_NODES

# `consume` handles 2 blocks/s and `produce` emits 1 block/5s, so balancing needs
# 10 produce workers per consume worker: 10 GPU nodes x 10 = 100 producer CPUs.
# The GPU nodes supply 80, so the remaining 20 need ceil(20 / 8) = 3 CPU nodes.
_PRODUCE_WORKERS_PER_CONSUME_WORKER = (
    CONSUME_BATCH_SIZE / ROWS_PER_BLOCK / CONSUME_SLEEP_S
) * PRODUCE_SLEEP_S
MIN_CPU_NODES = math.ceil(
    (
        EXPECTED_GPU_NODES * _PRODUCE_WORKERS_PER_CONSUME_WORKER
        - EXPECTED_GPU_NODES * CPUS_PER_NODE
    )
    / CPUS_PER_NODE
)
assert MIN_CPU_NODES > 0, (
    "The GPU nodes' own CPUs already satisfy the pipeline, so this test no "
    "longer exercises CPU scale-up. Adjust the sleeps or the compute config."
)


def produce(_: Dict[str, np.ndarray]) -> Iterator[Dict[str, np.ndarray]]:
    for _ in range(BLOCKS_PER_INPUT):
        time.sleep(PRODUCE_SLEEP_S)
        yield {"data": np.zeros(BLOCK_SHAPE, dtype=np.uint8)}


def consume(batch: Dict[str, np.ndarray]) -> Dict[str, np.ndarray]:
    time.sleep(CONSUME_SLEEP_S)
    return batch


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--assert-max-cpu-nodes",
        type=int,
        default=None,
        help="Assert that the autoscaler provisions at most this many CPU nodes.",
    )
    return parser.parse_args()


def main(args: argparse.Namespace) -> Dict[str, Any]:
    input_blocks = [
        pa.Table.from_pydict({"input_id": [input_id]}) for input_id in range(NUM_INPUTS)
    ]

    with ClusterResourceMonitor() as monitor:
        ds = (
            ray.data.from_blocks(input_blocks)
            .map_batches(produce)
            .map_batches(consume, num_gpus=1, num_cpus=0, batch_size=CONSUME_BATCH_SIZE)
        )
        # Don't materialize, so blocks are freed as they're consumed.
        for _ in ds.iter_internal_ref_bundles():
            pass

    peak_cpu_nodes = monitor.get_peak_cpu_nodes()
    peak_gpu_nodes = monitor.get_peak_gpu_nodes()
    print(f"Peak worker nodes: {peak_cpu_nodes} CPU, {peak_gpu_nodes} GPU")

    assert peak_gpu_nodes == EXPECTED_GPU_NODES, (
        f"Expected the autoscaler to provision {EXPECTED_GPU_NODES} GPU nodes, "
        f"but it provisioned {peak_gpu_nodes}"
    )
    assert peak_cpu_nodes >= MIN_CPU_NODES, (
        f"Expected the autoscaler to provision at least {MIN_CPU_NODES} CPU nodes "
        f"to balance the pipeline, but it provisioned {peak_cpu_nodes}"
    )
    if args.assert_max_cpu_nodes is not None:
        assert peak_cpu_nodes <= args.assert_max_cpu_nodes, (
            f"Expected the autoscaler to provision at most "
            f"{args.assert_max_cpu_nodes} CPU nodes, but it provisioned "
            f"{peak_cpu_nodes}"
        )

    return {
        "peak_cpu_nodes": peak_cpu_nodes,
        "peak_gpu_nodes": peak_gpu_nodes,
    }


if __name__ == "__main__":
    args = parse_args()
    ray.init()

    benchmark = Benchmark()
    benchmark.run_fn("main", main, args)
    benchmark.write_result()
