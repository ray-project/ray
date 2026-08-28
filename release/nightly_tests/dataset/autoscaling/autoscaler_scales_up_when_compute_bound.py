import argparse
import time
from typing import Any, Dict, Iterator

import numpy as np
import pyarrow as pa

import ray

from benchmark import Benchmark
from cluster_resource_monitor import ClusterResourceMonitor

# See #XXXXX for how these are derived. Don't shrink NUM_INPUTS: adding nodes
# takes minutes, so a shorter run is flaky.
NUM_INPUTS = 1000
BLOCKS_PER_INPUT = 4
PRODUCE_SLEEP_S = 5
CONSUME_SLEEP_S = 1
BLOCK_SHAPE = (128, 1024, 1024)
CONSUME_BATCH_SIZE = 2 * BLOCK_SHAPE[0]
EXPECTED_GPU_NODES = 10
MIN_CPU_NODES = 3


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
    """This test checks if the cluster scales up enough to balance the pipeline."""
    if not ray.is_initialized():
        ray.init()

    input_blocks = [
        pa.Table.from_pydict({"input_id": [input_id]}) for input_id in range(NUM_INPUTS)
    ]

    with ClusterResourceMonitor() as monitor:
        ds = (
            ray.data.from_blocks(input_blocks)
            .map_batches(produce)
            .map_batches(consume, num_gpus=1, batch_size=CONSUME_BATCH_SIZE)
        )
        # Don't materialize, so blocks are freed as they're consumed.
        for _ in ds.iter_internal_ref_bundles():
            pass

    peak_nodes = monitor.get_peak_node_counts()
    print(f"Peak worker nodes: {peak_nodes.cpu} CPU, {peak_nodes.gpu} GPU")

    assert peak_nodes.gpu == EXPECTED_GPU_NODES, (
        f"Expected the autoscaler to provision {EXPECTED_GPU_NODES} GPU nodes, "
        f"but it provisioned {peak_nodes.gpu}"
    )
    assert peak_nodes.cpu >= MIN_CPU_NODES, (
        f"Expected the autoscaler to provision at least {MIN_CPU_NODES} CPU nodes "
        f"to balance the pipeline, but it provisioned {peak_nodes.cpu}"
    )
    if args.assert_max_cpu_nodes is not None:
        assert peak_nodes.cpu <= args.assert_max_cpu_nodes, (
            f"Expected the autoscaler to provision at most "
            f"{args.assert_max_cpu_nodes} CPU nodes, but it provisioned "
            f"{peak_nodes.cpu}"
        )

    return {
        "peak_cpu_nodes": peak_nodes.cpu,
        "peak_gpu_nodes": peak_nodes.gpu,
    }


if __name__ == "__main__":
    args = parse_args()

    benchmark = Benchmark()
    benchmark.run_fn("main", main, args)
    benchmark.write_result()
