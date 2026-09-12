"""Checks that the autoscaler scales up both worker groups when a pipeline is
compute bound on its CPU stage.

`produce` is a slow CPU stage and `consume` is a fast GPU stage holding one GPU
each. Saturating the GPU group isn't enough to keep the GPUs busy, so a healthy
autoscaler also has to add CPU nodes. The test asserts that the peak worker node
counts are exactly the GPU group's size and at least the derived CPU node count.
"""

import argparse
import functools
import math
import time
from typing import Any, Dict, Iterator

import numpy as np
import pyarrow as pa

import ray

from benchmark import Benchmark
from cluster_resource_monitor import ClusterResourceMonitor


def produce(
    _: Dict[str, np.ndarray],
    *,
    num_output_batches: int,
    sleep_s: float,
    num_rows_per_output_batch: int,
    size_bytes_per_output_batch: int,
) -> Iterator[Dict[str, np.ndarray]]:
    size_bytes_per_row = size_bytes_per_output_batch // num_rows_per_output_batch
    for _ in range(num_output_batches):
        time.sleep(sleep_s)
        yield {
            "data": np.zeros(
                (num_rows_per_output_batch, size_bytes_per_row), dtype=np.uint8
            )
        }


def consume(batch: Dict[str, np.ndarray], *, sleep_s: float) -> Dict[str, np.ndarray]:
    time.sleep(sleep_s)
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
    # With 10,000 inputs this takes ~30 minutes, long enough that node
    # provisioning speed doesn't make the test flaky.
    num_inputs = 10000
    num_output_batches = 4
    produce_sleep_s = 5
    consume_sleep_s = 1
    num_rows_per_output_batch = 128
    size_bytes_per_output_batch = 128 * 1024 * 1024
    consume_batch_size = 2 * num_rows_per_output_batch

    # From the compute config.
    expected_gpu_nodes = 10
    cpus_per_node = 8

    # Each consumer processes 2 batches/s. Each producer emits 0.2 batches/s, so
    # one consumer needs 10 producers.
    producer_batches_per_consumer_batch = consume_batch_size / num_rows_per_output_batch
    producers_per_consumer = (
        producer_batches_per_consumer_batch * produce_sleep_s / consume_sleep_s
    )

    # Ten consumers need 100 producer CPUs. The GPU nodes provide 80, leaving a
    # 20-CPU shortfall, or 3 CPU-only nodes.
    producer_cpus_needed = expected_gpu_nodes * producers_per_consumer
    producer_cpus_on_gpu_nodes = expected_gpu_nodes * cpus_per_node
    cpu_shortfall = producer_cpus_needed - producer_cpus_on_gpu_nodes
    min_cpu_nodes = math.ceil(cpu_shortfall / cpus_per_node)
    assert min_cpu_nodes > 0, (
        "The GPU nodes' own CPUs already satisfy the pipeline, so this test no "
        "longer exercises CPU scale-up. Adjust the sleeps or the compute config."
    )

    producer = functools.partial(
        produce,
        num_output_batches=num_output_batches,
        sleep_s=produce_sleep_s,
        num_rows_per_output_batch=num_rows_per_output_batch,
        size_bytes_per_output_batch=size_bytes_per_output_batch,
    )
    consumer = functools.partial(consume, sleep_s=consume_sleep_s)

    input_blocks = [
        pa.Table.from_pydict({"input_id": [input_id]}) for input_id in range(num_inputs)
    ]

    with ClusterResourceMonitor() as monitor:
        ds = (
            ray.data.from_blocks(input_blocks)
            .map_batches(producer)
            .map_batches(
                consumer, num_gpus=1, num_cpus=0, batch_size=consume_batch_size
            )
        )
        # Don't materialize, so blocks are freed as they're consumed.
        for _ in ds.iter_internal_ref_bundles():
            pass

    peak_cpu_nodes = monitor.get_peak_cpu_nodes()
    peak_gpu_nodes = monitor.get_peak_gpu_nodes()
    print(f"Peak worker nodes: {peak_cpu_nodes} CPU, {peak_gpu_nodes} GPU")

    assert peak_gpu_nodes == expected_gpu_nodes, (
        f"Expected the autoscaler to provision {expected_gpu_nodes} GPU nodes, "
        f"but it provisioned {peak_gpu_nodes}"
    )
    assert peak_cpu_nodes >= min_cpu_nodes, (
        f"Expected the autoscaler to provision at least {min_cpu_nodes} CPU nodes "
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
