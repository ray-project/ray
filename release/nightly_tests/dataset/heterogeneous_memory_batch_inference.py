"""
Heterogeneous Memory Batch Inference Benchmark

Tests Ray Data memory management on a cluster with heterogeneous memory:
- CPU nodes: small memory, run data generation and preprocessing
- GPU nodes: large memory, run inference

The global object store memory threshold is the sum of all nodes' object store
memory. Because GPU nodes contribute a large share of that budget, CPU-only
stages can keep producing data without triggering backpressure, even when CPU
nodes' local object store is full. This benchmark exercises that scenario.

Pipeline: range -> gen_data -> cpu_process -> gpu_inference -> write
All UDFs are fake (sleep-based). Inference is the bottleneck.

Data size:
  - 400k rows x ~1 MB/row = ~400 GB total
  - Per CPU task: 1024 rows x 1 MB = ~1 GB
  - Per GPU task: 256 rows x 1 MB = ~256 MB

Cluster (heterogeneous_memory_compute.yaml):
  - 1 head node: m5.2xlarge (8 vCPUs, 32 GiB, no tasks scheduled)
  - 10 CPU workers: m5.2xlarge (8 vCPUs, 32 GiB, ~12 GiB object store each)
  - 2 GPU workers: r5.4xlarge (128 GiB, ~48 GiB object store each, 4 logical
    GPUs, 0 CPUs — only GPU tasks scheduled here)
  - Total object store: ~216 GiB (120 GiB CPU + 96 GiB GPU)
"""

import argparse
import time

import numpy as np
from benchmark import Benchmark, BenchmarkMetric

import ray


# ---------------------------------------------------------------------------
# UDFs
# ---------------------------------------------------------------------------


ROW_SIZE = 125_000  # ~1 MB per row (125K float64 elements x 8 bytes)


def gen_data(batch):
    """Generate ~1 MB of data per row."""
    n = len(batch["id"])
    batch["data"] = [np.random.rand(ROW_SIZE) for _ in range(n)]
    return batch


def cpu_process(batch):
    """Simulate CPU preprocessing. Moderately fast."""
    time.sleep(0.05)
    batch["processed"] = [1] * len(batch["data"])
    return batch


class FakeGPUInference:
    """Simulate slow GPU inference (bottleneck)."""

    def __init__(self):
        # Simulate model loading.
        time.sleep(2)

    def __call__(self, batch):
        time.sleep(0.5)
        batch["prediction"] = list(range(len(batch["data"])))
        return batch


def consume(batch):
    """Terminal op: discard data."""
    return {"n": [len(batch["prediction"])]}


# ---------------------------------------------------------------------------
# Pipeline
# ---------------------------------------------------------------------------


def build_and_run_pipeline(
    num_rows: int,
    gen_batch_size: int,
    cpu_batch_size: int,
    gpu_batch_size: int,
    gpu_concurrency: int,
):
    ds = ray.data.range(num_rows)

    ds = ds.map_batches(gen_data, batch_size=gen_batch_size)
    ds._set_name("gen_data")

    ds = ds.map_batches(cpu_process, batch_size=cpu_batch_size)
    ds._set_name("cpu_process")

    ds = ds.map_batches(
        FakeGPUInference,
        batch_size=gpu_batch_size,
        num_cpus=0,
        num_gpus=1,
        concurrency=gpu_concurrency,
    )
    ds._set_name("gpu_inference")

    ds = ds.map_batches(consume, batch_size=gpu_batch_size)
    ds._set_name("consume")

    total_rows = 0
    for batch in ds.iter_batches(batch_size=None):
        total_rows += batch["n"][0]

    return total_rows


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main(args):
    total_rows = build_and_run_pipeline(
        num_rows=args.num_rows,
        gen_batch_size=args.gen_batch_size,
        cpu_batch_size=args.cpu_batch_size,
        gpu_batch_size=args.gpu_batch_size,
        gpu_concurrency=args.gpu_concurrency,
    )

    return {
        BenchmarkMetric.NUM_ROWS: int(total_rows),
        "num_rows_input": int(args.num_rows),
        "gpu_concurrency": int(args.gpu_concurrency),
    }


def parse_args():
    p = argparse.ArgumentParser(
        description="Heterogeneous memory batch inference benchmark"
    )
    p.add_argument("--num-rows", type=int, default=400_000)
    p.add_argument("--gen-batch-size", type=int, default=1024)
    p.add_argument("--cpu-batch-size", type=int, default=1024)
    p.add_argument("--gpu-batch-size", type=int, default=256)
    p.add_argument("--gpu-concurrency", type=int, default=8)
    return p.parse_args()


if __name__ == "__main__":
    args = parse_args()
    benchmark = Benchmark()
    benchmark.run_fn("heterogeneous-memory-batch-inference", main, args)
    benchmark.write_result()
