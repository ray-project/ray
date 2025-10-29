#!/usr/bin/env python
"""
Single-node baseline benchmark for Ray Data LLM batch inference.

Measures throughput for simple single-node vLLM config (TP=1, PP=1) as a
consistent performance monitor across Ray releases.
"""
import os
import sys

import pytest

import ray
from ray.llm._internal.batch.benchmark.benchmark_processor import (
    Mode,
    VLLM_SAMPLING_PARAMS,
    benchmark,
)
from ray.llm._internal.batch.benchmark.dataset import ShareGPTDataset


@pytest.fixture(autouse=True)
def disable_vllm_compile_cache(monkeypatch):
    """Disable vLLM compile cache to avoid cache corruption."""
    monkeypatch.setenv("VLLM_DISABLE_COMPILE_CACHE", "1")


@pytest.fixture(autouse=True)
def cleanup_ray_resources():
    """Cleanup Ray resources between tests."""
    yield
    ray.shutdown()


def test_single_node_baseline_benchmark():
    """
    Single-node baseline benchmark: facebook/opt-1.3b, TP=1, PP=1, 1000 prompts.

    Logs BENCHMARK_THROUGHPUT and BENCHMARK_LATENCY for release test tracking.
    """
    # Dataset setup
    dataset_path = os.getenv(
        "RAY_LLM_BENCHMARK_DATASET_PATH", "/tmp/ray_llm_benchmark_dataset"
    )

    dataset = ShareGPTDataset(
        dataset_path=dataset_path,
        seed=0,
        hf_dataset_id="Crystalcareai/Code-feedback-sharegpt-renamed",
        hf_split="train",
        truncate_prompt=2048,
    )

    print(f"Loading {1000} prompts from ShareGPT dataset...")
    prompts = dataset.sample(num_requests=1000)
    print(f"Loaded {len(prompts)} prompts")

    ds = ray.data.from_items(prompts)

    # Benchmark config
    model = "facebook/opt-1.3b"
    batch_size = 64
    concurrency = 1

    print(
        f"\nBenchmark: {model}, batch={batch_size}, concurrency={concurrency}, TP=1, PP=1"
    )

    # Run benchmark
    result = benchmark(
        Mode.VLLM_ENGINE,
        ds,
        batch_size=batch_size,
        concurrency=concurrency,
        model=model,
        sampling_params=VLLM_SAMPLING_PARAMS,
        pipeline_parallel_size=1,
        tensor_parallel_size=1,
        distributed_executor_backend="mp",
    )

    result.show()

    # Assertions
    assert result.samples == len(prompts)
    assert result.throughput > 0

    # Log metrics for release test tracking
    print("\n" + "=" * 60)
    print("BENCHMARK METRICS")
    print("=" * 60)
    print(f"BENCHMARK_THROUGHPUT: {result.throughput:.4f} req/s")
    print(f"BENCHMARK_LATENCY: {result.elapsed_s:.4f} s")
    print(f"BENCHMARK_SAMPLES: {result.samples}")
    print("=" * 60)


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
