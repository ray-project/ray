#!/usr/bin/env python
"""
Single-node vLLM baseline benchmark for Ray Data LLM batch inference.

Measures throughput and supports env-driven thresholds and
JSON artifact output.
"""
import json
import os
import sys

import pytest

import ray
from ray.llm._internal.batch.benchmark.dataset import ShareGPTDataset
from ray.llm._internal.batch.benchmark.benchmark_processor import (
    Mode,
    VLLM_SAMPLING_PARAMS,
    benchmark,
)


# Benchmark constants
NUM_REQUESTS = 1000
MODEL_ID = "facebook/opt-1.3b"
BATCH_SIZE = 64
CONCURRENCY = 1


@pytest.fixture(autouse=True)
def disable_vllm_compile_cache(monkeypatch):
    """Disable vLLM compile cache to avoid cache corruption."""
    monkeypatch.setenv("VLLM_DISABLE_COMPILE_CACHE", "1")


@pytest.fixture(autouse=True)
def cleanup_ray_resources():
    """Cleanup Ray resources between tests."""
    yield
    ray.shutdown()


def _get_float_env(name: str, default: float | None = None) -> float | None:
    value = os.getenv(name)
    if value is None or value == "":
        return default
    try:
        return float(value)
    except ValueError:
        raise AssertionError(f"Invalid float for {name}: {value}")


def test_single_node_baseline_benchmark():
    """
    Single-node baseline benchmark: facebook/opt-1.3b, TP=1, PP=1, 1000 prompts.

    Logs BENCHMARK_* metrics and optionally asserts perf thresholds from env:
    - RAY_DATA_LLM_BENCHMARK_MIN_THROUGHPUT (req/s)
    - RAY_DATA_LLM_BENCHMARK_MAX_LATENCY_S (seconds)
    Writes JSON artifact to RAY_LLM_BENCHMARK_ARTIFACT_PATH if set.
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

    print(f"Loading {NUM_REQUESTS} prompts from ShareGPT dataset...")
    prompts = dataset.sample(num_requests=NUM_REQUESTS)
    print(f"Loaded {len(prompts)} prompts")

    ds = ray.data.from_items(prompts)

    # Benchmark config (single node, TP=1, PP=1)
    print(
        f"\nBenchmark: {MODEL_ID}, batch={BATCH_SIZE}, concurrency={CONCURRENCY}, TP=1, PP=1"
    )

    # Use benchmark processor to run a single-node vLLM benchmark
    result = benchmark(
        Mode.VLLM_ENGINE,
        ds,
        batch_size=BATCH_SIZE,
        concurrency=CONCURRENCY,
        model=MODEL_ID,
        sampling_params=VLLM_SAMPLING_PARAMS,
        pipeline_parallel_size=1,
        tensor_parallel_size=1,
        distributed_executor_backend="mp",
    )

    result.show()

    # Assertions and metrics
    assert result.samples == len(prompts)
    assert result.throughput > 0

    print("\n" + "=" * 60)
    print("BENCHMARK METRICS")
    print("=" * 60)
    print(f"BENCHMARK_THROUGHPUT: {result.throughput:.4f} req/s")
    print(f"BENCHMARK_LATENCY: {result.elapsed_s:.4f} s")
    print(f"BENCHMARK_SAMPLES: {result.samples}")
    print("=" * 60)

    # Optional thresholds to fail on regressions
    min_throughput = _get_float_env("RAY_DATA_LLM_BENCHMARK_MIN_THROUGHPUT", 5)
    max_latency_s = _get_float_env("RAY_DATA_LLM_BENCHMARK_MAX_LATENCY_S", 120)
    if min_throughput is not None:
        assert (
            result.throughput >= min_throughput
        ), f"Throughput regression: {result.throughput:.4f} < {min_throughput:.4f} req/s"
    if max_latency_s is not None:
        assert (
            result.elapsed_s <= max_latency_s
        ), f"Latency regression: {result.elapsed_s:.4f} > {max_latency_s:.4f} s"

    # Optional JSON artifact emission for downstream ingestion
    artifact_path = os.getenv("RAY_LLM_BENCHMARK_ARTIFACT_PATH")
    if artifact_path:
        metrics = {
            "model": MODEL_ID,
            "batch_size": BATCH_SIZE,
            "concurrency": CONCURRENCY,
            "samples": int(result.samples),
            "throughput_req_per_s": float(result.throughput),
            "elapsed_s": float(result.elapsed_s),
        }
        try:
            os.makedirs(os.path.dirname(artifact_path), exist_ok=True)
            with open(artifact_path, "w", encoding="utf-8") as f:
                json.dump(metrics, f, indent=2, sort_keys=True)
            print(f"Wrote benchmark artifact to: {artifact_path}")
        except Exception as e:  # noqa: BLE001
            print(
                f"Warning: failed to write benchmark artifact to {artifact_path}: {e}"
            )


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
