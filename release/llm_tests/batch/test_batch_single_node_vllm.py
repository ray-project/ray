#!/usr/bin/env python
"""
Single-node vLLM baseline benchmark for Ray Data LLM batch inference.

Uses the public processor API (like the multi-node test) to avoid internal
import collisions. Measures throughput and supports env-driven thresholds and
JSON artifact output.
"""
import json
import os
import time
import sys

import pytest

import ray
from ray.data.llm import build_llm_processor, vLLMEngineProcessorConfig
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


def _get_float_env(name: str) -> float | None:
    value = os.getenv(name)
    if value is None or value == "":
        return None
    try:
        return float(value)
    except ValueError:
        raise AssertionError(f"Invalid float for {name}: {value}")


def test_single_node_baseline_benchmark():
    """
    Single-node baseline benchmark: facebook/opt-1.3b, TP=1, PP=1, 1000 prompts.

    Logs BENCHMARK_* metrics and optionally asserts perf thresholds from env:
    - RAY_LLM_BENCHMARK_MIN_THROUGHPUT (req/s)
    - RAY_LLM_BENCHMARK_MAX_LATENCY_S (seconds)
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

    num_requests = 1000
    print(f"Loading {num_requests} prompts from ShareGPT dataset...")
    prompts = dataset.sample(num_requests=num_requests)
    print(f"Loaded {len(prompts)} prompts")

    ds = ray.data.from_items(prompts)

    # Benchmark config (single node, TP=1, PP=1)
    model = "facebook/opt-1.3b"
    batch_size = 64
    concurrency = 1

    cfg = vLLMEngineProcessorConfig(
        model_source=model,
        engine_kwargs=dict(
            pipeline_parallel_size=1,
            tensor_parallel_size=1,
            distributed_executor_backend="mp",
        ),
        tokenize=False,
        detokenize=False,
        apply_chat_template=False,
        concurrency=concurrency,
        batch_size=batch_size,
    )

    processor = build_llm_processor(
        cfg,
        preprocess=lambda row: dict(
            prompt=row["prompt"],
            sampling_params=dict(
                temperature=0.3,
                max_tokens=128,
                detokenize=True,
            ),
        ),
        postprocess=lambda row: dict(resp=row["generated_text"]),
    )

    print(
        f"\nBenchmark: {model}, batch={batch_size}, concurrency={concurrency}, TP=1, PP=1"
    )

    # Measure end-to-end time for a single pass
    start = time.perf_counter()
    out = processor(ds).count()
    elapsed = time.perf_counter() - start

    # Assertions and metrics
    assert out == len(prompts)
    throughput = out / elapsed if elapsed > 0 else 0.0
    assert throughput > 0

    print("\n" + "=" * 60)
    print("BENCHMARK METRICS")
    print("=" * 60)
    print(f"BENCHMARK_THROUGHPUT: {throughput:.4f} req/s")
    print(f"BENCHMARK_LATENCY: {elapsed:.4f} s")
    print(f"BENCHMARK_SAMPLES: {out}")
    print("=" * 60)

    # Optional thresholds to fail on regressions
    min_throughput = _get_float_env("RAY_LLM_BENCHMARK_MIN_THROUGHPUT")
    max_latency_s = _get_float_env("RAY_LLM_BENCHMARK_MAX_LATENCY_S")
    if min_throughput is not None:
        assert (
            throughput >= min_throughput
        ), f"Throughput regression: {throughput:.4f} < {min_throughput:.4f} req/s"
    if max_latency_s is not None:
        assert (
            elapsed <= max_latency_s
        ), f"Latency regression: {elapsed:.4f} > {max_latency_s:.4f} s"

    # Optional JSON artifact emission for downstream ingestion
    artifact_path = os.getenv("RAY_LLM_BENCHMARK_ARTIFACT_PATH")
    if artifact_path:
        metrics = {
            "model": model,
            "batch_size": batch_size,
            "concurrency": concurrency,
            "samples": int(out),
            "throughput_req_per_s": float(throughput),
            "elapsed_s": float(elapsed),
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
