#!/usr/bin/env python
"""
Single-node vLLM baseline benchmark for Ray Data LLM batch inference.

Measures throughput and supports env-driven thresholds and
JSON artifact output.
"""
import json
import os
import re
import sys
import time

import pytest
import requests

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


def _parse_prometheus_labels(label_blob: str | None) -> dict[str, str]:
    if not label_blob:
        return {}

    labels: dict[str, str] = {}
    for key, value in re.findall(r'(\w+)="((?:\\.|[^"\\])*)"', label_blob):
        labels[key] = bytes(value, "utf-8").decode("unicode_escape")
    return labels


def _metrics_scrape_addresses(node_addr: str) -> list[str]:
    addresses: list[str] = []
    for addr in (node_addr, "localhost", "127.0.0.1"):
        if addr and addr not in addresses:
            addresses.append(addr)
    return addresses


def _get_prometheus_metric_snapshot(
    metric_names: set[str],
) -> dict[str, list[tuple[dict[str, str], float]]]:
    snapshot: dict[str, list[tuple[dict[str, str], float]]] = {
        metric_name: [] for metric_name in metric_names
    }
    prom_pattern = re.compile(
        r"^([a-zA-Z_:][a-zA-Z0-9_:]*)(\{[^}]*\})?\s+"
        r"([+-]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?)$"
    )

    for node in ray.nodes():
        metrics_port = node.get("MetricsExportPort", -1)
        if metrics_port is None or metrics_port <= 0:
            continue

        node_addr = node.get("NodeManagerAddress", "127.0.0.1")
        response = None
        last_error = None

        for addr in _metrics_scrape_addresses(node_addr):
            try:
                candidate = requests.get(
                    f"http://{addr}:{metrics_port}/metrics", timeout=5
                )
                candidate.raise_for_status()
                response = candidate
                break
            except requests.RequestException as exc:
                last_error = exc
        if response is None:
            print(
                f"Warning: failed to scrape Ray Prometheus metrics "
                f"from node {node_addr}:{metrics_port}: {last_error}"
            )
            continue

        for line in response.text.splitlines():
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            match = prom_pattern.match(line)
            if not match:
                continue
            metric_name, raw_labels, value = match.groups()
            if metric_name not in metric_names:
                continue
            labels = _parse_prometheus_labels(raw_labels[1:-1] if raw_labels else None)
            snapshot[metric_name].append((labels, float(value)))

    return snapshot


def _sum_metric(
    snapshot: dict[str, list[tuple[dict[str, str], float]]],
    metric_name: str,
    model_id: str,
) -> float:
    series = snapshot.get(metric_name, [])
    total = 0.0
    has_model_name_label = False
    has_matching_model = False

    for labels, value in series:
        labeled_model = labels.get("model_name")
        if labeled_model is None:
            total += value
            continue

        has_model_name_label = True
        if labeled_model == model_id:
            has_matching_model = True
            total += value

    if has_model_name_label and not has_matching_model:
        return 0.0
    return total


def _build_engine_metrics(
    before_snapshot: dict[str, list[tuple[dict[str, str], float]]],
    after_snapshot: dict[str, list[tuple[dict[str, str], float]]],
    elapsed_s: float,
    model_id: str,
) -> dict[str, float | None]:
    prompt_tokens_metric = "ray_vllm_request_prompt_tokens_sum"
    generation_tokens_metric = "ray_vllm_generation_tokens_total"
    tpot_sum_metric = "ray_vllm_request_time_per_output_token_seconds_sum"
    tpot_count_metric = "ray_vllm_request_time_per_output_token_seconds_count"
    e2e_sum_metric = "ray_vllm_e2e_request_latency_seconds_sum"
    e2e_count_metric = "ray_vllm_e2e_request_latency_seconds_count"

    def metric_delta(metric_name: str) -> float:
        return max(
            0.0,
            _sum_metric(after_snapshot, metric_name, model_id)
            - _sum_metric(before_snapshot, metric_name, model_id),
        )

    prompt_tokens_delta = metric_delta(prompt_tokens_metric)
    generation_tokens_delta = metric_delta(generation_tokens_metric)
    total_tokens_delta = prompt_tokens_delta + generation_tokens_delta
    tpot_sum_delta = metric_delta(tpot_sum_metric)
    tpot_count_delta = metric_delta(tpot_count_metric)
    e2e_sum_delta = metric_delta(e2e_sum_metric)
    e2e_count_delta = metric_delta(e2e_count_metric)

    return {
        "prompt_tokens_total": prompt_tokens_delta,
        "generation_tokens_total": generation_tokens_delta,
        "total_tokens_total": total_tokens_delta,
        "generation_token_throughput_tok_per_s": (
            generation_tokens_delta / elapsed_s if elapsed_s > 0 else None
        ),
        "total_token_throughput_tok_per_s": (
            total_tokens_delta / elapsed_s if elapsed_s > 0 else None
        ),
        "mean_tpot_s": (
            tpot_sum_delta / tpot_count_delta if tpot_count_delta > 0 else None
        ),
        "mean_e2e_latency_s": (
            e2e_sum_delta / e2e_count_delta if e2e_count_delta > 0 else None
        ),
        "request_count": int(round(e2e_count_delta)),
    }


def _build_job_metrics(
    result, samples: int, engine_metrics: dict[str, float | None]
) -> dict:
    request_latency_stats_s = result.request_latency_stats_s
    mean_request_latency_s = (
        float(request_latency_stats_s["mean"])
        if request_latency_stats_s is not None
        else None
    )
    engine_e2e_s = engine_metrics.get("mean_e2e_latency_s")
    ray_data_overhead_per_req_s = (
        max(0.0, mean_request_latency_s - engine_e2e_s)
        if mean_request_latency_s is not None and engine_e2e_s is not None
        else None
    )

    return {
        "samples": int(samples),
        "elapsed_s": float(result.elapsed_s),
        "throughput_req_per_s": float(result.throughput),
        "mean_request_latency_s": mean_request_latency_s,
        "request_latency_stats_s": request_latency_stats_s,
        "engine_mean_e2e_latency_s": engine_e2e_s,
        "estimated_ray_data_overhead_per_req_s": ray_data_overhead_per_req_s,
    }


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

    metrics_export_port = int(
        os.getenv("RAY_DATA_LLM_BENCHMARK_METRICS_EXPORT_PORT", "8080")
    )
    ray_init_kwargs = {"ignore_reinit_error": True}

    if not os.getenv("RAY_ADDRESS"):
        ray_init_kwargs.update(
            _metrics_export_port=metrics_export_port,
            _system_config={"metrics_report_interval_ms": 1000},
        )

    ray.init(**ray_init_kwargs)

    ds = ray.data.from_items(prompts)

    # Benchmark config (single node, TP=1, PP=1)
    print(
        f"\nBenchmark: {MODEL_ID}, batch={BATCH_SIZE}, concurrency={CONCURRENCY}, TP=1, PP=1"
    )

    metric_names = {
        "ray_vllm_request_prompt_tokens_sum",
        "ray_vllm_generation_tokens_total",
        "ray_vllm_request_time_per_output_token_seconds_sum",
        "ray_vllm_request_time_per_output_token_seconds_count",
        "ray_vllm_e2e_request_latency_seconds_sum",
        "ray_vllm_e2e_request_latency_seconds_count",
    }
    before_snapshot = _get_prometheus_metric_snapshot(metric_names)

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
    )
    time.sleep(2)
    after_snapshot = _get_prometheus_metric_snapshot(metric_names)
    if not any(after_snapshot.values()):
        print(
            "Warning: no vLLM engine Prometheus metrics were scraped. "
            "Install Ray dashboard metrics deps (see "
            "python/requirements/llm/llm-test-requirements.txt) and ensure "
            "RAY_DATA_LLM_BENCHMARK_METRICS_EXPORT_PORT is reachable."
        )
    engine_metrics = _build_engine_metrics(
        before_snapshot=before_snapshot,
        after_snapshot=after_snapshot,
        elapsed_s=result.elapsed_s,
        model_id=MODEL_ID,
    )
    job_metrics = _build_job_metrics(result, len(prompts), engine_metrics)

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
    print("ENGINE_MEAN_TPOT_S:", engine_metrics["mean_tpot_s"])
    print("ENGINE_MEAN_E2E_LATENCY_S:", engine_metrics["mean_e2e_latency_s"])
    print(
        "ENGINE_GENERATION_TOKEN_THROUGHPUT_TOK_PER_S:",
        engine_metrics["generation_token_throughput_tok_per_s"],
    )
    print(
        "ENGINE_TOTAL_TOKEN_THROUGHPUT_TOK_PER_S:",
        engine_metrics["total_token_throughput_tok_per_s"],
    )
    print("MEAN_REQUEST_LATENCY_S:", job_metrics["mean_request_latency_s"])
    print("REQUEST_LATENCY_STATS_S:", job_metrics["request_latency_stats_s"])
    print(
        "ESTIMATED_RAY_DATA_OVERHEAD_PER_REQUEST_S:",
        job_metrics["estimated_ray_data_overhead_per_req_s"],
    )
    print("=" * 60)

    # Optional thresholds to fail on regressions
    min_throughput = _get_float_env("RAY_DATA_LLM_BENCHMARK_MIN_THROUGHPUT", 4)
    max_latency_s = _get_float_env("RAY_DATA_LLM_BENCHMARK_MAX_LATENCY_S", 150)
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
            "job_metrics": job_metrics,
            "engine_metrics": engine_metrics,
        }
        try:
            artifact_dir = os.path.dirname(artifact_path)
            if artifact_dir:
                os.makedirs(artifact_dir, exist_ok=True)
            with open(artifact_path, "w", encoding="utf-8") as f:
                json.dump(metrics, f, indent=2, sort_keys=True)
            print(f"Wrote benchmark artifact to: {artifact_path}")
        except Exception as e:  # noqa: BLE001
            print(
                f"Warning: failed to write benchmark artifact to {artifact_path}: {e}"
            )
    else:
        print(
            "Set RAY_LLM_BENCHMARK_ARTIFACT_PATH to write benchmark JSON "
            "(e.g. /tmp/ray_llm_benchmark.json)."
        )


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
