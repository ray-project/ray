import argparse
import gc
import json
import os
import statistics
from contextlib import contextmanager
from shutil import rmtree
from time import perf_counter
from typing import Any, Dict, List

from ray.anyscale.safetensors import set_local_cache_dir
from ray.anyscale.safetensors._private.http_downloader import _flush_pending_operations
from ray.anyscale.safetensors.torch import load_file

CACHE_DIR = "/mnt/local_storage/safetensors/"
DEFAULT_MODEL_URI = (
    "anyscale://release_test_weights/Mistral-7B-Instruct-v0.1.safetensors"
)

parser = argparse.ArgumentParser(
    "Benchmarking script for 'ray.anyscale.safetensors.torch.load_file'"
)
parser.add_argument("--device", type=str, default="cpu")
parser.add_argument("--model-uri", type=str, default=DEFAULT_MODEL_URI)
parser.add_argument("--num-trials", type=int, default=5)


@contextmanager
def _record_latency(key: str, results: Dict[str, float], *, cleanup: bool = True):
    print("Starting", key)
    start_time_s = perf_counter()
    yield
    results[key] = perf_counter() - start_time_s
    print("Finished", key)

    if cleanup:
        _flush_pending_operations(timeout_s=10)
        gc.collect()


def _run_trial(*, model_uri: str, device: str) -> Dict[str, float]:
    results: Dict[str, float] = {}

    rmtree(CACHE_DIR, ignore_errors=True)
    gc.collect()
    _flush_pending_operations(timeout_s=10)

    # Remote load time with no cache.
    set_local_cache_dir(None)
    with _record_latency("remote_load_no_disk_cache_latency_s", results):
        load_file(model_uri, device=device)

    # Remote load time with cache.
    set_local_cache_dir(CACHE_DIR)
    with _record_latency("disk_write_latency_s", results):
        # Don't clean up after load finishes because we use _flush_pending_operations
        # to measure the disk write time.
        with _record_latency(
            "remote_load_with_disk_cache_latency_s", results, cleanup=False
        ):
            load_file(model_uri, device=device)

        _flush_pending_operations(timeout_s=30)

    # Local load time from cache.
    with _record_latency("disk_load_latency_s", results):
        state_dict = load_file(model_uri, device=device)

    # Compute throughput metrics.
    total_size_bytes = sum(t.numel() * t.element_size() for t in state_dict.values())
    total_size_gb = total_size_bytes / (1024**3)
    results["remote_load_no_disk_cache_throughput_gb_s"] = (
        total_size_gb / results["remote_load_no_disk_cache_latency_s"]
    )
    results["remote_load_with_disk_cache_throughput_gb_s"] = (
        total_size_gb / results["remote_load_with_disk_cache_latency_s"]
    )
    results["disk_write_throughput_gb_s"] = (
        total_size_gb / results["disk_write_latency_s"]
    )
    results["disk_load_throughput_gb_s"] = (
        total_size_gb / results["disk_load_latency_s"]
    )

    return results


def _aggregate_results(trial_results: List[Dict[str, float]]) -> Dict[str, float]:
    """Return aggregated metric statistics.

    Assumes all trial results have the same keys.
    """
    metric_keys = list(trial_results[0].keys())

    results = {}
    for k in metric_keys:
        series = [trial[k] for trial in trial_results]
        results[k + "_min"] = min(series)
        results[k + "_max"] = max(series)
        results[k + "_p50"] = statistics.median(series)
        results[k + "_avg"] = statistics.mean(series)
        results[k + "_stdev"] = statistics.stdev(series)

    return results


def _convert_to_perf_metric_format(
    metrics: Dict[str, float],
) -> List[Dict[str, Any]]:
    """Convert metrics to the expected format of the release test pipeline.

    Returns a list of dicts with the keys:
        - perf_metric_name
        - perf_metric_value
        - perf_metric_type (LATENCY, THROUGHPUT)
    """
    perf_metrics = []
    for k, v in metrics.items():
        if "latency" in k:
            perf_metric_type = "LATENCY"
        elif "throughput" in k:
            perf_metric_type = "THROUGHPUT"
        else:
            raise ValueError("All metric names must contain 'latency' or 'throughput'.")

        perf_metrics.append(
            {
                "perf_metric_name": k,
                "perf_metric_value": v,
                "perf_metric_type": perf_metric_type,
            }
        )

    return perf_metrics


def run_benchmark(
    *,
    model_uri: str,
    device: str,
    num_trials: int,
) -> Dict[str, Any]:
    trial_results = []
    for i in range(num_trials + 1):
        print(f"Starting trial {i} / {num_trials}", "(warmup)" if i == 0 else "")

        trial_result = _run_trial(model_uri=model_uri, device=device)
        if i != 0:
            trial_results.append(trial_result)

        print("Finished trial:", trial_result)

    return {
        "device": device,
        "model_uri": model_uri,
        "num_trials": num_trials,
        "perf_metrics": _convert_to_perf_metric_format(
            _aggregate_results(trial_results),
        ),
    }


if __name__ == "__main__":
    results = json.dumps(
        run_benchmark(**vars(parser.parse_args())), indent=4, sort_keys=True
    )
    print(f"===== Benchmark results =====\n{results}\n=============================")

    output_path = os.environ.get("TEST_OUTPUT", "/tmp/release_test_out.json")
    if output_path:
        print(f"Writing results to: '{output_path}'")
        with open(output_path, "w") as f:
            f.write(results)
    else:
        print("'TEST_OUTPUT' env var not detected; not writing results.")
