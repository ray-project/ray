"""Reporting and result persistence for the benchmark."""

from __future__ import annotations

import json
import logging
from pathlib import Path
from statistics import mean
from typing import Optional

from ray.llm._internal.serve.benchmark.metrics import (
    percentile,
    serialize_raw_metrics,
    summarize_metrics,
)
from ray.llm._internal.serve.benchmark.models import TurnMetric, WorkloadSpec

logger = logging.getLogger(__name__)


def report_results(
    metrics: list[TurnMetric],
    spec: WorkloadSpec,
    bench_elapsed_s: float,
    first_chunk_threshold: int = 16,
    save_path: Optional[str] = None,
    warmup_s: float = 0.0,
    discarded_warmup_requests: int = 0,
) -> None:
    """Print and optionally save benchmark results."""
    if not metrics:
        print("No metrics collected.")
        return

    all_ttft = [m.ttft_ms for m in metrics]
    all_fc = [m.fc_ms for m in metrics]
    all_tpot = [m.tpot_ms for m in metrics if m.tpot_ms > 0]
    all_latency = [m.latency_ms for m in metrics]
    all_input = [m.input_tokens for m in metrics]
    all_output = [m.output_tokens for m in metrics]

    total_output_tokens = sum(all_output)
    throughput = total_output_tokens / bench_elapsed_s if bench_elapsed_s > 0 else 0

    print()
    print("=" * 70)
    print("BENCHMARK RESULTS")
    print("=" * 70)
    print(f"  Total requests:       {len(metrics)}")
    print(f"  Unique sessions:      {len({m.session_id for m in metrics})}")
    print(f"  Duration:             {bench_elapsed_s:.1f}s")
    if warmup_s > 0:
        print(f"  Warm-up excluded:     {warmup_s:.1f}s")
    if discarded_warmup_requests > 0:
        print(f"  Warm-up requests:     {discarded_warmup_requests} (discarded)")
    print(f"  Throughput:           {throughput:.1f} output tok/s")
    print(f"  Request rate:         {len(metrics) / bench_elapsed_s:.1f} req/s")
    print(
        f"  Avg input tokens:     {mean(all_input):.0f}  "
        f"(target ISL: {spec.effective_isl:.0f})"
    )
    print(f"  Avg output tokens:    {mean(all_output):.0f}  (target OSL: {spec.osl})")
    print()

    fc_label = f"FC({first_chunk_threshold})"
    print("  Latency Statistics:")
    for name, values in [
        ("TTFT", all_ttft),
        (fc_label, all_fc),
        ("TPOT", all_tpot),
        ("Latency", all_latency),
    ]:
        if not values:
            continue
        print(
            f"    {name:>8}:  avg={mean(values):>8.1f}ms  "
            f"P50={percentile(values, 50):>8.1f}ms  "
            f"P90={percentile(values, 90):>8.1f}ms  "
            f"P99={percentile(values, 99):>8.1f}ms"
        )
    print()

    print("  Per-Turn Breakdown:")
    print(
        f"    {'Turn':<6} {'Count':<7} {'Avg ISL':<9} {'Avg TTFT':<10} "
        f"{'Avg FC':<10} {'Avg TPOT':<10} {'Avg Lat':<10}"
    )
    for t in range(spec.num_turns):
        turn_metrics = [m for m in metrics if m.turn == t]
        if not turn_metrics:
            continue
        t_ttft = mean([m.ttft_ms for m in turn_metrics])
        t_fc = mean([m.fc_ms for m in turn_metrics])
        t_tpot_vals = [m.tpot_ms for m in turn_metrics if m.tpot_ms > 0]
        t_tpot = mean(t_tpot_vals) if t_tpot_vals else 0.0
        t_lat = mean([m.latency_ms for m in turn_metrics])
        t_isl = mean([m.input_tokens for m in turn_metrics])
        print(
            f"    {t + 1:<6} {len(turn_metrics):<7} {t_isl:<9.0f} "
            f"{t_ttft:<10.1f} {t_fc:<10.1f} {t_tpot:<10.1f} {t_lat:<10.1f}"
        )
    print("=" * 70)

    if save_path:
        stats = summarize_metrics(metrics, bench_elapsed_s)
        result = {
            "config": {
                "concurrency": spec.concurrency,
                "request_rate": spec.request_rate,
            },
            "spec": spec.summary(),
            "first_chunk_threshold": first_chunk_threshold,
            "benchmark": {
                "total_requests": len(metrics),
                "duration_s": round(bench_elapsed_s, 2),
                "warmup_s": round(warmup_s, 2),
                "discarded_warmup_requests": discarded_warmup_requests,
            },
            "stats": {
                ("measured_request_rate" if k == "request_rate" else k): v
                for k, v in stats.items()
                if k not in ("requests", "elapsed_s")
            },
            "per_turn": [],
            "raw_metrics": serialize_raw_metrics(metrics),
        }

        for t in range(spec.num_turns):
            turn_metrics = [m for m in metrics if m.turn == t]
            if not turn_metrics:
                continue
            t_ttft = [m.ttft_ms for m in turn_metrics]
            t_fc = [m.fc_ms for m in turn_metrics]
            t_tpot = [m.tpot_ms for m in turn_metrics if m.tpot_ms > 0]
            t_isl = [m.input_tokens for m in turn_metrics]
            result["per_turn"].append(
                {
                    "turn": t + 1,
                    "count": len(turn_metrics),
                    "avg_isl": round(mean(t_isl), 1),
                    "avg_ttft_ms": round(mean(t_ttft), 2),
                    "avg_fc_ms": round(mean(t_fc), 2),
                    "avg_tpot_ms": round(mean(t_tpot), 2) if t_tpot else 0,
                    "p50_fc_ms": round(percentile(t_fc, 50), 2),
                    "p99_ttft_ms": round(percentile(t_ttft, 99), 2),
                    "p99_fc_ms": round(percentile(t_fc, 99), 2),
                    "p99_tpot_ms": (round(percentile(t_tpot, 99), 2) if t_tpot else 0),
                }
            )

        Path(save_path).parent.mkdir(parents=True, exist_ok=True)
        with open(save_path, "w") as f:
            json.dump(result, f, indent=2)
        logger.info("Results saved to %s", save_path)
