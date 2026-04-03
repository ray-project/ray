"""Metrics computation and serialization for the benchmark."""

from __future__ import annotations

from statistics import mean

import numpy as np

from ray.llm._internal.serve.benchmark.models import TurnMetric


def percentile(values: list[float], p: float) -> float:
    """Compute the p-th percentile (0-100)."""
    if not values:
        return 0.0
    return float(np.percentile(values, p))


def summarize_metrics(metrics: list[TurnMetric], elapsed_s: float) -> dict:
    """Compute aggregate statistics from a list of TurnMetrics.

    ITL (inter-token latency) statistics are computed from raw per-token values
    flattened across all requests, capturing the full distribution including variance.
    """
    if not metrics:
        return {"requests": 0, "elapsed_s": round(elapsed_s, 2)}

    ttft = [m.ttft_ms for m in metrics]
    fc = [m.fc_ms for m in metrics]
    # Flatten per-token ITL values across all requests for accurate distribution stats
    itl_all = [v for m in metrics for v in m.itl_ms_list]
    latency = [m.latency_ms for m in metrics]
    out_tok = [m.output_tokens for m in metrics]
    in_tok = [m.input_tokens for m in metrics]
    total_output_tokens = sum(out_tok)

    return {
        "requests": len(metrics),
        "elapsed_s": round(elapsed_s, 2),
        "request_rate": round(len(metrics) / elapsed_s, 2) if elapsed_s > 0 else 0.0,
        "throughput_tok_s": round(total_output_tokens / elapsed_s, 1)
        if elapsed_s > 0
        else 0.0,
        "avg_input_tokens": round(mean(in_tok), 1),
        "avg_output_tokens": round(mean(out_tok), 1),
        "avg_ttft_ms": round(mean(ttft), 2),
        "p50_ttft_ms": round(percentile(ttft, 50), 2),
        "p90_ttft_ms": round(percentile(ttft, 90), 2),
        "p99_ttft_ms": round(percentile(ttft, 99), 2),
        "avg_fc_ms": round(mean(fc), 2),
        "p50_fc_ms": round(percentile(fc, 50), 2),
        "p90_fc_ms": round(percentile(fc, 90), 2),
        "p99_fc_ms": round(percentile(fc, 99), 2),
        "avg_itl_ms": round(float(np.mean(itl_all)), 2) if itl_all else 0.0,
        "std_itl_ms": round(float(np.std(itl_all)), 2) if itl_all else 0.0,
        "p50_itl_ms": round(percentile(itl_all, 50), 2) if itl_all else 0.0,
        "p90_itl_ms": round(percentile(itl_all, 90), 2) if itl_all else 0.0,
        "p99_itl_ms": round(percentile(itl_all, 99), 2) if itl_all else 0.0,
        "avg_latency_ms": round(mean(latency), 2),
        "p50_latency_ms": round(percentile(latency, 50), 2),
        "p90_latency_ms": round(percentile(latency, 90), 2),
        "p99_latency_ms": round(percentile(latency, 99), 2),
    }


def serialize_raw_metrics(metrics: list[TurnMetric]) -> list[dict]:
    """Serialize TurnMetrics to dicts suitable for JSON output."""
    return [
        {
            "session_id": m.session_id,
            "turn": m.turn,
            "ttft_ms": round(m.ttft_ms, 2),
            "fc_ms": round(m.fc_ms, 2),
            "itl_ms": round(m.itl_ms, 2),
            "latency_ms": round(m.latency_ms, 2),
            "input_tokens": m.input_tokens,
            "output_tokens": m.output_tokens,
            "start_time_ms": round(m.start_time_ms, 2),
        }
        for m in metrics
    ]
