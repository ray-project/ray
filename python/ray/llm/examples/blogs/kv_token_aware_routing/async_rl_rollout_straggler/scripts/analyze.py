#!/usr/bin/env python
"""Reduce router-cell artifacts to the metrics shown in the comparison plot."""

from __future__ import annotations

import argparse
import csv
import json
import math
import statistics
from collections import defaultdict
from pathlib import Path
from typing import Any

BIN_SECONDS = 0.5
# Matches the explicit engine setting in deploy.py.
VLLM_KV_BLOCK_TOKENS = 16
EXPECTED_ROLLOUTS = 80
TURNS_PER_ROLLOUT = 10


def read_jsonl(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for line in path.open():
        try:
            rows.append(json.loads(line))
        except json.JSONDecodeError:
            continue
    return rows


def metric(record: dict[str, Any], name: str) -> Any:
    value = (record.get("metrics") or {}).get(name)
    return value.get("value") if isinstance(value, dict) else value


def decode_start_ms(record: dict[str, Any]) -> Any:
    first_output = metric(record, "time_to_first_output_token")
    if isinstance(first_output, (int, float)):
        return first_output
    return metric(record, "time_to_first_token")


def percentile(values: list[float], quantile: float) -> float:
    ordered = sorted(values)
    if not ordered:
        return float("nan")
    index = (len(ordered) - 1) * quantile / 100
    lower, upper = math.floor(index), math.ceil(index)
    if lower == upper:
        return ordered[lower]
    return ordered[lower] + (ordered[upper] - ordered[lower]) * (index - lower)


def coefficient_of_variation(values: list[float]) -> float:
    mean = statistics.fmean(values)
    return statistics.pstdev(values) / mean if mean else float("nan")


def load_requests(cell: Path) -> list[dict[str, Any]]:
    export = cell / "aiperf_artifacts" / "profile_export.jsonl"
    requests: list[dict[str, Any]] = []
    for record in read_jsonl(export):
        metadata = record.get("metadata") or {}
        if metadata.get("benchmark_phase") not in (None, "profiling"):
            continue
        if metadata.get("was_cancelled") or record.get("error"):
            continue
        requests.append(
            {
                "request_id": metadata.get("x_request_id"),
                "session_id": metadata.get("conversation_id"),
                "turn": metadata.get("turn_index"),
                "start_ns": metadata.get("request_start_ns"),
                "end_ns": metadata.get("request_end_ns"),
                "ttft_ms": decode_start_ms(record),
                "isl": metric(record, "input_sequence_length"),
                "osl": metric(record, "output_sequence_length"),
                "prompt_tokens": metric(record, "usage_prompt_tokens"),
                "cached_prompt_tokens": metric(
                    record, "usage_prompt_cache_read_tokens"
                ),
            }
        )
    return requests


def attach_routes(cell: Path, requests: list[dict[str, Any]]) -> tuple[float, float]:
    by_request_id: dict[str, dict[str, Any]] = {}
    tracker_rows = tokenized_rows = 0
    for path in (cell / "routing").glob("routing.*.jsonl"):
        for route in read_jsonl(path):
            request_id = route.get("request_id")
            replica_id = route.get("replica_id")
            if request_id and replica_id:
                by_request_id[str(request_id)] = route
    for request in requests:
        placement = by_request_id.get(str(request.get("request_id")))
        if placement is None:
            raise SystemExit(f"{cell}: missing router decision for a profiling request")
        request["replica_id"] = str(placement["replica_id"])
        if placement.get("kv_tracker_present") is not None:
            tracker_rows += placement.get("kv_tracker_present") is True
            tokenized_rows += placement.get("kv_token_count") is not None
    routed = len(requests)
    return (
        tracker_rows / routed if routed else float("nan"),
        tokenized_rows / routed if routed else float("nan"),
    )


def prefix_cache_hit_rate(requests: list[dict[str, Any]]) -> tuple[float, float]:
    observed = [
        request
        for request in requests
        if isinstance(request["prompt_tokens"], (int, float))
        and isinstance(request["cached_prompt_tokens"], (int, float))
    ]
    if len(observed) != len(requests):
        raise SystemExit("missing response-level prefix-cache telemetry")
    prompt_tokens = sum(float(request["prompt_tokens"]) for request in observed)
    cached_tokens = sum(float(request["cached_prompt_tokens"]) for request in observed)
    return cached_tokens / prompt_tokens if prompt_tokens else float("nan"), len(
        observed
    ) / len(requests)


def decode_block_cv(requests: list[dict[str, Any]], replicas: int) -> float:
    served_replicas = sorted({str(request["replica_id"]) for request in requests})
    served_replicas += [
        f"idle-{index}" for index in range(len(served_replicas), replicas)
    ]
    bins: dict[int, dict[str, float]] = defaultdict(lambda: defaultdict(float))
    for request in requests:
        numeric = (
            request["start_ns"],
            request["end_ns"],
            request["ttft_ms"],
            request["isl"],
            request["osl"],
        )
        if not all(isinstance(value, (int, float)) for value in numeric):
            raise SystemExit(
                "missing timing or token count for decode-load reconstruction"
            )
        start_ns, end_ns, ttft_ms, isl, osl = (float(value) for value in numeric)
        first_token_s = start_ns / 1e9 + ttft_ms / 1000
        end_s = end_ns / 1e9
        if end_s <= first_token_s:
            continue
        first_bin = math.floor(first_token_s / BIN_SECONDS)
        last_bin = math.floor((end_s - 1e-9) / BIN_SECONDS)
        duration = end_s - first_token_s
        for bucket in range(first_bin, last_bin + 1):
            midpoint = (bucket + 0.5) * BIN_SECONDS
            emitted_fraction = min(1.0, max(0.0, (midpoint - first_token_s) / duration))
            blocks = math.ceil((isl + emitted_fraction * osl) / VLLM_KV_BLOCK_TOKENS)
            bins[bucket][str(request["replica_id"])] += blocks
    cvs = [
        coefficient_of_variation(
            [loads.get(replica, 0.0) for replica in served_replicas]
        )
        for loads in bins.values()
        if any(loads.values())
    ]
    finite_cvs = [value for value in cvs if math.isfinite(value)]
    return statistics.fmean(finite_cvs) if finite_cvs else float("nan")


def rollout_e2e_p99(requests: list[dict[str, Any]]) -> float:
    by_session: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for request in requests:
        by_session[str(request["session_id"])].append(request)
    if len(by_session) != EXPECTED_ROLLOUTS:
        raise SystemExit(
            f"expected {EXPECTED_ROLLOUTS} rollouts, got {len(by_session)}"
        )
    rollout_e2e_ms: list[float] = []
    for session, turns in by_session.items():
        if len(turns) != TURNS_PER_ROLLOUT:
            raise SystemExit(
                f"{session}: expected {TURNS_PER_ROLLOUT} turns, got {len(turns)}"
            )
        start_times = [turn["start_ns"] for turn in turns]
        end_times = [turn["end_ns"] for turn in turns]
        if not all(
            isinstance(value, (int, float)) for value in start_times + end_times
        ):
            raise SystemExit(f"{session}: missing rollout timing")
        rollout_e2e_ms.append((max(end_times) - min(start_times)) / 1e6)
    return percentile(rollout_e2e_ms, 99)


def summarize(cell: Path) -> dict[str, Any]:
    meta = json.loads((cell / "meta.json").read_text())
    requests = load_requests(cell)
    if len(requests) != EXPECTED_ROLLOUTS * TURNS_PER_ROLLOUT:
        raise SystemExit(f"{cell}: expected 800 complete turns, got {len(requests)}")
    tracker_rate, tokenized_rate = attach_routes(cell, requests)
    cache_rate, cache_coverage = prefix_cache_hit_rate(requests)
    replicas = int(meta["replicas"])
    return {
        "variant": meta["variant"],
        "cell_dir": str(cell.resolve()),
        "dag_sha256": meta["dag_sha256"],
        "aiperf_rc": meta["aiperf_rc"],
        "seed_rc": meta["seed_rc"],
        "routing_validation_rc": meta["routing_validation_rc"],
        "token_validation_rc": meta["token_validation_rc"],
        "response_cache_telemetry_coverage": cache_coverage,
        "response_cached_prompt_fraction": cache_rate,
        "reconstructed_decode_blocks_cv_mean": decode_block_cv(requests, replicas),
        "rollout_e2e_p99_ms": rollout_e2e_p99(requests),
        "kv_tracker_present_rate": tracker_rate,
        "kv_tokenized_route_rate": tokenized_rate,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--campaign", required=True, type=Path)
    parser.add_argument("--out-dir", required=True, type=Path)
    args = parser.parse_args()
    cells = sorted(path.parent for path in args.campaign.rglob("meta.json"))
    if not cells:
        raise SystemExit("no cell metadata found")
    rows = [summarize(cell) for cell in cells]
    args.out_dir.mkdir(parents=True, exist_ok=True)
    with (args.out_dir / "cells.csv").open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
    print(f"wrote {args.out_dir / 'cells.csv'}")


if __name__ == "__main__":
    main()
