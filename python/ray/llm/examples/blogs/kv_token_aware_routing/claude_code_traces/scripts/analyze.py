#!/usr/bin/env python3
"""Reduce validated CC benchmark cells to the plotted metrics."""

from __future__ import annotations

import argparse
import csv
import json
import math
import statistics
from collections import defaultdict
from pathlib import Path
from typing import Any, Iterator

ROUTER_VARIANTS = (
    "session-affinity",
    "kv-token-aware-balanced",
    "kv-token-aware-kv-biased",
)
CONCURRENCIES = (8, 16, 24, 32, 40)


def jsonl(path: Path) -> Iterator[dict[str, Any]]:
    with path.open() as file:
        for line in file:
            try:
                value = json.loads(line)
            except json.JSONDecodeError:
                continue
            if isinstance(value, dict):
                yield value


def metric(record: dict[str, Any], name: str) -> Any:
    value = (record.get("metrics") or {}).get(name)
    return value.get("value") if isinstance(value, dict) else value


def percentile(values: list[float], quantile: float) -> float:
    if not values:
        raise ValueError("cannot calculate a percentile of no values")
    ordered = sorted(values)
    index = quantile / 100 * (len(ordered) - 1)
    lower = math.floor(index)
    upper = math.ceil(index)
    return ordered[lower] + (ordered[upper] - ordered[lower]) * (index - lower)


def profile_path(cell: Path) -> Path:
    paths = list(cell.glob("aiperf_artifacts/**/profile_export.jsonl"))
    if not paths:
        raise FileNotFoundError(f"{cell}: missing profile_export.jsonl")
    return paths[0]


def request_rows(cell: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for record in jsonl(profile_path(cell)):
        metadata = record.get("metadata") or {}
        if metadata.get("benchmark_phase") != "profiling":
            continue
        ttft_ms = metric(record, "time_to_first_token")
        if ttft_ms is None:
            ttft_ms = metric(record, "time_to_first_output_token")
        rows.append(
            {
                "request_id": metadata.get("x_request_id"),
                "start_ns": metadata.get("request_start_ns"),
                "end_ns": metadata.get("request_end_ns"),
                "ttft_ms": ttft_ms,
                "tpot_ms": metric(record, "inter_token_latency"),
                "isl": metric(record, "input_sequence_length"),
                "osl": metric(record, "output_sequence_length"),
                "prompt_tokens": metric(record, "usage_prompt_tokens"),
                "cached_prompt_tokens": metric(record, "usage_prompt_cache_read_tokens"),
            }
        )
    if not rows:
        raise ValueError(f"{cell}: no profiling requests")
    return rows


def attach_replicas(cell: Path, rows: list[dict[str, Any]]) -> None:
    routes_by_id: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for path in sorted((cell / "routing").glob("routing.*.jsonl")):
        for route in jsonl(path):
            request_id = route.get("request_id")
            if request_id and route.get("replica_id"):
                routes_by_id[str(request_id)].append(route)
    for routes in routes_by_id.values():
        routes.sort(key=lambda route: float(route.get("ts") or 0))

    for row in rows:
        request_id = str(row["request_id"])
        routes = routes_by_id.get(request_id, [])
        if len(routes) != 1:
            raise ValueError(
                f"{cell}: expected one route for request {request_id}, found {len(routes)}"
            )
        row["replica_id"] = str(routes[0]["replica_id"])


def decode_load_cv(rows: list[dict[str, Any]], replicas: int) -> float:
    replica_ids = sorted({str(row["replica_id"]) for row in rows})
    replica_ids.extend(f"idle-{index}" for index in range(len(replica_ids), replicas))
    loads_by_bin: dict[int, dict[str, float]] = defaultdict(lambda: defaultdict(float))
    for row in rows:
        start_ns, end_ns = row["start_ns"], row["end_ns"]
        ttft_ms, isl, osl = row["ttft_ms"], row["isl"], row["osl"]
        if not all(
            isinstance(value, (int, float)) for value in (start_ns, end_ns, ttft_ms, isl, osl)
        ):
            raise ValueError("request is missing timing or token data")
        first_token_s = float(start_ns) / 1e9 + float(ttft_ms) / 1000
        end_s = float(end_ns) / 1e9
        if end_s <= first_token_s:
            continue
        duration = end_s - first_token_s
        for bucket in range(math.floor(first_token_s / 0.5), math.floor((end_s - 1e-9) / 0.5) + 1):
            elapsed_fraction = min(1.0, max(0.0, ((bucket + 0.5) * 0.5 - first_token_s) / duration))
            blocks = math.ceil((float(isl) + elapsed_fraction * float(osl)) / 64)
            loads_by_bin[bucket][str(row["replica_id"])] += blocks
    cvs = []
    for loads in loads_by_bin.values():
        values = [loads.get(replica_id, 0.0) for replica_id in replica_ids]
        mean = statistics.fmean(values)
        if mean:
            cvs.append(statistics.pstdev(values) / mean)
    if not cvs:
        raise ValueError("no active decode intervals")
    return statistics.fmean(cvs)


def selected_cell(cells_dir: Path, router_variant: str, concurrency: int) -> Path:
    cell = cells_dir / router_variant / f"c{concurrency}"
    metadata_path = cell / "meta.json"
    if not metadata_path.exists():
        raise FileNotFoundError(f"no validated {router_variant} cell at concurrency {concurrency}")
    metadata = json.loads(metadata_path.read_text())
    if metadata.get("raw_validation_passed") is not True:
        raise ValueError(f"{cell}: raw validation did not pass")
    return cell


def summarize_cell(cell: Path) -> dict[str, float | int | str]:
    metadata = json.loads((cell / "meta.json").read_text())
    rows = request_rows(cell)
    attach_replicas(cell, rows)
    ttft = [float(row["ttft_ms"]) for row in rows if isinstance(row["ttft_ms"], (int, float))]
    tpot = [float(row["tpot_ms"]) for row in rows if isinstance(row["tpot_ms"], (int, float))]
    starts = [int(row["start_ns"]) for row in rows if isinstance(row["start_ns"], (int, float))]
    ends = [int(row["end_ns"]) for row in rows if isinstance(row["end_ns"], (int, float))]
    output_tokens = sum(float(row["osl"]) for row in rows if isinstance(row["osl"], (int, float)))
    observed_cache_rows = [
        row
        for row in rows
        if isinstance(row["prompt_tokens"], (int, float))
        and isinstance(row["cached_prompt_tokens"], (int, float))
    ]
    prompt_tokens = [float(row["prompt_tokens"]) for row in observed_cache_rows]
    cached_tokens = [float(row["cached_prompt_tokens"]) for row in observed_cache_rows]
    if not ttft or not tpot or not starts or not ends or not prompt_tokens:
        raise ValueError(f"{cell}: incomplete latency, timing, or cache telemetry")
    window_s = (max(ends) - min(starts)) / 1e9
    if window_s <= 0:
        raise ValueError(f"{cell}: invalid measurement window")
    return {
        "router_variant": str(metadata["router_variant"]),
        "concurrency": int(metadata["concurrency"]),
        "ttft_p90_ms": percentile(ttft, 90),
        "tpot_p90_ms": percentile(tpot, 90),
        "output_tok_s_per_gpu": output_tokens / window_s / 8,
        "active_decode_blocks_cv": decode_load_cv(rows, replicas=4),
        "prefix_cache_hit_rate": sum(cached_tokens) / sum(prompt_tokens),
        "requests": len(rows),
        "cache_telemetry_coverage": len(observed_cache_rows) / len(rows),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cells-dir", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args()

    summaries = [
        summarize_cell(selected_cell(args.cells_dir, router_variant, concurrency))
        for concurrency in CONCURRENCIES
        for router_variant in ROUTER_VARIANTS
    ]
    args.out.parent.mkdir(parents=True, exist_ok=True)
    with args.out.open("w", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=sorted(summaries[0]))
        writer.writeheader()
        writer.writerows(summaries)
    print(f"[analyze] wrote {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
