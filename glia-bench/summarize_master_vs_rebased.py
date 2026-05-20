#!/usr/bin/env python3
"""Summarize master-vs-rebased perf sweep into a markdown table.

Reads the JSONL emitted by run_master_vs_rebased.sh and prints:
  1) per-(config, workload) mean wall + stdev + tps + output-hash agreement
  2) per-workload Δ (rebased vs master)
"""
import argparse
import json
import statistics
from collections import defaultdict
from pathlib import Path


def load(path):
    return [json.loads(line) for line in open(path) if line.strip()]


def summarize(rows):
    by = defaultdict(list)
    for r in rows:
        by[(r["config"], r["workload"])].append(r)
    out = {}
    for k, rs in by.items():
        walls = [r["wall_time_sec"] for r in rs]
        tps = [r["throughput_blocks_per_sec"] for r in rs]
        hashes = {r.get("output_hash") for r in rs}
        out[k] = {
            "n": len(rs),
            "wall_mean": statistics.mean(walls),
            "wall_stdev": statistics.stdev(walls) if len(walls) > 1 else 0.0,
            "tps_mean": statistics.mean(tps),
            "hashes": hashes,
        }
    return out


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "jsonl",
        nargs="?",
        default="results/optimization_perf_master_vs_rebased.jsonl",
    )
    args = parser.parse_args()
    rows = load(args.jsonl)
    s = summarize(rows)

    workloads = sorted({w for _, w in s.keys()}, key=lambda x: (
        # put control workloads after gain-driven ones
        x == "long_tasks", x
    ))

    print(f"{'workload':22s}  {'master':>14s}  {'rebased':>14s}  {'Δ wall':>8s}  {'Δ tps':>8s}  {'hash':>8s}")
    print("-" * 90)
    for wl in workloads:
        m = s.get(("master", wl))
        r = s.get(("rebased", wl))
        if not (m and r):
            continue
        master_str = f"{m['wall_mean']:6.2f}±{m['wall_stdev']:.2f}s"
        rebased_str = f"{r['wall_mean']:6.2f}±{r['wall_stdev']:.2f}s"
        delta_wall = (r["wall_mean"] - m["wall_mean"]) / m["wall_mean"] * 100
        delta_tps = (r["tps_mean"] - m["tps_mean"]) / m["tps_mean"] * 100
        # hash agreement
        if (m["hashes"] == r["hashes"]) and len(m["hashes"]) == 1:
            hash_str = "match"
        else:
            hash_str = "DIFF"
        print(
            f"{wl:22s}  {master_str:>14s}  {rebased_str:>14s}  "
            f"{delta_wall:>+7.1f}%  {delta_tps:>+7.1f}%  {hash_str:>8s}"
        )


if __name__ == "__main__":
    main()
