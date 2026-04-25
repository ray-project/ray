"""Aggregate optimization_perf.jsonl into per-(config, workload) stats.

Usage:
    python aggregate_perf.py results/optimization_perf.jsonl

Emits a summary table (pristine vs M6, mean+stdev, delta%, Welch's t) and
checks output-hash equality across reps.
"""
import json
import math
import statistics
import sys
from collections import defaultdict


def welch_t(a, b):
    """Two-sample Welch's t statistic (unequal variance). Returns (t, df)."""
    if len(a) < 2 or len(b) < 2:
        return float("nan"), float("nan")
    ma, mb = statistics.mean(a), statistics.mean(b)
    va, vb = statistics.variance(a), statistics.variance(b)
    na, nb = len(a), len(b)
    denom = math.sqrt(va / na + vb / nb)
    if denom == 0:
        return float("inf"), float("nan")
    t = (ma - mb) / denom
    # Welch–Satterthwaite df
    num = (va / na + vb / nb) ** 2
    den = (va / na) ** 2 / max(na - 1, 1) + (vb / nb) ** 2 / max(nb - 1, 1)
    df = num / den if den else float("nan")
    return t, df


def load(path):
    by_key = defaultdict(list)
    hashes = defaultdict(set)
    with open(path) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            rec = json.loads(line)
            key = (rec["config"], rec["workload"])
            by_key[key].append(rec)
            hashes[(rec["workload"],)].add(rec["output_hash"])
    return by_key, hashes


def summarize(by_key, hashes):
    workloads = sorted({w for _, w in by_key})
    print(f"{'workload':<18} {'config':<10} {'n':>3} "
          f"{'wall_mean':>10} {'wall_sd':>8} {'thpt_mean':>10} {'thpt_sd':>8}")
    for wl in workloads:
        for cfg in ("pristine", "m6"):
            recs = by_key.get((cfg, wl), [])
            if not recs:
                continue
            walls = [r["wall_time_sec"] for r in recs]
            thpts = [r["throughput_blocks_per_sec"] for r in recs]
            print(
                f"{wl:<18} {cfg:<10} {len(recs):>3} "
                f"{statistics.mean(walls):>10.3f} "
                f"{(statistics.stdev(walls) if len(walls)>1 else 0):>8.3f} "
                f"{statistics.mean(thpts):>10.3f} "
                f"{(statistics.stdev(thpts) if len(thpts)>1 else 0):>8.3f}"
            )

    print()
    print(f"{'workload':<18} {'Δwall%':>8} {'Δthpt%':>8} "
          f"{'welch_t(thpt)':>14} {'hash_consistent':>16}")
    for wl in workloads:
        p = by_key.get(("pristine", wl), [])
        m = by_key.get(("m6", wl), [])
        if not p or not m:
            continue
        pw = [r["wall_time_sec"] for r in p]
        mw = [r["wall_time_sec"] for r in m]
        pt = [r["throughput_blocks_per_sec"] for r in p]
        mt = [r["throughput_blocks_per_sec"] for r in m]
        dw = (statistics.mean(mw) - statistics.mean(pw)) / statistics.mean(pw) * 100
        dt = (statistics.mean(mt) - statistics.mean(pt)) / statistics.mean(pt) * 100
        t, df = welch_t(mt, pt)
        hc = len(hashes[(wl,)]) == 1
        print(
            f"{wl:<18} {dw:>+8.2f} {dt:>+8.2f} "
            f"{t:>+8.3f} (df={df:>4.1f}) {'yes' if hc else 'NO':>16}"
        )

    print()
    print("hashes per workload:")
    for wl in workloads:
        hs = sorted(hashes[(wl,)])
        print(f"  {wl}: {len(hs)} unique")
        for h in hs:
            print(f"    {h}")


def main():
    path = sys.argv[1] if len(sys.argv) > 1 else "results/optimization_perf.jsonl"
    by_key, hashes = load(path)
    summarize(by_key, hashes)


if __name__ == "__main__":
    main()
