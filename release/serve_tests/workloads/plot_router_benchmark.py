#!/usr/bin/env python3
"""
Plot router benchmark results from router_microbenchmark.py.

Produces multi-scale plots:
  1. Bar plot: p50 throughput by replica count, grouped by router
  2. Bar plot: p50 latency by replica count, grouped by router

Example:
    python plot_router_benchmark.py results.json -o /tmp/plots
"""

import argparse
import json
import os
import re
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd


ALGO_NAMES = {
    "pow2": "Power of Two",
    "capacity_queue": "CapacityQueue",
    "consistent_hash": "Consistent Hash",
}
ALGO_COLORS = {
    "Power of Two": "lightsteelblue",
    "CapacityQueue": "plum",
    "Consistent Hash": "plum",
}
# Ordered: pow2 first (left), alternatives to the right.
ROUTER_ORDER = ["Power of Two", "Consistent Hash", "CapacityQueue"]


def load_results(path: str) -> dict:
    with open(path) as f:
        data = json.load(f)
    if isinstance(data, list):
        return {"perf_metrics": data, "utilization_raw": {}}
    return {
        "perf_metrics": data.get("perf_metrics", []),
        "utilization_raw": data.get("utilization_raw", {}),
    }


def _parse_metric(name: str):
    """Parse e.g. router_pow2_128_p50_throughput_rps -> (pow2, 128, p50_throughput_rps)."""
    m = re.match(r"router_(\w+?)_(\d+)_(.+)", name)
    if not m:
        return None
    return m.group(1), int(m.group(2)), m.group(3)


def _parse_util_key(key: str):
    """Parse e.g. router_pow2_128 -> (pow2, 128)."""
    m = re.match(r"router_(\w+?)_(\d+)$", key)
    if not m:
        return None
    return m.group(1), int(m.group(2))


def build_metric_df(json_paths: list) -> pd.DataFrame:
    rows = []
    for path in json_paths:
        label = Path(path).stem
        data = load_results(path)
        for m in data["perf_metrics"]:
            parsed = _parse_metric(m["perf_metric_name"])
            if not parsed:
                continue
            router, replicas, kind = parsed
            rows.append(
                {
                    "file": label,
                    "router": ALGO_NAMES.get(router, router),
                    "replicas": replicas,
                    "kind": kind,
                    "value": float(m["perf_metric_value"]),
                }
            )
    return pd.DataFrame(rows)


def build_util_df(json_paths: list) -> pd.DataFrame:
    rows = []
    for path in json_paths:
        label = Path(path).stem
        data = load_results(path)
        for key, values in data.get("utilization_raw", {}).items():
            parsed = _parse_util_key(key)
            if not parsed:
                continue
            router, replicas = parsed
            for v in values:
                rows.append(
                    {
                        "file": label,
                        "router": ALGO_NAMES.get(router, router),
                        "replicas": replicas,
                        "utilization": float(v),
                    }
                )
    return pd.DataFrame(rows)


def _get_delta_vs_pow2(df, kind, replicas_list, compare_router):
    """Return compare_router deltas vs Power of Two for each scale."""
    deltas = []
    for r in replicas_list:
        pow2_row = df[
            (df["replicas"] == r)
            & (df["router"] == "Power of Two")
            & (df["kind"] == kind)
        ]
        compare_row = df[
            (df["replicas"] == r)
            & (df["router"] == compare_router)
            & (df["kind"] == kind)
        ]
        if pow2_row.empty or compare_row.empty:
            deltas.append(None)
            continue
        v_pow2 = pow2_row["value"].values[0]
        v_compare = compare_row["value"].values[0]
        if v_pow2 == 0:
            deltas.append(None)
            continue
        deltas.append((v_compare - v_pow2) / v_pow2 * 100)
    return deltas


def _bar_plot(ax, df, kind, ylabel, title, legend_loc="best"):
    """Grouped bar: x=replicas, hue=router."""
    subset = df[df["kind"] == kind].copy()
    if subset.empty:
        return

    replicas_list = sorted(subset["replicas"].unique())
    routers = [r for r in ROUTER_ORDER if r in subset["router"].values]
    x = np.arange(len(replicas_list))
    width = 0.42 / len(routers)

    bar_positions = {}
    for i, router in enumerate(routers):
        vals = []
        for r in replicas_list:
            row = subset[(subset["replicas"] == r) & (subset["router"] == router)]
            vals.append(row["value"].values[0] if not row.empty else 0)
        offset = (i - (len(routers) - 1) / 2) * width
        ax.bar(
            x + offset,
            vals,
            width,
            label=router,
            color=ALGO_COLORS.get(router),
            edgecolor="gray",
            linewidth=0.5,
        )
        for j, v in enumerate(vals):
            ax.text(
                x[j] + offset,
                v,
                f"{v:.1f}",
                ha="center",
                va="bottom",
                fontsize=7,
            )
        bar_positions[router] = (x + offset, vals)

    ax.set_xticks(x)
    ax.set_xticklabels(replicas_list)
    ax.set_xlabel("Replicas")
    ax.set_ylabel(ylabel)
    ax.set_title(title)
    legend = ax.legend(
        title="Router", loc=legend_loc, frameon=True, fancybox=False, edgecolor="gray"
    )
    legend.get_frame().set_facecolor("white")
    legend.get_frame().set_alpha(1.0)
    legend.set_zorder(10)

    # Annotate each non-pow2 router's delta against pow2.
    ymax = max(max(v for v in vals) for _, vals in bar_positions.values())
    label_offset = ymax * 0.05  # gap between value label and improvement %
    for compare_router in [r for r in routers if r != "Power of Two"]:
        deltas = _get_delta_vs_pow2(df, kind, replicas_list, compare_router)
        positions, vals = bar_positions.get(
            compare_router, (x, [0] * len(replicas_list))
        )
        for j, pct in enumerate(deltas):
            if pct is None:
                continue
            sign = "+" if pct >= 0 else ""
            is_good = (kind == "p50_throughput_rps" and pct > 0) or (
                kind == "p50_latency_ms" and pct < 0
            )
            ax.text(
                positions[j],
                vals[j] + label_offset,
                f"{sign}{pct:.1f}%",
                ha="center",
                va="bottom",
                fontsize=7,
                fontweight="bold",
                color="green" if is_good else "red",
            )

    ax.set_ylim(top=ymax * 1.2)


def _util_box_plot(ax, util_df):
    """Real box plot from per-replica utilization data (pow2 left, CQ right)."""
    if util_df.empty:
        return

    replicas_list = sorted(util_df["replicas"].unique())
    routers = [r for r in ROUTER_ORDER if r in util_df["router"].values]
    n_routers = len(routers)
    n_scales = len(replicas_list)
    width = 0.6 / n_routers

    positions_map = {}
    for i, router in enumerate(routers):
        offset = (i - (n_routers - 1) / 2) * width
        for j, r in enumerate(replicas_list):
            data = util_df[(util_df["replicas"] == r) & (util_df["router"] == router)][
                "utilization"
            ].values
            if len(data) == 0:
                continue
            pos = j + offset
            bp = ax.boxplot(
                data,
                positions=[pos],
                widths=width * 0.8,
                patch_artist=True,
                showfliers=False,
                medianprops={"color": "black", "linewidth": 1.5},
            )
            color = ALGO_COLORS.get(router, "#999999")
            for patch in bp["boxes"]:
                patch.set_facecolor(color)
                patch.set_alpha(0.8)
                patch.set_edgecolor("gray")
            positions_map[router] = color

    ax.axhline(y=1.0, color="gray", linestyle="--", alpha=0.5)
    ax.set_xticks(range(n_scales))
    ax.set_xticklabels(replicas_list)
    ax.set_xlabel("Replicas")
    ax.set_ylabel("Utilization")
    ax.set_title("Per-Replica Utilization Distribution")
    ax.set_ylim(0.7, 1.05)

    handles = [
        plt.Rectangle((0, 0), 1, 1, facecolor=positions_map[r], alpha=0.8)
        for r in routers
        if r in positions_map
    ]
    ax.legend(handles, [r for r in routers if r in positions_map], title="Router")


def _util_fallback_plot(ax, metric_df):
    """Fallback: bar + error bars from p25/p50/p75 when raw data is missing."""
    util_kinds = ["p25_utilization", "p50_utilization", "p75_utilization"]
    subset = metric_df[metric_df["kind"].isin(util_kinds)].copy()
    if subset.empty:
        return

    replicas_list = sorted(subset["replicas"].unique())
    routers = [r for r in ROUTER_ORDER if r in subset["router"].values]
    x = np.arange(len(replicas_list))
    width = 0.6 / len(routers)

    for i, router in enumerate(routers):
        medians, lows, highs = [], [], []
        for r in replicas_list:
            rs = subset[(subset["replicas"] == r) & (subset["router"] == router)]
            p25 = rs[rs["kind"] == "p25_utilization"]["value"]
            p50 = rs[rs["kind"] == "p50_utilization"]["value"]
            p75 = rs[rs["kind"] == "p75_utilization"]["value"]
            medians.append(p50.values[0] if not p50.empty else 0)
            lows.append(p25.values[0] if not p25.empty else 0)
            highs.append(p75.values[0] if not p75.empty else 0)

        medians = np.array(medians)
        lows = np.array(lows)
        highs = np.array(highs)
        offset = (i - (len(routers) - 1) / 2) * width
        ax.bar(
            x + offset,
            medians,
            width,
            label=router,
            color=ALGO_COLORS.get(router),
            alpha=0.8,
        )
        ax.errorbar(
            x + offset,
            medians,
            yerr=[medians - lows, highs - medians],
            fmt="none",
            color="black",
            capsize=4,
            capthick=1,
        )

    ax.axhline(y=1.0, color="gray", linestyle="--", alpha=0.5)
    ax.set_xticks(x)
    ax.set_xticklabels(replicas_list)
    ax.set_xlabel("Replicas")
    ax.set_ylabel("Utilization")
    ax.set_title("Utilization (p25 / p50 / p75)")
    ax.legend(title="Router")
    ax.set_ylim(0.7, 1.05)


def plot_metric(metric_df, output_dir, kind, ylabel, title, filename, suffix=""):
    """Produce one multi-scale figure for a single metric."""
    plt.style.use("seaborn-v0_8-whitegrid")
    fig, ax = plt.subplots(figsize=(10, 5))
    _bar_plot(ax, metric_df, kind, ylabel, title)
    plt.tight_layout()
    path = output_dir / f"{filename}{suffix}.png"
    fig.savefig(path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"Saved {path}")


def plot_results(metric_df, output_dir, suffix=""):
    """Produce throughput and latency figures with all scales on each plot."""
    plot_metric(
        metric_df,
        output_dir,
        "p50_throughput_rps",
        "Throughput (req/s)",
        "P50 Throughput",
        "p50_throughput",
        suffix=suffix,
    )
    plot_metric(
        metric_df,
        output_dir,
        "p50_latency_ms",
        "Latency (ms)",
        "P50 Latency",
        "p50_latency",
        suffix=suffix,
    )


def _print_delta_table(df, baseline, compare):
    """Print comparison table between two runs."""
    b = df[df["file"] == baseline]
    c = df[df["file"] == compare]

    print(f"\n--- Delta: {compare} vs {baseline} ---")
    header = f"{'Router':<18} {'Replicas':>8} {'Metric':<22} {'Base':>10} {'New':>10} {'Delta':>8}"
    print(header)
    print("-" * len(header))

    for kind in ["p50_throughput_rps", "p50_latency_ms", "p50_utilization"]:
        for _, rb in b[b["kind"] == kind].iterrows():
            rc = c[
                (c["kind"] == kind)
                & (c["router"] == rb["router"])
                & (c["replicas"] == rb["replicas"])
            ]
            if rc.empty:
                continue
            vb, vc = rb["value"], rc.iloc[0]["value"]
            delta = (vc - vb) / vb * 100 if vb else 0
            print(
                f"{rb['router']:<18} {rb['replicas']:>8} "
                f"{kind:<22} {vb:>10.2f} {vc:>10.2f} {delta:>+7.1f}%"
            )


def main():
    parser = argparse.ArgumentParser(description="Plot router benchmark results.")
    parser.add_argument(
        "json_files",
        nargs="+",
        help="One or more JSON files from router_microbenchmark.py",
    )
    parser.add_argument(
        "-o",
        "--output-dir",
        default=".",
        help="Output directory for plots (default: current directory)",
    )
    args = parser.parse_args()

    for p in args.json_files:
        if not os.path.isfile(p):
            raise FileNotFoundError(f"File not found: {p}")

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    metric_df = build_metric_df(args.json_files)
    if metric_df.empty:
        raise ValueError("No valid router metrics found.")

    if len(args.json_files) == 1:
        plot_results(metric_df, output_dir)
    else:
        files = sorted(metric_df["file"].unique())
        for f in files:
            plot_results(
                metric_df[metric_df["file"] == f],
                output_dir,
                suffix=f"_{f}",
            )
        if len(files) == 2:
            _print_delta_table(metric_df, files[0], files[1])


if __name__ == "__main__":
    main()
