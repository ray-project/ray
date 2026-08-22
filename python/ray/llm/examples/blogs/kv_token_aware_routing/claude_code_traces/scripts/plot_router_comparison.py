#!/usr/bin/env python3
"""Plot the CC router comparison from validated cell summaries."""

from __future__ import annotations

import argparse
import csv
from collections import defaultdict
from pathlib import Path
from typing import Any

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt

ROUTER_VARIANT_STYLE = {
    "session-affinity": {"color": "#6495ED", "marker": "o", "label": "ConsistentHashRouter"},
    "kv-token-aware-balanced": {
        "color": "#8ee78c",
        "marker": "s",
        "label": "KVAwareRouter (balanced)",
    },
    "kv-token-aware-kv-biased": {
        "color": "#dda0dd",
        "marker": "D",
        "label": "KVAwareRouter (cache biased)",
    },
}
RCPARAMS = {
    "figure.facecolor": "#16181d",
    "axes.facecolor": "#20242b",
    "axes.edgecolor": "#c9d1d9",
    "axes.labelcolor": "#f0f6fc",
    "axes.titlecolor": "#f0f6fc",
    "xtick.color": "#d0d7de",
    "ytick.color": "#d0d7de",
    "text.color": "#f0f6fc",
}
PANELS = (
    ("ttft_p90_ms", "p90 TTFT (lower is better)", "p90 TTFT (ms)", ".0f"),
    ("tpot_p90_ms", "p90 TPOT (lower is better)", "p90 TPOT (ms/token)", ".2f"),
    (
        "output_tok_s_per_gpu",
        "Output throughput (higher is better)",
        "Output throughput (tokens/s/GPU)",
        ".1f",
    ),
    (
        "active_decode_blocks_cv",
        "Mean active decode load CV (lower is better)",
        "Mean active decode-block load CV",
        ".2f",
    ),
    (
        "prefix_cache_hit_rate",
        "Prefix cache hit rate",
        "Cached prompt-token fraction",
        ".1%",
    ),
)


def load_rows(path: Path) -> list[dict[str, Any]]:
    with path.open() as file:
        rows = list(csv.DictReader(file))
    if len(rows) != 15:
        raise ValueError(f"expected 15 validated cells, found {len(rows)}")
    return [
        {
            key: float(value) if key not in {"router_variant"} else value
            for key, value in row.items()
        }
        for row in rows
    ]


def annotate_values(axis: Any, points: list[tuple[float, float, str, str]]) -> None:
    """Place compact color-matched labels without collisions."""

    figure = axis.figure
    figure.canvas.draw()
    renderer = figure.canvas.get_renderer()
    occupied: list[Any] = []
    offsets = ((6, 11), (6, -15), (-27, 11), (-27, -15), (6, 27), (-27, 27), (6, -31), (-27, -31))
    for x_value, y_value, text, color in sorted(points, key=lambda point: (point[0], -point[1])):
        for x_offset, y_offset in offsets:
            label = axis.annotate(
                text,
                (x_value, y_value),
                xytext=(x_offset, y_offset),
                textcoords="offset points",
                fontsize=7,
                color=color,
                ha="left" if x_offset >= 0 else "right",
                va="center",
                clip_on=False,
                zorder=5,
            )
            figure.canvas.draw()
            box = label.get_window_extent(renderer).expanded(1.08, 1.25)
            if not any(box.overlaps(other) for other in occupied):
                occupied.append(box)
                break
            label.remove()


def annotate_throughput(axis: Any, points: list[tuple[float, float, str, str]]) -> None:
    """Stack throughput labels in ascending order at each concurrency."""

    figure = axis.figure
    figure.canvas.draw()
    groups: dict[float, list[tuple[float, str, str]]] = defaultdict(list)
    for concurrency, value, text, color in points:
        groups[concurrency].append((value, text, color))
    pixels_per_point = figure.dpi / 72
    last_concurrency = max(groups)
    for concurrency, entries in sorted(groups.items()):
        ordered = sorted(entries)
        targets = [axis.transData.transform((concurrency, value))[1] for value, _, _ in ordered]
        stacked = [targets[0]]
        for target in targets[1:]:
            stacked.append(max(target, stacked[-1] + 34))
        shift = sum(targets) / len(targets) - sum(stacked) / len(stacked)
        for (value, text, color), target, position in zip(ordered, targets, stacked):
            at_end = concurrency == last_concurrency
            axis.annotate(
                text,
                (concurrency, value),
                xytext=((-13 if at_end else 13), (position + shift - target) / pixels_per_point),
                textcoords="offset points",
                ha="right" if at_end else "left",
                va="center",
                fontsize=7.2,
                color=color,
                bbox={
                    "boxstyle": "round,pad=0.13",
                    "facecolor": "#20242b",
                    "edgecolor": "none",
                    "alpha": 0.96,
                },
                clip_on=False,
                zorder=6,
            )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cells", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args()

    if args.out.exists():
        raise SystemExit(f"refusing to overwrite plot: {args.out}")
    rows = load_rows(args.cells)
    plt.rcParams.update(RCPARAMS)
    figure = plt.figure(figsize=(18, 9))
    grid = figure.add_gridspec(
        2, 6, left=0.06, right=0.985, top=0.84, bottom=0.11, hspace=0.46, wspace=0.50
    )
    axes = [
        figure.add_subplot(grid[0, 0:2]),
        figure.add_subplot(grid[0, 2:4]),
        figure.add_subplot(grid[0, 4:6]),
        figure.add_subplot(grid[1, 1:3]),
        figure.add_subplot(grid[1, 3:5]),
    ]
    concurrencies = sorted({int(row["concurrency"]) for row in rows})
    by_router_variant = {
        router_variant: sorted(
            (row for row in rows if row["router_variant"] == router_variant),
            key=lambda row: row["concurrency"],
        )
        for router_variant in ROUTER_VARIANT_STYLE
    }

    for axis, (metric, title, ylabel, value_format) in zip(axes, PANELS):
        labels: list[tuple[float, float, str, str]] = []
        baseline = {
            int(row["concurrency"]): float(row[metric])
            for row in by_router_variant["session-affinity"]
        }
        for router_variant, style in ROUTER_VARIANT_STYLE.items():
            variant_rows = by_router_variant[router_variant]
            x_values = [int(row["concurrency"]) for row in variant_rows]
            y_values = [float(row[metric]) for row in variant_rows]
            axis.plot(
                x_values,
                y_values,
                color=style["color"],
                marker=style["marker"],
                markersize=7,
                lw=2.1,
                label=style["label"],
                zorder=3,
            )
            for x_value, y_value in zip(x_values, y_values):
                text = format(y_value, value_format)
                if router_variant != "session-affinity":
                    text = f"{text}\n({(y_value / baseline[x_value] - 1) * 100:+.1f}%)"
                labels.append((x_value, y_value, text, style["color"]))
        axis.margins(x=0.08, y=0.18)
        (annotate_throughput if metric == "output_tok_s_per_gpu" else annotate_values)(axis, labels)
        axis.set(title=title, xlabel="Concurrency", ylabel=ylabel)
        axis.set_xticks(concurrencies)
        axis.grid(True, color="#6e7681", alpha=0.45)
    axes[0].legend(
        loc="upper left", fontsize=9, facecolor="#20242b", edgecolor="#8b949e", labelcolor="#f0f6fc"
    )
    figure.suptitle(
        "Router performance vs. concurrency on Weka Claude-Code traces", fontsize=16, y=0.975
    )
    figure.text(
        0.5,
        0.905,
        "gpt-oss-120b | 4 replicas × TP=2 | 120K context cap | GPU KV cache only",
        ha="center",
        fontsize=10,
    )
    args.out.parent.mkdir(parents=True, exist_ok=True)
    figure.savefig(args.out, dpi=180, facecolor=figure.get_facecolor())
    plt.close(figure)
    print(f"[plot] wrote {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
