#!/usr/bin/env python
"""Render the three-variant async RL rollout comparison."""

from __future__ import annotations

import argparse
import csv
import json
import math
from pathlib import Path
from typing import Any, Callable

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

VARIANT_ORDER = ("pure-kv-cache", "session-affinity", "kv-token-aware")
BASELINE = "session-affinity"
STYLE = {
    "session-affinity": {"color": "#6495ED", "label": "ConsistentHash\nRouter"},
    "pure-kv-cache": {
        "color": "#B0B8C4",
        "label": "PureKVCacheAffinity\nRouter\n(KV cache overlap only)",
    },
    "kv-token-aware": {
        "color": "#DDA0DD",
        "label": "KVAwareRouter\n(KV cache overlap +\ntoken load)",
    },
}
RC = {
    "figure.facecolor": "#16181D",
    "axes.facecolor": "#20242B",
    "axes.edgecolor": "#C9D1D9",
    "axes.labelcolor": "#F0F6FC",
    "axes.titlecolor": "#F0F6FC",
    "xtick.color": "#D0D7DE",
    "ytick.color": "#D0D7DE",
    "text.color": "#F0F6FC",
}


def number(row: dict[str, str], key: str) -> float:
    try:
        value = float(row[key])
    except (KeyError, TypeError, ValueError) as exc:
        raise SystemExit(f"{row.get('cell_dir')}: missing {key}") from exc
    if not math.isfinite(value):
        raise SystemExit(f"{row.get('cell_dir')}: invalid {key}={value!r}")
    return value


def load_rows(campaign: Path) -> dict[str, dict[str, str]]:
    table = campaign / "analysis" / "cells.csv"
    rows = {row["variant"]: row for row in csv.DictReader(table.open())}
    if set(rows) != set(VARIANT_ORDER):
        raise SystemExit(
            f"expected router variants {VARIANT_ORDER}, got {tuple(sorted(rows))}"
        )
    dag_hashes: set[str] = set()
    for variant, row in rows.items():
        for key in (
            "aiperf_rc",
            "seed_rc",
            "routing_validation_rc",
            "token_validation_rc",
        ):
            if number(row, key) != 0:
                raise SystemExit(f"{variant}: {key} failed")
        if number(row, "response_cache_telemetry_coverage") < 0.99:
            raise SystemExit(f"{variant}: incomplete cache telemetry")
        meta = json.loads((Path(row["cell_dir"]) / "meta.json").read_text())
        dag_hashes.add(str(meta["dag_sha256"]))
        if variant != "session-affinity":
            if number(row, "kv_tracker_present_rate") != 1:
                raise SystemExit(f"{variant}: missing KVAwareRouter tracker")
            if number(row, "kv_tokenized_route_rate") != 1:
                raise SystemExit(f"{variant}: KVAwareRouter fallback was used")
        scoring_env = meta.get("router_scoring_env") or {}
        if (
            variant == "pure-kv-cache"
            and scoring_env.get("DYN_ROUTER_CACHE_AFFINITY_ONLY") != "1"
        ):
            raise SystemExit(
                "pure-kv-cache variant did not enable the demonstration scorer"
            )
        if variant == "kv-token-aware" and scoring_env.get(
            "DYN_ROUTER_CACHE_AFFINITY_ONLY"
        ):
            raise SystemExit(
                "kv-token-aware variant unexpectedly used the demonstration scorer"
            )
    if len(dag_hashes) != 1:
        raise SystemExit("router variants did not use the same calibrated workload")
    return rows


def draw_panel(
    axis: Any,
    rows: dict[str, dict[str, str]],
    *,
    key: str,
    title: str,
    ylabel: str,
    scale: float,
    formatter: Callable[[float], str],
    percentage_points: bool = False,
) -> None:
    values = [number(rows[variant], key) * scale for variant in VARIANT_ORDER]
    baseline = values[VARIANT_ORDER.index(BASELINE)]
    for position, (variant, value) in enumerate(zip(VARIANT_ORDER, values)):
        bar = axis.bar(
            position,
            value,
            color=STYLE[variant]["color"],
            width=0.62,
            edgecolor="#C9D1D9",
            linewidth=0.5,
            zorder=3,
        )[0]
        label = formatter(value)
        if variant != BASELINE:
            delta = (
                value - baseline
                if percentage_points
                else (value / baseline - 1.0) * 100.0
            )
            suffix = " pp" if percentage_points else "%"
            label += f"\n({delta:+.1f}{suffix})"
        axis.annotate(
            label,
            xy=(bar.get_x() + bar.get_width() / 2, bar.get_height()),
            xytext=(0, 5),
            textcoords="offset points",
            ha="center",
            va="bottom",
            color=STYLE[variant]["color"],
            fontsize=9,
            fontweight="semibold",
        )
    axis.set(
        title=title,
        ylabel=ylabel,
        xticks=range(len(VARIANT_ORDER)),
        xticklabels=[STYLE[variant]["label"] for variant in VARIANT_ORDER],
        xlim=(-0.5, len(VARIANT_ORDER) - 0.5),
        ylim=(0, max(values) * 1.3),
    )
    axis.tick_params(axis="x", labelsize=9)
    axis.grid(axis="y", color="#6E7681", alpha=0.35, zorder=0)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--campaign", required=True, type=Path)
    parser.add_argument("--out", required=True, type=Path)
    args = parser.parse_args()
    if args.out.exists():
        raise SystemExit(f"refusing to overwrite {args.out}")
    rows = load_rows(args.campaign)
    args.out.parent.mkdir(parents=True, exist_ok=True)

    plt.rcParams.update(RC)
    figure, axes = plt.subplots(1, 3, figsize=(18, 5.5))
    figure.subplots_adjust(left=0.06, right=0.965, bottom=0.25, top=0.72, wspace=0.38)
    draw_panel(
        axes[0],
        rows,
        key="rollout_e2e_p99_ms",
        title="p99 rollout end-to-end latency (lower is better)",
        ylabel="p99 rollout E2E latency (s)",
        scale=1 / 1000,
        formatter=lambda value: f"{value:.1f}s",
    )
    draw_panel(
        axes[1],
        rows,
        key="response_cached_prompt_fraction",
        title="Prefix cache hit rate",
        ylabel="Prefix cache hit rate (%)",
        scale=100,
        formatter=lambda value: f"{value:.1f}%",
        percentage_points=True,
    )
    draw_panel(
        axes[2],
        rows,
        key="reconstructed_decode_blocks_cv_mean",
        title="Mean active decode-block load CV (lower is better)",
        ylabel="Mean active decode-block load CV",
        scale=1,
        formatter=lambda value: f"{value:.3f}",
    )
    figure.suptitle("Async multi-turn RL rollouts", fontsize=16, y=0.98)
    figure.text(
        0.5,
        0.90,
        "H100 | gpt-oss-120b | 4 replicas × TP=2 | 8 rollouts × 10 turns × 10 steps | "
        "OSL=1K; 2 final-turn stragglers/step (OSL=8K) | C=16",
        ha="center",
        fontsize=10,
    )
    figure.savefig(
        args.out, dpi=180, facecolor=figure.get_facecolor(), bbox_inches="tight"
    )
    plt.close(figure)


if __name__ == "__main__":
    main()
