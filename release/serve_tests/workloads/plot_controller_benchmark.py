#!/usr/bin/env python3
"""
Plot controller benchmark results from microbenchmarks.py --run-controller.

Takes one or more JSON files (output from microbenchmarks.py --run-controller)
and produces seaborn FacetGrid plots for each metric, with file names as labels
and std for confidence intervals.

When 2+ files are provided, runs a t-test (first vs last file) to assess
statistical significance of changes.

Dependencies: matplotlib, pandas, seaborn, scipy

Example usage with the master.json and pydantic.json files:
    python release/serve_tests/workloads/plot_controller_benchmark.py \
        release/serve_tests/workloads/master.json  \
        release/serve_tests/workloads/pydantic.json \
        -o /tmp/controller_plot.png
"""

import argparse
import json
import os
import re
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
from scipy import stats


def load_benchmark_json(path: str) -> list[dict]:
    """Load perf_metrics from a controller benchmark JSON file.

    Supports both formats:
    - {"perf_metrics": [...]}  (from save_test_results)
    - [...]  (raw list of perf metric dicts)
    """
    with open(path) as f:
        data = json.load(f)
    if isinstance(data, list):
        perf_metrics = data
    else:
        perf_metrics = data.get("perf_metrics", [])
    if not perf_metrics:
        raise ValueError(f"No perf_metrics found in {path}")
    return perf_metrics


def _parse_replicas_from_metric_name(name: str) -> int | None:
    """Extract replica count from metric name, e.g. controller_foo_10_replicas -> 10."""
    m = re.search(r"_(\d+)_replicas$", name)
    return int(m.group(1)) if m else None


def build_plot_dataframe(json_paths: list[str]) -> pd.DataFrame:
    """Build a DataFrame for plotting from multiple JSON files."""
    rows = []
    for path in json_paths:
        label = Path(path).stem
        metrics = load_benchmark_json(path)
        for m in metrics:
            name = m.get("perf_metric_name")
            value = m.get("perf_metric_value")
            std = m.get("perf_metric_std", 0.0)
            n = m.get("perf_metric_sample_size", 1)
            if name is not None and value is not None:
                replicas = _parse_replicas_from_metric_name(name)
                rows.append(
                    {
                        "metric": name,
                        "base_metric": name.rsplit("_", 2)[0]
                        if replicas is not None
                        else name,
                        "replicas": replicas if replicas is not None else -1,
                        "file": label,
                        "mean": float(value),
                        "std": float(std),
                        "n": int(n),
                    }
                )
    return pd.DataFrame(rows)


def run_ttest(df: pd.DataFrame, first_file: str, last_file: str) -> dict[str, dict]:
    """
    Run independent t-test for each metric between first and last file.
    Returns dict of metric_name -> {statistic, pvalue, significant}.
    """
    results = {}
    for metric in df["metric"].unique():
        m1 = df[(df["metric"] == metric) & (df["file"] == first_file)]
        m2 = df[(df["metric"] == metric) & (df["file"] == last_file)]
        if m1.empty or m2.empty:
            continue
        row1 = m1.iloc[0]
        row2 = m2.iloc[0]
        try:
            stat, pval = stats.ttest_ind_from_stats(
                mean1=row1["mean"],
                std1=row1["std"],
                nobs1=row1["n"],
                mean2=row2["mean"],
                std2=row2["std"],
                nobs2=row2["n"],
            )
            results[metric] = {
                "statistic": float(stat),
                "pvalue": float(pval),
                "significant": pval < 0.05,
            }
        except Exception:
            results[metric] = {"statistic": None, "pvalue": None, "significant": None}
    return results


def plot_facet_grid(df: pd.DataFrame, output_path: str) -> None:
    """Create a seaborn FacetGrid bar plot for each metric with std as CI.

    X-axis: replica count. Hue: file (when multiple files).
    """
    # Use replica count on x-axis; filter out metrics without replica suffix
    plot_df = df[df["replicas"] >= 0].copy()
    if plot_df.empty:
        # Fallback: no replica pattern found, use file on x-axis
        plot_df = df.copy()
        plot_df["replicas"] = plot_df["file"]
        x_var, facet_var = "file", "metric"
    else:
        plot_df["replicas"] = plot_df["replicas"].astype(int)
        x_var, facet_var = "replicas", "base_metric"

    base_metrics = sorted(plot_df[facet_var].unique())
    n_metrics = len(base_metrics)
    col_wrap = min(3, n_metrics)

    replica_order = (
        sorted(plot_df["replicas"].unique()) if x_var == "replicas" else None
    )

    g = sns.FacetGrid(
        plot_df,
        col=facet_var,
        col_wrap=col_wrap,
        col_order=base_metrics,
        sharey=False,
        height=4,
        aspect=1.2,
    )
    has_multiple_files = plot_df["file"].nunique() > 1
    g.map_dataframe(
        sns.barplot,
        x=x_var,
        y="mean",
        hue="file" if has_multiple_files else x_var,
        palette="husl",
        legend=has_multiple_files,
        order=replica_order,
        hue_order=(sorted(plot_df["file"].unique()) if has_multiple_files else None),
    )

    # Add error bars (std as CI) to each facet
    for ax, base_metric in zip(g.axes.flat, base_metrics):
        subset = plot_df[plot_df[facet_var] == base_metric].sort_values(
            ["file", x_var]  # seaborn bar order: hue then x
        )
        # Match bars to data: seaborn containers = one per hue, bars per x
        bars = [bar for c in ax.containers for bar in c]
        for (_, row), bar in zip(subset.iterrows(), bars):
            ax.errorbar(
                bar.get_x() + bar.get_width() / 2,
                row["mean"],
                yerr=row["std"],
                fmt="none",
                color="black",
                capsize=4,
                capthick=1,
            )
        ax.set_xlabel("Replicas" if x_var == "replicas" else "")
        ax.tick_params(axis="x", rotation=0)

    g.set_titles(col_template="{col_name}")
    if plot_df["file"].nunique() > 1:
        g.add_legend(title="File")
    plt.tight_layout()
    plt.savefig(output_path, dpi=150, bbox_inches="tight")
    plt.close()
    print(f"Saved plot to {output_path}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Plot controller benchmark JSON outputs and optionally run t-tests."
    )
    parser.add_argument(
        "json_files",
        nargs="+",
        help="One or more JSON files from microbenchmarks.py --run-controller",
    )
    parser.add_argument(
        "-o",
        "--output",
        default="controller_benchmark_plot.png",
        help="Output plot path (default: controller_benchmark_plot.png)",
    )
    args = parser.parse_args()

    for p in args.json_files:
        if not os.path.isfile(p):
            raise FileNotFoundError(f"File not found: {p}")

    df = build_plot_dataframe(args.json_files)
    if df.empty:
        raise ValueError("No valid metrics found in the provided JSON files.")

    # Plot
    plot_facet_grid(df, args.output)

    # T-test when 2+ files
    if len(args.json_files) >= 2:
        first_label = Path(args.json_files[0]).stem
        last_label = Path(args.json_files[-1]).stem
        ttest_results = run_ttest(df, first_label, last_label)

        # Build table: rows=base_metric, cols=replicas
        table_rows = []
        for full_metric, r in ttest_results.items():
            replicas = _parse_replicas_from_metric_name(full_metric)
            base = (
                full_metric.rsplit("_", 2)[0] if replicas is not None else full_metric
            )
            if replicas is None:
                replicas = -1
            if r["pvalue"] is not None:
                cell = "✓" if r["significant"] else "✗"
            else:
                cell = "—"
            table_rows.append({"base_metric": base, "replicas": replicas, "cell": cell})

        if table_rows:
            ttest_df = pd.DataFrame(table_rows)
            pivot = ttest_df.pivot(
                index="base_metric", columns="replicas", values="cell"
            )
            # Sort columns by replica count
            pivot = pivot.reindex(columns=sorted([c for c in pivot.columns if c >= 0]))
            pivot = pivot.sort_index()

            print("\n--- Statistical significance (t-test: first vs last file) ---")
            print(f"Comparing: {first_label} vs {last_label}")
            print("✓ = significant (p<0.05), ✗ = not significant\n")
            print(pivot.to_string())


if __name__ == "__main__":
    main()
