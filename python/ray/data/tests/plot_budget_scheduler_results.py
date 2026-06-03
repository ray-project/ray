"""Parse test output and plot budget vs default scheduler comparison.

Usage:
    python -m pytest test_budget_scheduler_integration.py -vs 2>&1 | tee results.log
    python plot_budget_scheduler_results.py results.log
"""

import re
import sys
from collections import defaultdict

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np


def parse_results(log_path):
    """Parse pytest output into {test_name: {"budget": [times], "default": [times]}}."""
    results = defaultdict(lambda: {"budget": [], "default": []})
    current_test = None
    current_variant = None

    with open(log_path) as f:
        for line in f:
            # Match test start: "test_name[budget-...] PASSED" or the verbose line
            m = re.match(r".*::(\w+)\[(\w+)(?:-(.+))?\]\s", line)
            if m:
                test_name = m.group(1)
                variant = m.group(2)  # "budget" or "default"
                params = m.group(3)  # e.g. "10" or None
                label = f"{test_name}" + (f"[{params}]" if params else "")
                current_test = label
                current_variant = variant

            # Match timing lines containing "time=1.23s"
            m = re.search(r"time=(\d+\.\d+)s", line)
            if m and current_test and current_variant:
                t = float(m.group(1))
                results[current_test][current_variant].append(t)

    return dict(results)


def plot_results(results, log_path="results.log"):
    tests = sorted(results.keys())
    n = len(tests)

    budget_medians = []
    budget_lo = []
    budget_hi = []
    default_medians = []
    default_lo = []
    default_hi = []

    for test in tests:
        for label, meds, lo, hi in [
            ("budget", budget_medians, budget_lo, budget_hi),
            ("default", default_medians, default_lo, default_hi),
        ]:
            times = results[test].get(label, [])
            if times:
                median = float(np.median(times))
                meds.append(median)
                lo.append(median - min(times))
                hi.append(max(times) - median)
            else:
                meds.append(0)
                lo.append(0)
                hi.append(0)

    x = np.arange(n)
    width = 0.35

    fig, ax = plt.subplots(figsize=(max(10, n * 1.2), 6))

    ax.bar(
        x - width / 2,
        budget_medians,
        width,
        yerr=[budget_lo, budget_hi],
        label="Budget Scheduler",
        color="#4C72B0",
        capsize=4,
    )
    ax.bar(
        x + width / 2,
        default_medians,
        width,
        yerr=[default_lo, default_hi],
        label="Default Scheduler",
        color="#DD8452",
        capsize=4,
    )

    ax.set_ylabel("Time (seconds)")
    ax.set_title("Budget Scheduler vs Default Scheduler (median, min/max error bars)")
    ax.set_xticks(x)
    short_labels = [t.replace("test_", "") for t in tests]
    ax.set_xticklabels(short_labels, rotation=30, ha="right", fontsize=9)
    ax.legend()

    # Add speedup annotations
    for i, test in enumerate(tests):
        b = budget_medians[i]
        d = default_medians[i]
        if b > 0 and d > 0:
            speedup = d / b
            y = max(b, d) + max(budget_hi[i], default_hi[i]) + 0.1
            ax.text(i, y, f"{speedup:.2f}x", ha="center", va="bottom", fontsize=8)

    fig.tight_layout()
    out_path = (
        log_path.replace(".log", ".png") if log_path.endswith(".log") else "results.png"
    )
    fig.savefig(out_path, dpi=150)
    print(f"Saved plot to {out_path}")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print(f"Usage: python {sys.argv[0]} <results.log>")
        sys.exit(1)
    results = parse_results(sys.argv[1])
    if not results:
        print("No results found in log file.")
        sys.exit(1)
    print(f"Parsed {len(results)} tests:")
    for test, data in sorted(results.items()):
        for variant in ["budget", "default"]:
            times = data[variant]
            if times:
                print(
                    f"  {test} [{variant}]: median={np.median(times):.2f}s min={min(times):.2f}s max={max(times):.2f}s"
                )
    plot_results(results, sys.argv[1])
