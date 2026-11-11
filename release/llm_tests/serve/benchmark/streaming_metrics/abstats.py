"""Paired statistics for abtest.py output: median, mean and a bootstrap 95%
confidence interval of the mean paired delta, per load point and metric.

Disturbed cycles: at a fixed load point TPOT is engine-bound and the arms differ
by ~1-2%, while node-wide disturbances (seen as both replicas slowing together,
in arms with and without the observer) raise one run's TPOT by 20%+. A cycle with
any run whose mean TPOT exceeds the point's all-run median by more than 10% is
reported, and excluded in the second ("clean") table.

Several RUN_DIRs (separate suites on the same deployment) are pooled: each
suite's cycles stay paired within that suite, and are keyed by (suite, cycle).

Usage: python abstats.py RUN_DIR [RUN_DIR ...]
"""
import re
import sys
from pathlib import Path

import numpy as np

sys.path.insert(0, str(Path(__file__).parent))
from bench import analyze_dir  # noqa: E402

# itl_ms is vllm bench's per-chunk ITL (gap between SSE events); it moves with
# chunks_per_token when the frontend coalesces tokens. tpot_ms is per token.
METRICS = [
    ("ttft_ms", "mean"),
    ("ttft_ms", "p50"),
    ("ttft_ms", "p90"),
    ("tpot_ms", "mean"),
    ("tpot_ms", "p90"),
    ("itl_ms", "mean"),
    ("itl_ms", "p50"),
    ("itl_ms", "p99"),
    ("chunks_per_token", None),
    ("e2e_s", "mean"),
]


def value(run, m, q):
    return run[m] if q is None else run[m][q]


def main(roots):
    runs = {}
    for suite, root in enumerate(map(Path, roots)):
        for d in sorted(root.iterdir()):
            m = re.match(
                r"(r\d+_c\d+)_(on|off|copies|py|hap|ong|offg|pyg|placebo)_(\d+)$",
                d.name,
            )
            if m and (
                (d / "client0.json").exists() or (d / "client0.json.gz").exists()
            ):
                cycle = int(m.group(3)) if len(roots) == 1 else (suite, int(m.group(3)))
                runs.setdefault(m.group(1), {}).setdefault(m.group(2), {})[
                    cycle
                ] = analyze_dir(d)
    rng = np.random.default_rng(0)
    for point, modes in runs.items():
        tpot = {
            (arm, c): r["tpot_ms"]["mean"]
            for arm, rs in modes.items()
            for c, r in rs.items()
        }
        median = np.median(list(tpot.values()))
        disturbed = {c for (arm, c), v in tpot.items() if v > 1.10 * median}
        print(
            f"\n##### {point}: median TPOT {median:.3f} ms; disturbed runs: "
            + (
                ", ".join(
                    f"{arm}_{c} ({v / median - 1:+.0%})"
                    for (arm, c), v in sorted(tpot.items())
                    if c in disturbed and v > 1.10 * median
                )
                or "none"
            )
        )
        for label, skip in (("all cycles", set()), ("clean cycles", disturbed)):
            if label == "clean cycles" and not disturbed:
                continue
            for arm, base in (
                ("on", "off"),
                ("on", "copies"),
                ("on", "py"),
                ("py", "copies"),
                ("ong", "off"),
                ("ong", "offg"),
                ("ong", "on"),
                ("offg", "off"),
                ("ong", "pyg"),
                ("pyg", "py"),
                ("on", "placebo"),
                ("placebo", "copies"),
            ):
                report(point, modes, arm, base, rng, skip, label)


def report(point, modes, arm, base, rng, skip=frozenset(), label="all cycles"):
    cycles = sorted((set(modes.get(arm, {})) & set(modes.get(base, {}))) - skip)
    if not cycles:
        return
    print(
        f"\n=== {point}: {len(cycles)} paired cycles, {label} ({arm.upper()} vs {base.upper()}, %) ==="
    )
    for m, q in METRICS:
        deltas = np.array(
            [
                (value(modes[arm][c], m, q) / value(modes[base][c], m, q) - 1) * 100
                for c in cycles
            ]
        )
        boot = [rng.choice(deltas, len(deltas)).mean() for _ in range(10000)]
        lo, hi = np.percentile(boot, [2.5, 97.5])
        print(
            f"  {m}.{q or '':5s} median {np.median(deltas):+6.2f}  mean {deltas.mean():+6.2f}  "
            f"95% CI [{lo:+6.2f}, {hi:+6.2f}]  per-cycle "
            + " ".join(f"{x:+.1f}" for x in deltas)
        )
    if arm not in ("on", "ong"):
        return
    ok = all(
        modes[arm][c]["haproxy"]["ttft_count"]
        == modes[arm][c]["haproxy"]["client_requests"]
        and modes[arm][c]["haproxy"]["errors"] == 0
        and 0
        <= modes[arm][c]["haproxy"]["client_itl_tokens"]
        - modes[arm][c]["haproxy"]["itl_count"]
        <= 0.001 * modes[arm][c]["haproxy"]["client_itl_tokens"]
        for c in cycles
    )
    print(f"  ON correctness checks: {'all OK' if ok else 'MISMATCH'}")


if __name__ == "__main__":
    main(sys.argv[1:])
