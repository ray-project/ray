"""ITL distribution shape per arm: near-zero (coalesced reads) fraction, p50 excluding them.

Usage: python itl_shape.py RUN_DIR [base_arm other_arm ...]
"""
import glob
import gzip
import json
import sys
from pathlib import Path
import numpy as np


def itls(run, skip=0.2):
    reqs, firsts, lasts = [], [], []
    for f in sorted(glob.glob(f"{run}/client[0-9]*.json*")):
        d = json.load(gzip.open(f, "rt") if f.endswith(".gz") else open(f))
        firsts.append(min(d["start_times"]))
        lasts.append(max(d["start_times"]))
        reqs += [
            (s, i) for s, i, e in zip(d["start_times"], d["itls"], d["errors"]) if not e
        ]
    t0, t1 = max(firsts), min(lasts)
    lo, hi = t0 + skip * (t1 - t0), t1 - skip * (t1 - t0)
    return np.concatenate([np.array(i) for s, i in reqs if lo <= s <= hi]) * 1e3


root = Path(sys.argv[1])
arms = sys.argv[2:] or ["copies", "on", "off"]
rows = {a: [] for a in arms}
for c in range(100):
    if not all((root / f"r36_c256_{a}_{c}").exists() for a in arms):
        break
    for a in arms:
        x = itls(root / f"r36_c256_{a}_{c}")
        z = x < 0.3
        rows[a].append(
            [
                z.mean() * 100,
                np.percentile(x, 50),
                np.percentile(x[~z], 50),
                np.mean(x[~z]),
                np.mean((x > 4) & (x < 8)) * 100,
                np.mean(x >= 8) * 100,
            ]
        )
cols = ["%<0.3ms", "p50", "p50(>0.3)", "mean(>0.3)", "%4-8ms", "%>=8ms"]
for a in arms:
    m = np.array(rows[a]).mean(0)
    print(f"{a:7s}", "  ".join(f"{c} {v:.3f}" for c, v in zip(cols, m)))
base = np.array(rows[arms[0]])
for a in arms[1:]:
    d = np.array(rows[a]) - base
    print(
        f"{a}-{arms[0]} paired mean diff:",
        "  ".join(f"{c} {v:+.3f}" for c, v in zip(cols, d.mean(0))),
    )
