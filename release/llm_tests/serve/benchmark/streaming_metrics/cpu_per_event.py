"""Replica event-loop and HAProxy CPU per streamed event, per arm, from the
sched.json and component_cpu.json files abtest.py writes.

Usage: python cpu_per_event.py RUN_DIR [RUN_DIR ...] [copies|py]  (paired reference arm)
"""
import collections
import glob
import gzip
import json
import re
import sys
import numpy as np


def chunks(d):
    n = tok = 0
    for f in glob.glob(d + "/client[0-9]*.json*"):
        j = json.load(gzip.open(f, "rt") if f.endswith(".gz") else open(f))
        for itl, ol, err in zip(j["itls"], j["output_lens"], j["errors"]):
            if not err:
                n += len(itl) + 1
                tok += ol
    return n, tok


rows = collections.defaultdict(list)
for root in [a for a in sys.argv[1:] if a not in ("copies", "py")]:
    for d in sorted(glob.glob(root + "/r*_c*_*_[0-9]*")):
        m = re.search(r"_(on|off|copies|py|ong|offg|pyg|placebo)_(\d+)$", d)
        try:
            s = json.load(open(d + "/sched.json"))
            c = json.load(open(d + "/component_cpu.json"))
        except OSError:
            continue
        by_pid = collections.defaultdict(list)
        for r in s["replicas"]:
            by_pid[r["thread"].split(":")[0]].append(r)
        loop = sum(max(v, key=lambda r: r["run_s"])["run_s"] for v in by_pid.values())
        n, tok = chunks(d)
        hap = sum(r["run_s"] for r in s["haproxy"])
        rows[m.group(1)].append(
            (
                int(m.group(2)),
                loop / n * 1e6,
                c["frontend_s"] / n * 1e6,
                hap / n * 1e6,
                n / tok,
                loop,
                n,
            )
        )
ref = sys.argv[-1] if sys.argv[-1] in ("copies", "py") else "copies"
base = {c: v for c, *v in rows.get(ref, [])}
for arm, v in rows.items():
    a = np.array([x[1:] for x in v])
    print(
        f"{arm:7s} n={len(v)} loop_us/event {a[:,0].mean():6.2f} (sd {a[:,0].std():.2f})  process_us/event {a[:,1].mean():6.2f}  haproxy_us/event {a[:,2].mean():5.2f}  events/token {a[:,3].mean():.3f}  loop_s {a[:,4].mean():.1f}"
    )
    if arm != ref and base:
        d = [x[1] - base[x[0]][0] for x in v if x[0] in base]
        if d:
            print(
                f"         paired loop_us/event minus {ref}: mean {np.mean(d):+.2f}  per-cycle "
                + " ".join(f"{x:+.2f}" for x in d)
            )
