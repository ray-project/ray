"""Run large_body_rss.py in given abtest arms on the live deployment.

Usage: python mode_rss.py --out DIR [--rounds N] copies on
"""
import argparse
import json
import os
import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
import abtest  # noqa: E402

parser = argparse.ArgumentParser()
parser.add_argument("--out", required=True)
parser.add_argument("--rounds", type=int, default=3)
parser.add_argument("arms", nargs="+")
args = parser.parse_args()

import ray  # noqa: E402

os.environ["RAY_RUNTIME_ENV_HOOK"] = "sm_noop_hook.env_hook"
ray.init(address="auto", namespace="serve", log_to_driver=False)
replicas, proxy = abtest.actors()
out = Path(args.out)
for arm in args.arms:
    sides = {"copies": ("copies", False), "on": (True, True), "off": (False, False)}[
        arm
    ]
    abtest.set_mode(
        arm == "on", replicas, proxy, out / f"mode_{arm}", "dist_final", *sides
    )
    tunables = abtest.haproxy_glibc_tunables()
    print(arm, "HAProxy GLIBC_TUNABLES:", tunables, flush=True)
    subprocess.run(
        [
            abtest.PY,
            str(Path(__file__).parent / "large_body_rss.py"),
            "--out",
            str(out / f"rss_{arm}.json"),
            "--rounds",
            str(args.rounds),
        ],
        check=True,
    )
    rows = json.loads((out / f"rss_{arm}.json").read_text())
    (out / f"rss_{arm}.json").write_text(
        json.dumps({"glibc_tunables": tunables, "rows": rows}, indent=1)
    )
abtest.set_mode(True, replicas, proxy, out / "restored", "dist_final")
print("RSS DONE", flush=True)
