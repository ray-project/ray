#!/usr/bin/env python
"""Is this cluster able to run the health tests at all? Answer in 20 seconds.

    python release/train_tests/health/00_preflight.py

Checks every prerequisite that has bitten us, on every GPU node, before any
GPU time is spent. Exits non-zero if something that matters is wrong.
"""
import argparse
import subprocess
import sys

import ray
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy

OK, WARN, BAD = "ok  ", "warn", "FAIL"


@ray.remote(num_cpus=0)
def _probe():
    """Everything we want to know, collected in one hop per node."""
    import os
    import socket

    def run(cmd):
        try:
            p = subprocess.run(
                cmd, shell=True, capture_output=True, text=True, timeout=30
            )
            return p.returncode, (p.stdout + p.stderr).strip()
        except Exception as e:  # noqa: BLE001
            return 127, str(e)

    info = {"host": socket.gethostname(), "pid": os.getpid()}
    info["ncclras"] = run("ncclras --version 2>&1")
    info["nvidia_smi"] = run(
        "nvidia-smi --query-gpu=name,driver_version --format=csv,noheader"
    )
    info["ptrace_scope"] = run("cat /proc/sys/kernel/yama/ptrace_scope")
    info["capsh"] = run("capsh --print 2>/dev/null | grep -i ptrace")

    # The real py-spy test: attach to a process that is NOT our descendant.
    # ptrace_scope=1 allows only descendants, and py-spy runs as a child of
    # the process it has to trace, which is the wrong direction.
    info["pyspy"] = run(
        "sleep 30 & SLEEP_PID=$!; py-spy dump --pid $SLEEP_PID >/dev/null 2>&1; "
        "echo $?; kill $SLEEP_PID 2>/dev/null"
    )

    try:
        import torch

        info["torch"] = (0, torch.__version__)
        info["nccl"] = (0, ".".join(str(x) for x in torch.cuda.nccl.version()))
        info["cuda_ok"] = (0, str(torch.cuda.is_available()))
        info["gpus"] = (0, str(torch.cuda.device_count()))
    except Exception as e:  # noqa: BLE001
        info["torch"] = (1, f"import failed: {e}")
    return info


def line(status, label, detail=""):
    print(f"  [{status}] {label:<28} {detail}")


def check_node(node, verbose):
    node_id = node["NodeID"]
    ip = node.get("NodeManagerAddress", node_id[:12])
    ref = _probe.options(
        scheduling_strategy=NodeAffinitySchedulingStrategy(node_id=node_id, soft=False)
    ).remote()
    info = ray.get(ref, timeout=120)

    print(f"\n{info['host']} ({ip})")
    fatal = []

    rc, out = info["ncclras"]
    ver = out.split()[-1] if out and rc == 0 else "?"
    major_minor = (
        tuple(int(x) for x in ver.split(".")[:2]) if ver[:1].isdigit() else (0, 0)
    )
    if rc != 0:
        line(BAD, "ncclras", f"not on PATH ({out.splitlines()[:1]})")
        fatal.append("ncclras missing")
    elif major_minor < (2, 28):
        line(BAD, "ncclras", f"{ver} -- needs >= 2.28 for `-f json`")
        fatal.append(f"ncclras {ver} too old")
    else:
        line(OK, "ncclras", ver)

    rc, out = info.get("nccl", (1, "?"))
    if rc == 0:
        mm = tuple(int(x) for x in out.split(".")[:2])
        if mm < (2, 28):
            line(BAD, "in-process NCCL", f"{out} -- needs >= 2.28")
            fatal.append(f"in-process NCCL {out} too old")
        else:
            line(OK, "in-process NCCL", out)
    else:
        line(BAD, "in-process NCCL", out)
        fatal.append("torch import failed")

    line(OK if info.get("cuda_ok", (1,))[0] == 0 else BAD, "torch", info["torch"][1])
    line(OK, "GPUs on node", info.get("gpus", (1, "?"))[1])
    line(OK, "driver", info["nvidia_smi"][1].replace("\n", " | "))

    # py-spy: degraded, not fatal. The dumps still happen, they just lose the
    # native frames where a NCCL hang actually lives.
    pyspy_rc = info["pyspy"][1].splitlines()[0] if info["pyspy"][1] else "?"
    if pyspy_rc == "0":
        line(OK, "py-spy (native dump)", "works")
    else:
        line(
            WARN,
            "py-spy (native dump)",
            f"blocked (ptrace_scope={info['ptrace_scope'][1]}); stack dumps "
            "will fall back to Python-only",
        )

    if verbose:
        line(OK, "capsh ptrace", info["capsh"][1] or "(none)")
    return fatal


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args()

    ray.init(address="auto")
    nodes = [n for n in ray.nodes() if n["Alive"] and n["Resources"].get("GPU", 0) > 0]

    print(f"{len(nodes)} GPU node(s) in the cluster")
    total_gpus = sum(int(n["Resources"]["GPU"]) for n in nodes)
    print(f"{total_gpus} GPU(s) total")

    fatal = []
    for node in nodes:
        fatal += check_node(node, args.verbose)

    print()
    if len(nodes) < 2:
        print(
            "NOTE: 1 GPU node. Enough for step 1 (RAS), not for the rank->host\n"
            "      attribution or node-scoped diagnostics in step 2."
        )
    if total_gpus < 4:
        print(
            "NOTE: < 4 GPUs. The peer-relative checks need >= 3 ranks plus an\n"
            "      outlier, so straggler/numerical policies will not fire."
        )

    if fatal:
        print(f"\n{len(fatal)} blocking problem(s):")
        for f in sorted(set(fatal)):
            print(f"  - {f}")
        print("\nSee release/train_tests/health/CLUSTER_SETUP.md")
        return 1

    print("Preflight passed. Run 01_nccl_ras.py next.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
