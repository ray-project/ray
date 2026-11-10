#!/usr/bin/env python
"""Check that the running cluster can run the health scripts.

    python release/train_tests/health/check_cluster.py

Prints whether the installed ``ray/train`` matches this checkout's, and for
every node: its GPUs as torch sees them, ``py-spy``, and whether
``NCCLRASReadyProbe`` finds an ``ncclras`` and an NCCL of at least 2.28, under
the ``runtime_env`` that ``nccl_hang.py`` uses. Then which scripts can run.
"""
import shutil
import sys
from pathlib import Path
from typing import Dict, List

import ray
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy

sys.path.insert(0, str(Path(__file__).parent))
import harness  # noqa: E402
import nccl_ras_health  # noqa: E402

harness.ship_by_value(harness, nccl_ras_health)

CHECKOUT_TRAIN = Path(__file__).resolve().parents[3] / "python" / "ray" / "train"


def differing_train_files() -> List[str]:
    """This checkout's ``ray/train`` files, tests aside, that the installed Ray
    lacks or has different."""
    installed = Path(ray.__file__).parent / "train"
    differ = []
    for path in sorted(CHECKOUT_TRAIN.rglob("*.py")):
        rel = path.relative_to(CHECKOUT_TRAIN)
        if "tests" in rel.parts:
            continue
        other = installed / rel
        if not other.exists() or other.read_bytes() != path.read_bytes():
            differ.append(str(rel))
    return differ


def _inspect() -> Dict:
    info = {"node": ray.get_runtime_context().get_node_id()[:8]}
    try:
        import torch

        info["torch"] = torch.__version__
        info["gpus"] = [
            torch.cuda.get_device_name(i) for i in range(torch.cuda.device_count())
        ]
    except Exception as e:  # noqa: BLE001
        info["torch"], info["gpus"] = f"error: {e}", []
    info["py_spy"] = shutil.which("py-spy") is not None
    info["nccl_ras"] = nccl_ras_health.NCCLRASReadyProbe().poll().detail
    return info


def main() -> int:
    ray.init(address="auto", runtime_env=harness.nccl_runtime_env())
    print(f"Ray {ray.__version__}, commit {ray.__commit__[:10]}")
    if CHECKOUT_TRAIN.is_dir():
        differ = differing_train_files()
        if differ:
            print(f"The installed ray/train differs from {CHECKOUT_TRAIN} in:")
            for rel in differ[:20]:
                print(f"  {rel}")
            print("Rebuild the image from this branch, or run sync_ray_train.py.")
        else:
            print(f"The installed ray/train matches {CHECKOUT_TRAIN}.")

    nodes = [n for n in ray.nodes() if n["Alive"]]
    inspect = ray.remote(_inspect)
    infos = ray.get(
        [
            inspect.options(
                num_cpus=0,
                num_gpus=1 if n["Resources"].get("GPU") else 0,
                scheduling_strategy=NodeAffinitySchedulingStrategy(
                    n["NodeID"], soft=False
                ),
            ).remote()
            for n in nodes
        ]
    )
    print(f"\n  {'node':<9} {'torch':<14} {'py-spy':<7} {'GPUs':<28} NCCL RAS")
    for info in infos:
        gpus = ", ".join(info["gpus"]) or "-"
        print(
            f"  {info['node']:<9} {info['torch']:<14} "
            f"{'yes' if info['py_spy'] else 'no':<7} {gpus:<28} {info['nccl_ras']}"
        )

    gpu_infos = [i for i in infos if i["gpus"]]
    gpus = sum(len(i["gpus"]) for i in gpu_infos)
    not_ready = {
        i["node"]: i["nccl_ras"] for i in gpu_infos if i["nccl_ras"] != "ready"
    }
    print(f"\n  {gpus} GPUs on {len(gpu_infos)} nodes")
    print(f"  gpu_faults.py, gr00t_recovery.py: {'ready' if gpus else 'no GPUs'}")
    if not gpus:
        print("  nccl_hang.py: no GPUs")
    elif not_ready:
        print(f"  nccl_hang.py: not ready: {not_ready}")
    else:
        print("  nccl_hang.py: ready")
    return 0 if gpus else 1


if __name__ == "__main__":
    sys.exit(main())
