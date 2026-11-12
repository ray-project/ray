#!/usr/bin/env python
"""Check that the running cluster can run the health scripts.

    python release/train_tests/health/check_cluster.py

Prints whether the installed ``ray/train`` matches this checkout's, the system
NCCL and torch's, and for every node: torch, the GPUs it sees, its NCCL and
``py-spy``, as ``gpu_faults.py`` and ``gr00t_recovery.py`` run them. On each GPU
node, it also runs ``NCCLRASReadyProbe`` under ``nccl_hang.py``'s
``runtime_env``. Then which scripts can run.
"""
import shutil
import sys
from pathlib import Path
from typing import Dict, List

import ray
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy

sys.path.insert(0, str(Path(__file__).parent))
import gpu_health  # noqa: E402
import harness  # noqa: E402
import nccl_ras_health  # noqa: E402

harness.ship_by_value(gpu_health, harness, nccl_ras_health)

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


def _version(version) -> str:
    return ".".join(map(str, version)) if version else "-"


def _inspect() -> Dict:
    info = {"node": ray.get_runtime_context().get_node_id()[:8], "gpus": []}
    try:
        import torch

        info["torch"] = torch.__version__
        info["gpus"] = [
            torch.cuda.get_device_name(i) for i in range(torch.cuda.device_count())
        ]
    except Exception as e:  # noqa: BLE001
        info["torch"] = f"error: {e}"
    info["nccl"] = _version(harness.torch_nccl_version())
    info["py_spy"] = shutil.which("py-spy") is not None
    return info


def _nccl_ras_ready() -> str:
    return nccl_ras_health.NCCLRASReadyProbe().poll().detail


def _on(node: Dict, **options):
    return dict(
        num_cpus=0,
        num_gpus=1 if node["Resources"].get("GPU") else 0,
        scheduling_strategy=NodeAffinitySchedulingStrategy(node["NodeID"], soft=False),
        **options,
    )


def main() -> int:
    ray.init(address="auto")
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

    nccl_env = harness.nccl_runtime_env()
    print(
        f"System NCCL {_version(harness.system_nccl_version())}, torch's NCCL "
        f"{_version(harness.torch_nccl_version())}: nccl_hang.py "
        f"{'preloads' if 'LD_PRELOAD' in nccl_env['env_vars'] else 'does not preload'}"
        f" {harness.SYSTEM_LIBNCCL}."
    )

    nodes = [n for n in ray.nodes() if n["Alive"]]
    gpu_nodes = [n for n in nodes if n["Resources"].get("GPU")]
    inspect, ready = ray.remote(_inspect), ray.remote(_nccl_ras_ready)
    infos = ray.get([inspect.options(**_on(n)).remote() for n in nodes])
    nccl_ras = dict(
        zip(
            [n["NodeID"][:8] for n in gpu_nodes],
            ray.get(
                [
                    ready.options(**_on(n, runtime_env=nccl_env)).remote()
                    for n in gpu_nodes
                ]
            ),
        )
    )

    print(
        f"\n  {'node':<9} {'torch':<14} {'NCCL':<8} {'py-spy':<7} {'GPUs':<16} NCCL RAS"
    )
    for info in infos:
        gpus = ", ".join(info["gpus"]) or "-"
        print(
            f"  {info['node']:<9} {info['torch'][:60]:<14} {info['nccl']:<8} "
            f"{'yes' if info['py_spy'] else 'no':<7} {gpus:<16} "
            f"{nccl_ras.get(info['node'], '-')}"
        )

    blind = [i["node"] for i in infos if i["node"] in nccl_ras and not i["gpus"]]
    not_ready = {node: d for node, d in nccl_ras.items() if d != "ready"}
    gpus = sum(int(n["Resources"]["GPU"]) for n in gpu_nodes)
    print(f"\n  Ray: {gpus} GPUs on {len(gpu_nodes)} nodes")
    if not gpu_nodes:
        print("  gpu_faults.py, gr00t_recovery.py, nccl_hang.py: no GPUs")
        return 1
    if blind:
        print(f"  gpu_faults.py, gr00t_recovery.py: torch sees no GPU on {blind}")
    else:
        print("  gpu_faults.py, gr00t_recovery.py: ready")
    if not_ready:
        print(f"  nccl_hang.py: not ready: {not_ready}")
    else:
        print("  nccl_hang.py: ready")
    return 1 if blind else 0


if __name__ == "__main__":
    sys.exit(main())
