#!/usr/bin/env python
"""Install this checkout's ``python/ray/train`` into the installed Ray on every
node of the running cluster, so the health stack runs without building Ray.

    python release/train_tests/health/sync_ray_train.py

Every change on the health stack is Python under ``ray/train``, so a Ray
nightly from the same master plus this script is enough. It replaces
``ray/train`` in each node's installed Ray, and keeps the old one as
``ray/train.orig`` the first time. Run it again after nodes join or after the
checkout changes; new worker processes pick it up, running ones don't.
"""
import io
import os
import shutil
import sys
import zipfile
from pathlib import Path

import ray
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy

TRAIN_DIR = Path(__file__).resolve().parents[3] / "python" / "ray" / "train"


def _zip(root: Path) -> bytes:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as archive:
        for path in root.rglob("*"):
            if "__pycache__" in path.parts:
                continue
            if path.is_file():
                archive.write(path, path.relative_to(root))
    return buffer.getvalue()


@ray.remote(num_cpus=0)
def _install(payload: bytes) -> str:
    target = os.path.join(os.path.dirname(ray.__file__), "train")
    if not os.path.exists(target + ".orig"):
        shutil.copytree(target, target + ".orig")
    staging = target + ".new"
    shutil.rmtree(staging, ignore_errors=True)
    with zipfile.ZipFile(io.BytesIO(payload)) as archive:
        archive.extractall(staging)
    shutil.rmtree(target)
    os.replace(staging, target)
    return f"{ray.get_runtime_context().get_node_id()[:8]} {target}"


def main() -> int:
    if not TRAIN_DIR.is_dir():
        print(f"{TRAIN_DIR} not found; run this from a Ray checkout.")
        return 1
    ray.init(address="auto")
    payload = ray.put(_zip(TRAIN_DIR))
    nodes = [n["NodeID"] for n in ray.nodes() if n["Alive"]]
    installed = ray.get(
        [
            _install.options(
                scheduling_strategy=NodeAffinitySchedulingStrategy(node, soft=False)
            ).remote(payload)
            for node in nodes
        ]
    )
    print(f"Installed {TRAIN_DIR} on {len(installed)} nodes:")
    for line in installed:
        print(f"  {line}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
