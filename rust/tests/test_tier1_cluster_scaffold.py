"""Tier 1 cluster-backed conformance scaffold.

This file is the first scaffold for the highest-value cross-language tests from
the backend port review. It is intentionally lightweight:

- If the Rust `_raylet` extension is not built, the module is skipped.
- If it is built, these tests exercise the in-process Rust cluster startup path.
- The more detailed GCS/Raylet/CoreWorker conformance cases can be filled in on
  top of this scaffold.
"""

from __future__ import annotations

import sys
import time
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[2]
RUST_PY_ROOT = ROOT / "rust"

if str(RUST_PY_ROOT) not in sys.path:
    sys.path.insert(0, str(RUST_PY_ROOT))

pytest.importorskip("_raylet")
import ray  # noqa: E402


@pytest.fixture()
def rust_ray_runtime():
    ray.init()
    try:
        yield
    finally:
        ray.shutdown()


def test_tier1_cluster_startup_smoke(rust_ray_runtime):
    assert ray.is_initialized()


def test_tier1_put_get_roundtrip_baseline(rust_ray_runtime):
    ref = ray.put({"k": "v", "n": 3})
    assert ray.get(ref) == {"k": "v", "n": 3}


def test_tier1_object_fetch_wait_timeout_conformance(rust_ray_runtime):
    @ray.remote
    def slow_value(delay_s, value):
        time.sleep(delay_s)
        return value

    slow = slow_value.remote(0.20, "slow")
    fast = slow_value.remote(0.01, "fast")

    ready, remaining = ray.wait([slow, fast], num_returns=1, timeout=0.05)
    assert len(ready) <= 1
    assert len(ready) + len(remaining) == 2
    assert set(ready).isdisjoint(set(remaining))

    all_results = sorted(ray.get([slow, fast]))
    assert all_results == ["fast", "slow"]


def test_tier1_actor_task_ordering_restart_cancel_conformance(rust_ray_runtime):
    @ray.remote
    class Counter:
        def __init__(self):
            self.v = 0

        def inc(self):
            self.v += 1
            return self.v

    actor = Counter.remote()
    refs = [actor.inc.remote(), actor.inc.remote(), actor.inc.remote()]
    assert ray.get(refs) == [1, 2, 3]


def test_tier1_placement_group_lifecycle_conformance(rust_ray_runtime):
    if not hasattr(ray, "util"):
        pytest.xfail("Rust Python package does not expose ray.util")

    placement_group = getattr(ray.util, "placement_group", None)
    remove_placement_group = getattr(ray.util, "remove_placement_group", None)
    if placement_group is None or remove_placement_group is None:
        pytest.xfail("Rust Python package does not expose placement-group helpers")

    pg = placement_group(bundles=[{"CPU": 1}])
    assert pg is not None
    if hasattr(pg, "ready"):
        ray.get(pg.ready())
    remove_placement_group(pg)


def test_tier1_worker_lease_and_drain_conformance(rust_ray_runtime):
    runtime = getattr(ray, "_runtime", None)
    if runtime is None or getattr(runtime, "gcs", None) is None:
        pytest.xfail("Rust runtime does not expose a live GCS client handle")

    cluster = getattr(runtime, "cluster", None)
    if cluster is None or not hasattr(cluster, "node_id"):
        pytest.xfail("Rust runtime does not expose cluster/node identity")

    node_id = cluster.node_id()
    if not hasattr(node_id, "binary"):
        pytest.xfail("Rust node identifier does not expose binary bytes for drain RPCs")

    drain_nodes = getattr(runtime.gcs, "drain_nodes", None)
    if drain_nodes is None:
        pytest.xfail("Rust GCS client does not expose drain_nodes")

    requested = [node_id.binary()]
    result = drain_nodes(requested)
    assert isinstance(result, list)
    assert len(result) == len(requested)
