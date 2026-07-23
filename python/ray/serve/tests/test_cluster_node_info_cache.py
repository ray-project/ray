import asyncio

import pytest

import ray
from ray._raylet import GcsClient
from ray.serve._private.cluster_node_info_cache import DefaultClusterNodeInfoCache
from ray.serve._private.default_impl import create_cluster_node_info_cache
from ray.serve._private.test_utils import get_node_id
from ray.tests.conftest import *  # noqa


def test_get_alive_nodes(ray_start_cluster):
    cluster = ray_start_cluster
    cluster.add_node(resources={"head": 1})
    ray.init(address=cluster.address)
    worker_node = cluster.add_node(resources={"worker": 1})
    cluster.wait_for_nodes()

    head_node_id = ray.get(get_node_id.options(resources={"head": 1}).remote())
    worker_node_id = ray.get(get_node_id.options(resources={"worker": 1}).remote())

    gcs_client = GcsClient(address=ray.get_runtime_context().gcs_address)
    cluster_node_info_cache = create_cluster_node_info_cache(gcs_client)
    cluster_node_info_cache.update()
    assert set(cluster_node_info_cache.get_alive_nodes()) == {
        (head_node_id, ray.nodes()[0]["NodeName"], ""),
        (worker_node_id, ray.nodes()[0]["NodeName"], ""),
    }
    assert cluster_node_info_cache.get_alive_node_ids() == {
        head_node_id,
        worker_node_id,
    }
    assert (
        cluster_node_info_cache.get_alive_node_ids()
        == cluster_node_info_cache.get_active_node_ids()
    )

    cluster.remove_node(worker_node)
    cluster.wait_for_nodes()

    # The killed worker node shouldn't show up in the alive node list.
    cluster_node_info_cache.update()
    assert cluster_node_info_cache.get_alive_nodes() == [
        (head_node_id, ray.nodes()[0]["NodeName"], "")
    ]
    assert cluster_node_info_cache.get_alive_node_ids() == {head_node_id}
    assert (
        cluster_node_info_cache.get_alive_node_ids()
        == cluster_node_info_cache.get_active_node_ids()
    )


# Snapshots shaped like _apply_snapshot's tuple:
# (alive_nodes, alive_node_id_set, node_labels, total_resources, available_resources).
_OLD = ([("old", "old", "")], frozenset({"old"}), {}, {}, {})
_NEW = ([("new", "new", "")], frozenset({"new"}), {}, {}, {})


def test_refresh_async_stages_without_touching_live_cache():
    """refresh_async must NOT mutate the live cache when the executor completes -- it
    only STAGES the snapshot. The live cache changes only when apply_pending() runs (at
    the control-loop tick boundary), so a background refresh can never land mid-tick.
    This is the invariant the per-cycle update() used to guarantee."""

    async def _run():
        cache = DefaultClusterNodeInfoCache(object())  # GCS is never called
        cache._compute_snapshot = lambda *a: _NEW
        assert cache.get_alive_nodes() is None  # nothing applied yet

        await cache.refresh_async()
        # Staged, but the LIVE cache is untouched.
        assert cache._pending_snapshot == _NEW
        assert cache.get_alive_nodes() is None

        # The tick-boundary swap promotes it.
        cache.apply_pending()
        assert cache.get_alive_nodes() == _NEW[0]
        assert cache._pending_snapshot is None  # consumed

    asyncio.run(_run())


def test_apply_pending_is_noop_without_staged_snapshot():
    """apply_pending() with nothing staged leaves the live cache unchanged, so ticks
    with no completed refresh keep the last snapshot."""

    async def _run():
        cache = DefaultClusterNodeInfoCache(object())
        cache._compute_snapshot = lambda *a: _OLD
        cache.update()  # seed the live cache synchronously
        assert cache.get_alive_nodes() == _OLD[0]

        cache.apply_pending()  # nothing staged
        assert cache.get_alive_nodes() == _OLD[0]  # unchanged

    asyncio.run(_run())


def test_update_applies_synchronously():
    """update() (startup + shutdown path) still fetches and applies in one blocking
    call, independent of the staging mechanism."""

    async def _run():
        cache = DefaultClusterNodeInfoCache(object())
        cache._compute_snapshot = lambda *a: _NEW
        cache.update()
        assert cache.get_alive_nodes() == _NEW[0]

    asyncio.run(_run())


def test_live_cache_frozen_while_refresh_in_flight():
    """End-to-end of the reviewer's concern: while a refresh is suspended in the
    executor, the live cache stays frozen, and when the executor result finally lands it
    is STAGED (not applied), so an in-flight refresh cannot clobber a snapshot a reader
    is mid-tick on -- including one written by a synchronous shutdown update()."""

    async def _run():
        cache = DefaultClusterNodeInfoCache(object())
        loop = asyncio.get_running_loop()

        # Seed a known live snapshot.
        cache._compute_snapshot = lambda *a: _OLD
        cache.update()
        assert cache.get_alive_nodes() == _OLD[0]

        # Route the executor step to a future we resolve by hand.
        exec_future = loop.create_future()
        loop.run_in_executor = lambda executor, fn, *a: exec_future

        task = asyncio.create_task(cache.refresh_async())
        await asyncio.sleep(0)  # let it suspend on the future
        # Live cache is still the seeded snapshot while the refresh is in flight.
        assert cache.get_alive_nodes() == _OLD[0]

        # The executor result lands: it is STAGED, not applied.
        exec_future.set_result(_NEW)
        await task
        assert cache.get_alive_nodes() == _OLD[0]  # live cache still frozen
        assert cache._pending_snapshot == _NEW  # staged for the next tick

    asyncio.run(_run())


def test_refresh_async_single_flight():
    """Only one refresh may be in flight; a concurrent call returns immediately without
    issuing a second executor job."""

    async def _run():
        cache = DefaultClusterNodeInfoCache(object())
        loop = asyncio.get_running_loop()
        calls = {"n": 0}
        exec_future = loop.create_future()

        def _run_in_executor(executor, fn, *a):
            calls["n"] += 1
            return exec_future

        loop.run_in_executor = _run_in_executor
        cache._compute_snapshot = lambda *a: _NEW

        first = asyncio.create_task(cache.refresh_async())
        await asyncio.sleep(0)
        # A second call while the first is in flight is a no-op.
        await cache.refresh_async()
        assert calls["n"] == 1

        exec_future.set_result(_NEW)
        await first
        assert calls["n"] == 1

    asyncio.run(_run())


def test_refresh_async_captures_prior_state_before_executor():
    """refresh_async snapshots the carry-forward state on the event loop and hands it to
    the executor, so _compute_snapshot never reads self off-thread (guards against a
    concurrent apply_pending() mutating self mid-compute)."""

    async def _run():
        cache = DefaultClusterNodeInfoCache(object())
        cache._apply_snapshot(_OLD)  # live cache now holds _OLD's alive-id set

        captured = {}

        def _compute(prior):
            captured["prior"] = prior
            return _NEW

        cache._compute_snapshot = _compute
        await cache.refresh_async()

        # prior[0] is the alive-id set, captured from self at call time (== _OLD), not
        # read from self inside the executor.
        assert captured["prior"][0] == _OLD[1]

    asyncio.run(_run())


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-s", __file__]))
