import sys
from typing import TYPE_CHECKING

import pytest

import ray
from ray import cloudpickle
from ray.data import ActorPoolStrategy
from ray.data._internal.util import explain_plan

if TYPE_CHECKING:
    from ray._private.worker import BaseContext

# ---------------------------------------------------------------------------
# UDF helpers
# ---------------------------------------------------------------------------


class Sync:
    def __call__(self, batch):
        return batch


class Sync2:
    def __call__(self, batch):
        return batch


class AsyncUDF:
    async def __call__(self, batch):
        return batch


class AsyncUDF2:
    async def __call__(self, batch):
        return batch


# Workers resolve UDFs by pickle value because the test module is not an
# importable top-level package from a worker's cwd.
cloudpickle.register_pickle_by_value(sys.modules[__name__])

_SAME_STRATEGY = ActorPoolStrategy(size=1)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _fused_name(up: str, down: str) -> str:
    return f"MapBatches({up})->MapBatches({down})"


def _plan(ds) -> str:
    """Return the optimised physical plan string (same source as ds.explain())."""
    return explain_plan(ds._logical_plan)


def _is_fused(ds, up: str, down: str) -> bool:
    return _fused_name(up, down) in _plan(ds)


# ---------------------------------------------------------------------------
# Unit test for _udf_is_async — no Ray cluster needed
# ---------------------------------------------------------------------------


def test_udf_is_async_detects_callable_class() -> None:
    """_udf_is_async must return True for a class whose __call__ is async."""
    from ray.data._internal.experimental.logical.rules.operator_fusion import (
        _udf_is_async,
    )

    class SyncClass:
        def __call__(self, x):
            return x

    class AsyncClass:
        async def __call__(self, x):
            return x

    async def async_fn(x):
        return x

    def sync_fn(x):
        return x

    assert _udf_is_async(AsyncClass) is True
    assert _udf_is_async(SyncClass) is False
    assert _udf_is_async(async_fn) is True
    assert _udf_is_async(sync_fn) is False
    # Instance of async callable class
    assert _udf_is_async(AsyncClass()) is True
    assert _udf_is_async(SyncClass()) is False


# ---------------------------------------------------------------------------
# Plan-level fusion tests (no execution needed)
# ---------------------------------------------------------------------------


def test_actor_actor_fuses_when_strategies_equal(
    ray_start_10_cpus_shared: "BaseContext",
) -> None:
    """Two actor map ops with identical strategies must appear fused in the plan."""
    ds = (
        ray.data.range(10, override_num_blocks=2)
        .map_batches(Sync, compute=_SAME_STRATEGY)
        .map_batches(Sync2, compute=_SAME_STRATEGY)
    )
    assert _is_fused(ds, "Sync", "Sync2"), _plan(ds)


def test_actor_actor_no_fuse_when_pool_size_differs(
    ray_start_10_cpus_shared: "BaseContext",
) -> None:
    """Actor->Actor with different pool sizes must NOT appear fused."""
    ds = (
        ray.data.range(10, override_num_blocks=2)
        .map_batches(Sync, compute=ActorPoolStrategy(size=1))
        .map_batches(Sync2, compute=ActorPoolStrategy(size=2))
    )
    assert not _is_fused(ds, "Sync", "Sync2"), _plan(ds)


def test_actor_actor_no_fuse_when_output_cap_differs(
    ray_start_10_cpus_shared: "BaseContext",
) -> None:
    """Actor->Actor with different max_num_outputs_per_actor must NOT fuse."""
    ds = (
        ray.data.range(10, override_num_blocks=2)
        .map_batches(
            Sync, compute=ActorPoolStrategy(size=1, max_num_outputs_per_actor=1)
        )
        .map_batches(Sync2, compute=ActorPoolStrategy(size=1))
    )
    assert not _is_fused(ds, "Sync", "Sync2"), _plan(ds)


def test_actor_actor_no_fuse_when_output_bytes_cap_differs(
    ray_start_10_cpus_shared: "BaseContext",
) -> None:
    """Actor->Actor with different max_num_output_bytes_per_actor must NOT fuse."""
    ds = (
        ray.data.range(10, override_num_blocks=2)
        .map_batches(
            Sync,
            compute=ActorPoolStrategy(
                size=1, max_num_output_bytes_per_actor=64 * 1024 * 1024
            ),
        )
        .map_batches(Sync2, compute=ActorPoolStrategy(size=1))
    )
    assert not _is_fused(ds, "Sync", "Sync2"), _plan(ds)


def test_async_sync_pair_not_fused(ray_start_10_cpus_shared: "BaseContext") -> None:
    """Async-then-sync actor pair must NOT fuse."""
    ds = (
        ray.data.range(10, override_num_blocks=2)
        .map_batches(AsyncUDF, compute=_SAME_STRATEGY)
        .map_batches(Sync, compute=_SAME_STRATEGY)
    )
    assert not _is_fused(ds, "AsyncUDF", "Sync"), _plan(ds)


def test_sync_async_pair_not_fused(ray_start_10_cpus_shared: "BaseContext") -> None:
    """Sync-then-async actor pair must NOT fuse (symmetric check)."""
    ds = (
        ray.data.range(10, override_num_blocks=2)
        .map_batches(Sync, compute=_SAME_STRATEGY)
        .map_batches(AsyncUDF, compute=_SAME_STRATEGY)
    )
    assert not _is_fused(ds, "Sync", "AsyncUDF"), _plan(ds)


def test_async_async_pair_not_fused(ray_start_10_cpus_shared: "BaseContext") -> None:
    """Async-then-async actor pair must NOT fuse.

    Fusing two async UDFs deadlocks the fused task before its first UDF call:
    the downstream transform's coroutine pulls the upstream transform's sync
    bridge generator on the actor's shared asyncio loop thread, which then
    blocks in ``output_queue.get()`` before the upstream producer coroutine
    (queued behind it on the same loop) can run
    (``plan_udf_map_op.py::_generate_transform_fn_for_async_map``). This is the
    llm_batch chat_template->tokenize hang from release runs 7743/7749.
    """
    ds = (
        ray.data.range(10, override_num_blocks=2)
        .map_batches(AsyncUDF, compute=_SAME_STRATEGY)
        .map_batches(AsyncUDF2, compute=_SAME_STRATEGY)
    )
    assert not _is_fused(ds, "AsyncUDF", "AsyncUDF2"), _plan(ds)


def test_three_way_actor_fusion(ray_start_10_cpus_shared: "BaseContext") -> None:
    """Three consecutive actor map ops with equal strategies must all fuse."""
    ds = (
        ray.data.range(10, override_num_blocks=2)
        .map_batches(Sync, compute=_SAME_STRATEGY)
        .map_batches(Sync2, compute=_SAME_STRATEGY)
        .map_batches(Sync, compute=_SAME_STRATEGY)
    )
    plan = _plan(ds)
    assert "MapBatches(Sync)->MapBatches(Sync2)->MapBatches(Sync)" in plan, plan


# ---------------------------------------------------------------------------
# Correctness test (requires execution)
# ---------------------------------------------------------------------------


def test_actor_actor_fuses_and_produces_correct_output(
    ray_start_10_cpus_shared: "BaseContext",
) -> None:
    """Fused Actor->Actor pipeline must produce the same rows as the unfused plan."""
    n = 20

    class AddOne:
        def __call__(self, batch):
            batch["val"] = [x + 1 for x in batch["id"]]
            return batch

    class Double:
        def __call__(self, batch):
            batch["val"] = [x * 2 for x in batch["val"]]
            return batch

    strategy = ActorPoolStrategy(size=1)
    ds = (
        ray.data.range(n, override_num_blocks=n)
        .map_batches(AddOne, compute=strategy)
        .map_batches(Double, compute=strategy)
    )
    # Verify fusion in the plan.
    assert "MapBatches(AddOne)->MapBatches(Double)" in _plan(ds), _plan(ds)
    # Verify correctness.
    rows = sorted(ds.take_all(), key=lambda r: r["id"])
    assert [r["val"] for r in rows] == [(i + 1) * 2 for i in range(n)]


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-x", __file__]))
