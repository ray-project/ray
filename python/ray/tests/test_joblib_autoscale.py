"""Tests for the opt-in autoscaling mode of ``ray.util.multiprocessing.Pool``.

Covers fixed-pool compatibility, pull-based dispatch, asynchronous APIs,
failure handling, idle reaping, and end-to-end autoscaling.

Callables are defined as closures inside each test so cloudpickle serializes
them by value and Ray workers never need to import this test module.
"""

import gc
import sys
import threading
import time
import weakref

import numpy as np
import pytest

import ray
from ray._common.test_utils import wait_for_condition
from ray.util.multiprocessing import Pool

# ---------------------------------------------------------------------------
# Correctness
# ---------------------------------------------------------------------------


def test_default_path_uses_fixed_pool(shutdown_only):
    """autoscale=False keeps the existing fixed-pool invariant."""
    ray.init(num_cpus=4, include_dashboard=False, ignore_reinit_error=True)
    pool = Pool(processes=4)
    assert len(pool._actor_pool) == 4
    assert all(slot is not None for slot in pool._actor_pool)
    assert pool._autoscale is False
    pool.terminate()


def test_autoscale_lazy_creation(shutdown_only):
    """autoscale=True, initial_size=0: no actors at startup, lazy-create."""
    ray.init(num_cpus=2, include_dashboard=False, ignore_reinit_error=True)
    pool = Pool(processes=2, autoscale=True, max_size=2, initial_size=0)
    assert len(pool._actor_pool) == 2
    assert all(slot is None for slot in pool._actor_pool)

    def square(x):
        return x * x

    result = pool.map(square, range(4))
    assert result == [0, 1, 4, 9]
    assert any(slot is not None for slot in pool._actor_pool)
    pool.terminate()


def test_autoscale_zero_cpu_head_accepts_explicit_target(shutdown_only):
    """A zero-CPU head can create pending actors to drive scale-up."""
    ray.init(num_cpus=0, include_dashboard=False, ignore_reinit_error=True)
    pool = Pool(processes=2, autoscale=True, max_size=2, initial_size=0)

    result = pool.apply_async(lambda: 1)
    wait_for_condition(lambda: len(pool._starting_actor_refs) == 1, timeout=10)
    assert not result.ready()
    pool.terminate()
    with pytest.raises(Exception):
        result.get(timeout=10)


def test_autoscale_pool_can_be_collected(shutdown_only):
    """The dispatcher thread must not keep an unused Pool alive."""
    ray.init(num_cpus=2, include_dashboard=False, ignore_reinit_error=True)
    pool = Pool(processes=2, autoscale=True, max_size=2, initial_size=0)
    dispatcher = pool._dispatcher_thread
    pool_ref = weakref.ref(pool)

    del pool
    gc.collect()

    wait_for_condition(
        lambda: pool_ref() is None and not dispatcher.is_alive(),
        timeout=10,
    )


def test_autoscale_pool_lives_until_async_result_finishes(shutdown_only):
    """Dropping the Pool must not strand an outstanding AsyncResult."""
    ray.init(num_cpus=1, include_dashboard=False, ignore_reinit_error=True)
    pool = Pool(processes=1, autoscale=True, max_size=1, initial_size=0)

    def slow_identity(value):
        time.sleep(0.5)
        return value

    result = pool.apply_async(slow_identity, (42,))
    running_actor_refs = pool._running_actor_refs
    wait_for_condition(lambda: len(running_actor_refs) == 1, timeout=10)
    dispatcher = pool._dispatcher_thread
    pool_ref = weakref.ref(pool)

    del pool
    gc.collect()

    assert pool_ref() is not None
    assert result.get(timeout=10) == 42
    wait_for_condition(
        lambda: pool_ref() is None and not dispatcher.is_alive(),
        timeout=10,
    )


def test_autoscale_rejects_invalid_sizes(shutdown_only):
    """Invalid autoscaling bounds must fail during Pool construction."""
    ray.init(num_cpus=2, include_dashboard=False, ignore_reinit_error=True)
    invalid_options = [
        {"max_size": 0},
        {"max_size": 2, "min_size": -1},
        {"max_size": 2, "min_size": 3},
        {"max_size": 2, "initial_size": -1},
        {"max_size": 2, "initial_size": 3},
        {"max_size": 2, "idle_timeout_s": -1},
    ]
    for options in invalid_options:
        with pytest.raises(ValueError):
            Pool(processes=2, autoscale=True, **options)


def test_autoscale_submits_only_to_ready_actors(shutdown_only):
    """Pending actors must not strand work when max_size exceeds CPUs."""
    ray.init(num_cpus=2, include_dashboard=False, ignore_reinit_error=True)
    pool = Pool(
        processes=8,
        autoscale=True,
        max_size=8,
        initial_size=0,
        idle_timeout_s=999,
    )

    def compute(x):
        return x + sum(j * j for j in range(500))

    start = time.monotonic()
    async_result = pool.map_async(compute, range(50))
    assert time.monotonic() - start < 1
    result = async_result.get(timeout=30)
    assert result == [compute(i) for i in range(50)]
    assert list(pool.imap(compute, range(10), chunksize=1)) == [
        compute(i) for i in range(10)
    ]
    pool.terminate()


def test_autoscale_map_accepts_numpy_arrays(shutdown_only):
    """Array truth-value semantics must not affect autoscale chunking."""
    ray.init(num_cpus=2, include_dashboard=False, ignore_reinit_error=True)
    pool = Pool(processes=2, autoscale=True, max_size=2, initial_size=0)

    def square(value):
        return value * value

    values = np.array([1, 2, 3, 4])
    assert pool.map(square, values) == [1, 4, 9, 16]
    assert pool.map_async(square, values).get(timeout=10) == [1, 4, 9, 16]
    pool.terminate()


def test_autoscale_apply_async_returns_immediately(shutdown_only):
    """apply_async must not wait for actor startup or task completion."""
    ray.init(num_cpus=1, include_dashboard=False, ignore_reinit_error=True)
    pool = Pool(processes=2, autoscale=True, max_size=2, initial_size=0)

    def slow_identity(x):
        time.sleep(1)
        return x

    start = time.monotonic()
    result = pool.apply_async(slow_identity, (42,))
    assert time.monotonic() - start < 0.5
    assert not result.ready()
    assert result.get(timeout=10) == 42
    pool.terminate()


def test_autoscale_actor_start_failures_do_not_hang(shutdown_only):
    """Actor configuration and initializer failures must reach the caller."""
    ray.init(num_cpus=2, include_dashboard=False, ignore_reinit_error=True)

    with pytest.raises(ValueError):
        Pool(
            processes=2,
            autoscale=True,
            max_size=2,
            initial_size=0,
            ray_remote_args={"num_cpus": -1},
        )

    def bad_initializer():
        raise RuntimeError("initializer failed")

    pool = Pool(
        processes=2,
        autoscale=True,
        max_size=2,
        initial_size=0,
        initializer=bad_initializer,
    )
    result = pool.apply_async(lambda: 1)
    with pytest.raises(ray.exceptions.ActorDiedError):
        result.get(timeout=10)
    wait_for_condition(
        lambda: not pool._dispatcher_thread.is_alive(),
        timeout=10,
    )
    assert pool._closed
    pool.terminate()


def test_autoscale_serialization_error_does_not_stop_dispatcher(shutdown_only):
    """A bad batch must fail without stranding later valid submissions."""
    ray.init(num_cpus=1, include_dashboard=False, ignore_reinit_error=True)
    pool = Pool(processes=4, autoscale=True, max_size=4, initial_size=0)

    def identity(value):
        return value

    assert pool.apply(identity, (1,)) == 1

    bad_result = pool.apply_async(identity, (threading.Lock(),))
    valid_result = pool.apply_async(identity, (2,))

    with pytest.raises(TypeError):
        bad_result.get(timeout=10)
    assert valid_result.get(timeout=10) == 2
    assert pool.apply(identity, (3,)) == 3
    assert pool._dispatcher_thread.is_alive()
    pool.terminate()


def test_joblib_autoscale_propagates_serialization_errors(shutdown_only):
    """Joblib must report serialization failures instead of timing out."""
    from joblib import Parallel, delayed, parallel_backend

    from ray.util.joblib import register_ray

    ray.init(num_cpus=2, include_dashboard=False, ignore_reinit_error=True)
    register_ray(autoscale=True, max_size=4, initial_size=0)

    def identity(value):
        return value

    with pytest.raises(TypeError):
        with parallel_backend("ray", n_jobs=4):
            Parallel(timeout=10)(
                delayed(identity)(value) for value in [1, threading.Lock(), 2]
            )

    with parallel_backend("ray", n_jobs=4):
        assert Parallel(timeout=10)(delayed(identity)(value) for value in [3, 4]) == [
            3,
            4,
        ]


def test_joblib_autoscale_propagates_errors_and_can_be_reused(shutdown_only):
    """Joblib failures must propagate without stranding later dispatch."""
    from joblib import Parallel, delayed, parallel_backend

    from ray.util.joblib import register_ray

    ray.init(num_cpus=2, include_dashboard=False, ignore_reinit_error=True)
    register_ray(autoscale=True, max_size=4, initial_size=0, idle_timeout_s=999)

    def maybe_fail(x):
        if x == 5:
            raise ValueError("expected joblib failure")
        return x

    with pytest.raises(ValueError, match="expected joblib failure"):
        with parallel_backend("ray", n_jobs=4):
            Parallel(pre_dispatch=2)(delayed(maybe_fail)(x) for x in range(20))

    with parallel_backend("ray", n_jobs=4):
        results = Parallel(pre_dispatch=2)(
            delayed(lambda x: x * x)(x) for x in range(20)
        )
    assert results == [x * x for x in range(20)]


def test_joblib_n_jobs_limits_autoscale_concurrency(shutdown_only):
    """max_size must not exceed joblib's n_jobs concurrency limit."""
    from joblib import Parallel, delayed, parallel_backend

    from ray.util.joblib import register_ray

    ray.init(num_cpus=4, include_dashboard=False, ignore_reinit_error=True)
    register_ray(
        autoscale=True,
        min_size=4,
        max_size=4,
        initial_size=4,
        idle_timeout_s=999,
    )

    @ray.remote(num_cpus=0)
    class ConcurrencyTracker:
        def __init__(self):
            self.current = 0
            self.maximum = 0

        def enter(self):
            self.current += 1
            self.maximum = max(self.maximum, self.current)

        def leave(self):
            self.current -= 1

        def get_maximum(self):
            return self.maximum

    tracker = ConcurrencyTracker.remote()

    def tracked_identity(value):
        ray.get(tracker.enter.remote())
        try:
            time.sleep(0.1)
            return value
        finally:
            ray.get(tracker.leave.remote())

    with parallel_backend("ray", n_jobs=2):
        results = Parallel(n_jobs=2, batch_size=1, pre_dispatch="all")(
            delayed(tracked_identity)(value) for value in range(8)
        )

    assert results == list(range(8))
    assert ray.get(tracker.get_maximum.remote()) == 2


def test_autoscale_terminate_finishes_queued_results(shutdown_only):
    """Undispatched batches fail instead of leaving ResultThreads blocked."""
    ray.init(num_cpus=1, include_dashboard=False, ignore_reinit_error=True)
    pool = Pool(processes=4, autoscale=True, max_size=4, initial_size=0)

    def slow_identity(x):
        time.sleep(10)
        return x

    pool._registry.append((object(), ray.put("cached")))
    pool._registry_hashable["cached"] = ray.put("cached")
    result = pool.map_async(slow_identity, range(8), chunksize=1)
    pool.terminate()
    assert not pool._registry
    assert not pool._registry_hashable
    with pytest.raises(Exception):
        result.get(timeout=10)


@pytest.mark.parametrize("stop_method", ["close", "terminate"])
def test_autoscale_imap_does_not_submit_after_stop(shutdown_only, stop_method):
    """Lazy imap submissions after stopping the pool must not hang."""
    ray.init(num_cpus=1, include_dashboard=False, ignore_reinit_error=True)
    pool = Pool(processes=1, autoscale=True, max_size=1, initial_size=0)

    result = pool.imap(lambda value: value, iter(range(3)), chunksize=1)
    wait_for_condition(lambda: result._result_thread._num_ready == 1, timeout=10)
    getattr(pool, stop_method)()

    assert result.next(timeout=5) == 0
    error = result.next(timeout=5)
    assert isinstance(error, Exception)
    expected_error = RuntimeError if stop_method == "terminate" else ValueError
    assert isinstance(error.underlying, expected_error)


def test_autoscale_imap_exact_chunks_stops_result_thread(shutdown_only):
    """Exact chunk multiples must still signal the dynamic result thread."""
    ray.init(num_cpus=2, include_dashboard=False, ignore_reinit_error=True)
    pool = Pool(processes=8, autoscale=True, max_size=8, initial_size=0)

    for values in ([], list(range(10))):
        result = pool.imap(lambda value: value, iter(values), chunksize=1)
        assert list(result) == values
        wait_for_condition(lambda: not result._result_thread.is_alive(), timeout=5)
    pool.terminate()


def test_autoscale_idle_reap(shutdown_only):
    """Idle actors past idle_timeout_s are reaped down to min_size."""
    ray.init(num_cpus=2, include_dashboard=False, ignore_reinit_error=True)
    pool = Pool(
        processes=2,
        autoscale=True,
        max_size=2,
        initial_size=2,
        min_size=0,
        idle_timeout_s=0.5,
    )
    assert sum(1 for s in pool._actor_pool if s is not None) == 2
    wait_for_condition(
        lambda: sum(1 for s in pool._actor_pool if s is not None) == 0,
        timeout=10,
    )
    pool.terminate()


def test_autoscale_does_not_reap_busy_actor(shutdown_only):
    """A batch running past idle_timeout_s must finish before actor reaping."""
    ray.init(num_cpus=1, include_dashboard=False, ignore_reinit_error=True)
    pool = Pool(
        processes=1,
        autoscale=True,
        max_size=1,
        initial_size=0,
        idle_timeout_s=0.1,
    )

    def slow_identity(value):
        time.sleep(0.5)
        return value

    result = pool.apply_async(slow_identity, (42,))
    wait_for_condition(lambda: len(pool._running_actor_refs) == 1, timeout=10)
    time.sleep(0.2)
    assert pool._actor_pool[0] is not None
    assert result.get(timeout=10) == 42
    wait_for_condition(lambda: pool._actor_pool[0] is None, timeout=10)
    pool.terminate()


def test_autoscale_maxtasksperchild_replaces_actor(shutdown_only):
    """The batch reaching maxtasksperchild runs before actor replacement."""
    ray.init(num_cpus=1, include_dashboard=False, ignore_reinit_error=True)
    pool = Pool(
        processes=1,
        autoscale=True,
        max_size=1,
        initial_size=0,
        maxtasksperchild=1,
        idle_timeout_s=999,
    )

    def actor_id(_):
        return str(ray.get_runtime_context().get_actor_id())

    actor_ids = pool.map(actor_id, range(4), chunksize=1)
    assert len(set(actor_ids)) == 4
    pool.terminate()


# ---------------------------------------------------------------------------
# End-to-end autoscaling (AutoscalingCluster — the real Ray autoscaler)
# ---------------------------------------------------------------------------


def test_autoscaling_cluster_e2e():
    """Pending num_cpus=1 actors drive the autoscaler to add worker nodes.

    Uses AutoscalingCluster (the same autoscaler KubeRay uses) with a zero-CPU
    head + 4-CPU workers. register_ray(autoscale=True, max_size=8) creates
    pending actors that surface CPU demand; the autoscaler adds workers; the
    work completes.
    """
    import platform

    if platform.system() == "Windows":
        pytest.skip("AutoscalingCluster not supported on Windows.")

    from joblib import Parallel, delayed, parallel_backend

    from ray.cluster_utils import AutoscalingCluster
    from ray.util.joblib import register_ray

    cluster = AutoscalingCluster(
        head_resources={"CPU": 0},
        worker_node_types={
            "cpu_worker": {
                "resources": {"CPU": 4, "object_store_memory": 500 * 1024 * 1024},
                "node_config": {},
                "min_workers": 0,
                "max_workers": 2,
            },
        },
        include_dashboard=False,
    )
    try:
        cluster.start()
        ray.init("auto")

        # The head has no CPUs. All eight actors are pending until the
        # autoscaler launches workers.
        register_ray(autoscale=True, max_size=8, initial_size=0, idle_timeout_s=999)

        def compute(x):
            time.sleep(0.5)
            return x + sum(j * j for j in range(1000))

        with parallel_backend("ray", n_jobs=8):
            results = Parallel()(delayed(compute)(i) for i in range(50))

        offset = sum(j * j for j in range(1000))
        expected = [i + offset for i in range(50)]
        assert results == expected

        wait_for_condition(lambda: ray.cluster_resources()["CPU"] >= 8, timeout=60)
        total_cpus = ray.cluster_resources()["CPU"]
        assert total_cpus >= 8, f"expected 8 CPUs after scaling, got {total_cpus}"

    finally:
        ray.shutdown()
        cluster.shutdown()


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
