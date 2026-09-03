import os
import sys

import joblib
import pytest

import ray
from ray._common.test_utils import SignalActor, wait_for_condition
from ray.util.joblib import register_ray
from ray.util.joblib.ray_backend import RayBackend, _configure_pool_args
from ray.util.multiprocessing import Pool
from ray.util.multiprocessing.pool import (
    PoolTaskError,
    _ActorSlotState,
    _LegacyActorSet,
)


def _state_count(pool, state):
    return sum(
        slot_state is state for slot_state, _outstanding in pool._actor_set.snapshot()
    )


def _current_actor_id():
    return ray.get_runtime_context().get_actor_id()


@pytest.mark.parametrize(
    "options",
    [
        {"max_size": 0},
        {"min_size": -1, "max_size": 1},
        {"min_size": 2, "max_size": 1},
        {"max_size": 1, "idle_timeout_s": -1},
    ],
)
def test_pool_validates_capacity(shutdown_only, options):
    ray.init(num_cpus=1)
    with pytest.raises(ValueError):
        Pool(**options)


@pytest.mark.parametrize("maxtasksperchild", [0, -1, 1.5, True])
def test_pool_validates_maxtasksperchild(maxtasksperchild):
    with pytest.raises(ValueError, match="maxtasksperchild"):
        Pool(max_size=1, maxtasksperchild=maxtasksperchild)
    assert not ray.is_initialized()


@pytest.mark.parametrize(
    ("option", "value"),
    [
        ("get_if_exists", True),
        ("max_concurrency", 2),
        ("max_restarts", 1),
        ("max_task_retries", 1),
    ],
)
def test_adjustable_pool_rejects_incompatible_actor_options(option, value):
    with pytest.raises(ValueError, match=rf"{option}=.*got {value}"):
        Pool(max_size=1, ray_remote_args={option: value})
    assert not ray.is_initialized()


def test_pool_scales_with_work_and_releases_idle_actors(shutdown_only):
    ray.init(num_cpus=2)
    signal = SignalActor.remote()
    pool = Pool(
        min_size=1,
        max_size=2,
        idle_timeout_s=0.05,
        ray_remote_args={"num_cpus": 1},
    )

    def wait_for_signal(signal):
        ray.get(signal.wait.remote())

    results = [pool.apply_async(wait_for_signal, (signal,)) for _ in range(6)]
    wait_for_condition(lambda: _state_count(pool, _ActorSlotState.ACTIVE) == 2)
    ray.get(signal.send.remote())
    assert [result.get(timeout=20) for result in results] == [None] * 6
    wait_for_condition(
        lambda: _state_count(pool, _ActorSlotState.ACTIVE) == 1
        and _state_count(pool, _ActorSlotState.EMPTY) == 1
    )

    pool.close()
    pool.join()


def test_pool_scales_from_and_back_to_zero(shutdown_only):
    ray.init(num_cpus=1)
    pool = Pool(min_size=0, max_size=1, idle_timeout_s=0.05)

    first_actor_id = pool.apply(_current_actor_id)
    wait_for_condition(lambda: _state_count(pool, _ActorSlotState.EMPTY) == 1)
    second_actor_id = pool.apply(_current_actor_id)

    assert first_actor_id != second_actor_id
    pool.close()
    pool.join()


def test_pending_actor_runs_after_active_actor_releases_resource(shutdown_only):
    ray.init(num_cpus=1)
    signal = SignalActor.remote()
    pool = Pool(
        min_size=1,
        max_size=2,
        idle_timeout_s=60,
        ray_remote_args={"num_cpus": 1},
    )

    def wait_and_return_actor_id(signal):
        ray.get(signal.wait.remote())
        return _current_actor_id()

    results = [pool.apply_async(wait_and_return_actor_id, (signal,)) for _ in range(3)]
    wait_for_condition(
        lambda: _state_count(pool, _ActorSlotState.ACTIVE) == 1
        and _state_count(pool, _ActorSlotState.STARTING) == 1
    )
    ray.get(signal.send.remote())
    actor_ids = [result.get(timeout=20) for result in results]

    assert len(set(actor_ids)) == 2
    pool.close()
    pool.join()


def test_close_waits_for_work_assigned_to_pending_actors(shutdown_only):
    ray.init(num_cpus=1)
    pool = Pool(
        min_size=0,
        max_size=2,
        idle_timeout_s=60,
        ray_remote_args={"num_cpus": 1},
    )
    results = [pool.apply_async(abs, (value,)) for value in range(-4, 0)]

    pool.close()

    assert [result.get(timeout=20) for result in results] == [4, 3, 2, 1]
    pool.join()


def test_unschedulable_actor_remains_pending_until_terminated(shutdown_only):
    ray.init(num_cpus=1)
    pool = Pool(
        min_size=0,
        max_size=1,
        idle_timeout_s=0,
        ray_remote_args={"resources": {"unavailable_resource": 1}},
    )

    result = pool.apply_async(abs, (-1,))

    assert not result.ready()
    assert _state_count(pool, _ActorSlotState.STARTING) == 1
    pool.terminate()
    pool.join()


def test_pool_recycles_actor_after_maxtasksperchild(shutdown_only):
    ray.init(num_cpus=1)
    pool = Pool(
        min_size=0,
        max_size=1,
        idle_timeout_s=60,
        maxtasksperchild=2,
    )

    actor_ids = [pool.apply(_current_actor_id) for _ in range(4)]

    assert actor_ids[0] == actor_ids[1]
    assert actor_ids[2] == actor_ids[3]
    assert actor_ids[0] != actor_ids[2]
    pool.close()
    pool.join()


def test_pool_recovers_capacity_after_actor_death(shutdown_only):
    ray.init(num_cpus=1)
    pool = Pool(min_size=0, max_size=1, idle_timeout_s=60)

    with pytest.raises(ray.exceptions.RayError):
        pool.apply(os._exit, (1,))
    wait_for_condition(lambda: _state_count(pool, _ActorSlotState.EMPTY) == 1)
    assert pool.apply(abs, (-1,)) == 1

    pool.close()
    pool.join()


def test_pool_preserves_exceptions_returned_as_values(shutdown_only):
    ray.init(num_cpus=1)
    pool = Pool(min_size=0, max_size=1, idle_timeout_s=60)

    for returned_exception in (
        ValueError("returned as data"),
        PoolTaskError(ValueError("wrapper returned as data")),
    ):

        def return_exception(value=returned_exception):
            return value

        result = pool.apply_async(return_exception)
        value = result.get(timeout=10)
        assert type(value) is type(returned_exception)
        assert result.successful()

    pool.close()
    pool.join()


def test_default_pool_preserves_advanced_actor_options(shutdown_only):
    ray.init(num_cpus=1)
    pool = Pool(processes=1, ray_remote_args={"max_concurrency": 2})

    assert isinstance(pool._actor_set, _LegacyActorSet)
    assert pool.apply(abs, (-1,)) == 1

    pool.close()
    pool.join()


def test_joblib_respects_capacity_and_maxtasksperchild(shutdown_only):
    ray.init(num_cpus=2)
    register_ray()

    backend = RayBackend(
        min_size=0,
        max_size=1,
        idle_timeout_s=60,
        maxtasksperchild=2,
        ray_remote_args={"num_cpus": 1},
    )
    assert backend.configure(n_jobs=2) == 2
    assert backend._pool._actor_set.max_size == 1
    backend.terminate()

    with joblib.parallel_backend(
        "ray",
        n_jobs=2,
        min_size=0,
        max_size=1,
        idle_timeout_s=60,
        maxtasksperchild=2,
        ray_remote_args={"num_cpus": 1},
    ):
        actor_ids = joblib.Parallel(batch_size=1, pre_dispatch=1)(
            joblib.delayed(_current_actor_id)() for _ in range(4)
        )

    assert actor_ids[0] == actor_ids[1]
    assert actor_ids[2] == actor_ids[3]
    assert actor_ids[0] != actor_ids[2]


def test_joblib_backend_can_be_reused_after_task_failure(shutdown_only):
    ray.init(num_cpus=2)
    register_ray()

    def maybe_fail(value):
        if value == 2:
            raise ValueError("expected failure")
        return value

    with (
        pytest.raises(ValueError, match="expected failure"),
        joblib.parallel_backend("ray", n_jobs=2, min_size=0, max_size=2),
    ):
        joblib.Parallel(pre_dispatch=2)(
            joblib.delayed(maybe_fail)(value) for value in range(4)
        )

    with joblib.parallel_backend("ray", n_jobs=2, min_size=0, max_size=2):
        values = joblib.Parallel(pre_dispatch=2)(
            joblib.delayed(abs)(value) for value in range(-4, 0)
        )

    assert values == [4, 3, 2, 1]


def test_joblib_pool_argument_filter():
    assert _configure_pool_args(
        {
            "min_size": 0,
            "idle_timeout_s": 1,
            "maxtasksperchild": 2,
            "temp_folder": "/joblib-only",
            "context": "spawn",
        }
    ) == {"min_size": 0, "idle_timeout_s": 1, "maxtasksperchild": 2}

    with pytest.raises(TypeError, match="unexpected Pool argument: max_sze"):
        _configure_pool_args({"max_sze": 2})


def test_zero_cpu_head_creates_pending_pool_demand():
    from ray.cluster_utils import Cluster

    cluster = Cluster()
    cluster.add_node(num_cpus=0)
    ray.init(address=cluster.address)
    try:
        pool = Pool(
            min_size=0,
            max_size=1,
            idle_timeout_s=60,
            ray_remote_args={"num_cpus": 1},
        )
        result = pool.apply_async(str, ("scaled",))
        assert not result.ready()

        cluster.add_node(num_cpus=1)
        assert result.get(timeout=20) == "scaled"
        pool.close()
        pool.join()
    finally:
        ray.shutdown()
        cluster.shutdown()


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
