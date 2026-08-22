import concurrent.futures
import gc
import os
import queue
import threading
import time
import weakref

import joblib
import pytest

import ray
from ray.util.joblib import register_ray
from ray.util.joblib.ray_backend import RayBackend
from ray.util.multiprocessing import Pool
from ray.util.multiprocessing.pool import (
    PoolTaskError,
    _ElasticActorSet,
    _ElasticSlotState,
)


class _FakeObjectRef:
    def __init__(self):
        self.completion = concurrent.futures.Future()

    def future(self):
        return self.completion


class _FakeRemoteMethod:
    def __init__(self, function):
        self._function = function

    def remote(self, *args):
        return self._function(*args)


class _FakeActor:
    def __init__(self, ready=True):
        self.batch_refs = []
        self.readiness_ref = _FakeObjectRef()
        if ready:
            self.readiness_ref.completion.set_result(None)
        self.exit_ref = _FakeObjectRef()
        self.ping = _FakeRemoteMethod(lambda: self.readiness_ref)
        self.run_batch = _FakeRemoteMethod(self._run_batch)
        self.__ray_terminate__ = _FakeRemoteMethod(lambda: self.exit_ref)

    def _run_batch(self, _func, _batch):
        ref = _FakeObjectRef()
        self.batch_refs.append(ref)
        return ref


def _wait_for(predicate, timeout=10):
    deadline = time.monotonic() + timeout
    while not predicate():
        if time.monotonic() >= deadline:
            raise TimeoutError("condition was not reached")
        time.sleep(0.01)


def _state_count(pool, state):
    return sum(
        slot_state is state
        for slot_state, _outstanding in pool._elastic_actor_set.snapshot()
    )


def test_draining_slot_is_not_reused_before_exit_confirmation():
    actors = []

    def create_actor():
        actor = _FakeActor()
        actors.append(actor)
        return actor

    actor_set = _ElasticActorSet(create_actor, min_size=0, max_size=1, idle_timeout_s=0)
    first_ref = actor_set.submit(None, [])
    first_ref.completion.set_result([])
    _wait_for(lambda: actor_set.snapshot()[0][0] is _ElasticSlotState.DRAINING)

    submitted = threading.Event()

    def submit_again():
        actor_set.submit(None, [])
        submitted.set()

    submitter = threading.Thread(target=submit_again)
    submitter.start()
    assert not submitted.wait(0.05)

    actors[0].exit_ref.completion.set_result(None)
    assert submitted.wait(1)
    assert len(actors) == 2

    actors[1].batch_refs[0].completion.set_result([])
    actor_set.close()
    actors[1].exit_ref.completion.set_result(None)
    actor_set.join()
    submitter.join()


def test_actor_creation_failure_leaves_slot_reusable():
    actor = _FakeActor()
    attempts = 0

    def create_actor():
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise RuntimeError("actor creation failed")
        return actor

    actor_set = _ElasticActorSet(
        create_actor, min_size=0, max_size=1, idle_timeout_s=60
    )

    with pytest.raises(RuntimeError, match="actor creation failed"):
        actor_set.submit(None, [])
    assert actor_set.snapshot() == [(_ElasticSlotState.EMPTY, 0)]

    batch_ref = actor_set.submit(None, [])
    batch_ref.completion.set_result([])
    actor_set.close()
    actor.exit_ref.completion.set_result(None)
    actor_set.join()


def test_starting_slot_becomes_active_only_after_readiness():
    actor = _FakeActor(ready=False)
    actor_set = _ElasticActorSet(
        lambda: actor, min_size=0, max_size=1, idle_timeout_s=60
    )

    batch_ref = actor_set.submit(None, [])
    assert actor_set.snapshot() == [(_ElasticSlotState.STARTING, 1)]

    actor.readiness_ref.completion.set_result(None)
    assert actor_set.snapshot() == [(_ElasticSlotState.ACTIVE, 1)]
    batch_ref.completion.set_result([])
    assert actor_set.snapshot() == [(_ElasticSlotState.ACTIVE, 0)]

    actor_set.close()
    actor.exit_ref.completion.set_result(None)
    actor_set.join()


def test_ready_actor_yields_capacity_to_pending_work():
    actors = [_FakeActor(ready=False), _FakeActor(ready=False)]
    actor_set = _ElasticActorSet(
        lambda: actors.pop(0), min_size=0, max_size=2, idle_timeout_s=60
    )

    first_ref = actor_set.submit(None, [])
    second_ref = actor_set.submit(None, [])
    assert actor_set.snapshot() == [
        (_ElasticSlotState.STARTING, 1),
        (_ElasticSlotState.STARTING, 1),
    ]

    first_actor, second_actor = actor_set._slots[0].actor, actor_set._slots[1].actor
    first_actor.readiness_ref.completion.set_result(None)
    first_ref.completion.set_result([])
    assert actor_set.snapshot() == [
        (_ElasticSlotState.DRAINING, 0),
        (_ElasticSlotState.STARTING, 1),
    ]

    first_actor.exit_ref.completion.set_result(None)
    second_actor.readiness_ref.completion.set_result(None)
    second_ref.completion.set_result([])
    actor_set.close()
    second_actor.exit_ref.completion.set_result(None)
    actor_set.join()


def test_readiness_failure_releases_slot_for_retry():
    failed_actor = _FakeActor(ready=False)
    replacement_actor = _FakeActor()
    actors = [failed_actor, replacement_actor]
    actor_set = _ElasticActorSet(
        lambda: actors.pop(0), min_size=0, max_size=1, idle_timeout_s=60
    )

    actor_set.submit(None, [])
    failed_actor.readiness_ref.completion.set_exception(
        RuntimeError("initializer failed")
    )

    assert actor_set.snapshot() == [(_ElasticSlotState.EMPTY, 0)]
    replacement_ref = actor_set.submit(None, [])
    replacement_ref.completion.set_result([])
    assert actor_set.snapshot() == [(_ElasticSlotState.ACTIVE, 0)]

    actor_set.close()
    replacement_actor.exit_ref.completion.set_result(None)
    actor_set.join()


@pytest.mark.parametrize(
    "options",
    [
        {"max_size": 0},
        {"min_size": -1, "max_size": 1},
        {"min_size": 2, "max_size": 1},
        {"max_size": 1, "idle_timeout_s": -1},
    ],
)
def test_elastic_pool_validates_capacity(shutdown_only, options):
    ray.init(num_cpus=1)
    with pytest.raises(ValueError):
        Pool(**options)


def test_elastic_pool_rejects_maxtasksperchild(shutdown_only):
    ray.init(num_cpus=1)
    with pytest.raises(ValueError, match="maxtasksperchild"):
        Pool(max_size=1, maxtasksperchild=1)


def test_elastic_pool_scales_on_submission_and_reaps_on_idle(shutdown_only):
    ray.init(num_cpus=2)
    pool = Pool(
        min_size=0,
        max_size=2,
        idle_timeout_s=0.05,
        ray_remote_args={"num_cpus": 1},
    )

    results = [pool.apply_async(time.sleep, (0.1,)) for _ in range(6)]
    _wait_for(lambda: _state_count(pool, _ElasticSlotState.ACTIVE) == 2)
    assert [result.get() for result in results] == [None] * 6
    _wait_for(lambda: _state_count(pool, _ElasticSlotState.EMPTY) == 2)

    pool.close()
    pool.join()


def test_elastic_pool_close_preserves_accepted_results(shutdown_only):
    ray.init(num_cpus=2)
    pool = Pool(
        min_size=0,
        max_size=2,
        idle_timeout_s=60,
        ray_remote_args={"num_cpus": 1},
    )
    results = [pool.apply_async(time.sleep, (0.02,)) for _ in range(4)]

    pool.close()

    assert [result.get() for result in results] == [None] * 4
    with pytest.raises(ValueError, match="Pool not running"):
        pool.apply_async(abs, (-1,))
    pool.join()
    assert _state_count(pool, _ElasticSlotState.EMPTY) == 2


def test_elastic_pool_terminate_kills_outstanding_work(shutdown_only):
    ray.init(num_cpus=1)
    pool = Pool(
        min_size=0,
        max_size=1,
        idle_timeout_s=60,
        ray_remote_args={"num_cpus": 1},
    )
    result = pool.apply_async(time.sleep, (30,))
    _wait_for(lambda: _state_count(pool, _ElasticSlotState.ACTIVE) == 1)

    pool.terminate()
    pool.join()

    with pytest.raises(ray.exceptions.RayError):
        result.get()
    assert _state_count(pool, _ElasticSlotState.EMPTY) == 1


def test_elastic_pool_recovers_after_actor_death(shutdown_only):
    ray.init(num_cpus=1)
    pool = Pool(
        min_size=0,
        max_size=1,
        idle_timeout_s=60,
        ray_remote_args={"num_cpus": 1},
    )

    with pytest.raises(ray.exceptions.RayError):
        pool.apply(os._exit, (1,))
    _wait_for(lambda: _state_count(pool, _ElasticSlotState.EMPTY) == 1)
    assert pool.apply(abs, (-1,)) == 1

    pool.close()
    pool.join()


def test_elastic_pool_retries_after_initializer_failure(shutdown_only, tmp_path):
    ray.init(num_cpus=1)
    marker = tmp_path / "initializer-attempted"

    def fail_once(marker_path):
        if not os.path.exists(marker_path):
            open(marker_path, "w").close()
            raise RuntimeError("initializer failed once")

    pool = Pool(
        min_size=0,
        max_size=1,
        idle_timeout_s=60,
        initializer=fail_once,
        initargs=(str(marker),),
    )

    with pytest.raises(ray.exceptions.RayActorError):
        pool.apply_async(abs, (-1,)).get(timeout=20)
    _wait_for(lambda: _state_count(pool, _ElasticSlotState.EMPTY) == 1)
    assert pool.apply(abs, (-2,)) == 2

    pool.close()
    pool.join()


def test_elastic_pool_preserves_exception_values(shutdown_only):
    ray.init(num_cpus=1)
    pool = Pool(min_size=0, max_size=1, idle_timeout_s=60)
    callback_values = queue.Queue()

    for returned_exception in (
        ValueError("returned as data"),
        PoolTaskError(ValueError("wrapper returned as data")),
    ):

        def return_exception(value=returned_exception):
            return value

        result = pool.apply_async(return_exception, callback=callback_values.put)
        value = result.get(timeout=10)
        callback_value = callback_values.get(timeout=10)

        assert type(value) is type(returned_exception)
        assert type(callback_value) is type(returned_exception)
        assert result.successful()

    imap_value = next(pool.imap(lambda _: ValueError("imap data"), [None], chunksize=1))
    assert isinstance(imap_value, ValueError)
    assert str(imap_value) == "imap data"
    pool.close()
    pool.join()


def test_map_variants_stop_at_iteration_end(shutdown_only):
    ray.init(num_cpus=1)
    pool = Pool(min_size=0, max_size=1, idle_timeout_s=60)

    class UnderreportedIterable:
        def __len__(self):
            return 0

        def __iter__(self):
            return iter([1, 2, 3])

    assert pool.map(abs, UnderreportedIterable(), chunksize=1) == [1, 2, 3]
    assert list(pool.imap(abs, UnderreportedIterable(), chunksize=1)) == [1, 2, 3]
    assert pool.starmap(pow, [(2, 3), (3, 2)], chunksize=1) == [8, 9]
    assert pool.map(abs, [], chunksize=None) == []
    assert list(pool.imap(abs, [], chunksize=None)) == []
    pool.close()
    pool.join()


def test_callback_failure_does_not_replace_task_result(shutdown_only):
    ray.init(num_cpus=1)
    pool = Pool(min_size=0, max_size=1, idle_timeout_s=60)
    callback_called = threading.Event()

    def fail():
        raise ZeroDivisionError("task failed")

    def bad_error_callback(_):
        callback_called.set()
        raise RuntimeError("callback failed")

    result = pool.apply_async(fail, error_callback=bad_error_callback)
    assert callback_called.wait(timeout=30)
    with pytest.raises(ZeroDivisionError, match="task failed"):
        result.get(timeout=30)

    pool.close()
    pool.join()


def test_serialization_failures_do_not_poison_elastic_actor(shutdown_only):
    ray.init(num_cpus=1)
    pool = Pool(min_size=0, max_size=1, idle_timeout_s=60)

    with pytest.raises(TypeError):
        pool.apply_async(abs, (threading.Lock(),))
    assert pool.apply(abs, (-2,)) == 2

    def return_unserializable_value():
        return threading.Lock()

    with pytest.raises(ray.exceptions.RayTaskError):
        pool.apply_async(return_unserializable_value).get(timeout=10)
    assert pool.apply(abs, (-3,)) == 3

    pool.close()
    pool.join()


def test_close_with_backlog_preserves_order(shutdown_only):
    ray.init(num_cpus=2)
    pool = Pool(
        min_size=0,
        max_size=2,
        idle_timeout_s=60,
        ray_remote_args={"num_cpus": 1},
    )
    result = pool.map_async(abs, range(-24, 0), chunksize=1)

    pool.close()
    pool.join()

    assert result.get(timeout=30) == list(range(24, 0, -1))
    assert _state_count(pool, _ElasticSlotState.EMPTY) == 2


def test_busy_actor_is_not_reaped_and_idle_capacity_regrows(shutdown_only):
    ray.init(num_cpus=1)
    pool = Pool(min_size=0, max_size=1, idle_timeout_s=0.05)

    result = pool.apply_async(time.sleep, (0.2,))
    _wait_for(lambda: _state_count(pool, _ElasticSlotState.ACTIVE) == 1)
    time.sleep(0.1)
    assert _state_count(pool, _ElasticSlotState.ACTIVE) == 1
    assert result.get(timeout=10) is None
    _wait_for(lambda: _state_count(pool, _ElasticSlotState.EMPTY) == 1)

    for value in (-1, -2, -3):
        assert pool.apply(abs, (value,)) == abs(value)
        _wait_for(lambda: _state_count(pool, _ElasticSlotState.EMPTY) == 1)

    pool.close()
    pool.join()


def test_idle_reaping_preserves_minimum_capacity(shutdown_only):
    ray.init(num_cpus=2)
    pool = Pool(min_size=1, max_size=2, idle_timeout_s=0.05)

    results = [pool.apply_async(time.sleep, (0.1,)) for _ in range(4)]
    assert [result.get(timeout=20) for result in results] == [None] * 4
    _wait_for(
        lambda: _state_count(pool, _ElasticSlotState.ACTIVE) == 1
        and _state_count(pool, _ElasticSlotState.EMPTY) == 1
    )

    pool.close()
    pool.join()


def test_concurrent_driver_submissions_have_no_loss_or_duplication(shutdown_only):
    ray.init(num_cpus=2)
    pool = Pool(
        min_size=0,
        max_size=2,
        idle_timeout_s=60,
        ray_remote_args={"num_cpus": 1},
    )
    outputs = [None] * 4
    errors = []

    def submit(thread_index):
        try:
            start = thread_index * 20
            outputs[thread_index] = pool.map(
                abs, range(-start - 20, -start), chunksize=1
            )
        except Exception as error:
            errors.append(error)

    threads = [threading.Thread(target=submit, args=(index,)) for index in range(4)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert not errors
    assert sorted(value for output in outputs for value in output) == list(range(1, 81))
    pool.close()
    pool.join()


def test_capacity_above_available_resources_does_not_strand_work(shutdown_only):
    ray.init(num_cpus=1)
    pool = Pool(
        min_size=0,
        max_size=4,
        idle_timeout_s=60,
        ray_remote_args={"num_cpus": 1},
    )

    result = pool.map_async(abs, range(-12, 0), chunksize=1)
    try:
        assert result.get(timeout=20) == list(range(12, 0, -1))
    finally:
        pool.terminate()
        pool.join()


def test_imap_error_does_not_stop_later_lazy_submissions(shutdown_only):
    ray.init(num_cpus=1)
    pool = Pool(min_size=0, max_size=1, idle_timeout_s=60)

    def maybe_fail(value):
        if value == 1:
            raise ValueError("user task failed")
        return value

    result = pool.imap(maybe_fail, iter(range(3)), chunksize=1)
    assert result.next(timeout=20) == 0
    error = result.next(timeout=20)
    assert isinstance(error, PoolTaskError)
    assert isinstance(error.underlying, ValueError)
    assert result.next(timeout=20) == 2
    with pytest.raises(StopIteration):
        result.next(timeout=20)

    pool.close()
    pool.join()


def test_closing_lazy_imap_settles_collector(shutdown_only):
    ray.init(num_cpus=1)
    pool = Pool(min_size=0, max_size=1, idle_timeout_s=60)
    result = pool.imap(abs, iter(range(-3, 0)), chunksize=1)
    _wait_for(lambda: result._result_thread._num_ready == 1, timeout=20)

    pool.close()

    assert result.next(timeout=20) == 3
    rejected = result.next(timeout=20)
    assert isinstance(rejected, PoolTaskError)
    assert isinstance(rejected.underlying, ValueError)
    with pytest.raises(StopIteration):
        result.next(timeout=20)
    _wait_for(lambda: not result._result_thread.is_alive())
    pool.join()


def test_abandoned_imap_releases_collector_thread(shutdown_only):
    ray.init(num_cpus=1)
    pool = Pool(min_size=0, max_size=1, idle_timeout_s=60)
    result = pool.imap(abs, iter(range(-100, 0)), chunksize=1)
    result_thread = result._result_thread
    result_ref = weakref.ref(result)
    assert result.next(timeout=20) == 100

    del result
    gc.collect()

    _wait_for(lambda: result_ref() is None and not result_thread.is_alive(), timeout=20)
    pool.terminate()
    pool.join()


def test_fixed_pool_path_is_unchanged(shutdown_only):
    ray.init(num_cpus=2)
    pool = Pool(processes=2)
    callback_values = queue.Queue()

    assert pool._elastic_actor_set is None
    assert pool.map(abs, [-1, -2], chunksize=1) == [1, 2]

    successful = pool.apply_async(abs, (-3,), callback=callback_values.put)
    assert successful.get(timeout=20) == 3
    assert callback_values.get(timeout=20) == 3

    def fail():
        raise ValueError("fixed pool failure")

    failed = pool.apply_async(fail, error_callback=callback_values.put)
    with pytest.raises(ValueError, match="fixed pool failure"):
        failed.get(timeout=20)
    callback_error = callback_values.get(timeout=20)
    assert isinstance(callback_error, ValueError)

    pool.close()
    pool.join()


def test_joblib_uses_n_jobs_as_elastic_ceiling(shutdown_only):
    ray.init(num_cpus=2)
    register_ray()

    backend = RayBackend(
        min_size=0,
        max_size=8,
        idle_timeout_s=0.05,
        ray_remote_args={"num_cpus": 1},
    )
    assert backend.configure(n_jobs=2) == 2
    assert backend._pool._elastic_actor_set.max_size == 2
    backend.terminate()

    with joblib.parallel_backend(
        "ray",
        n_jobs=2,
        min_size=0,
        max_size=8,
        idle_timeout_s=0.05,
        ray_remote_args={"num_cpus": 1},
    ):
        values = joblib.Parallel()(joblib.delayed(abs)(value) for value in range(-4, 0))

    assert values == [4, 3, 2, 1]


def test_joblib_failure_propagates_and_backend_can_be_reused(shutdown_only):
    ray.init(num_cpus=2)
    register_ray()

    def maybe_fail(value):
        if value == 3:
            raise ValueError("expected joblib failure")
        return value

    with (
        pytest.raises(ValueError, match="expected joblib failure"),
        joblib.parallel_backend(
            "ray",
            n_jobs=2,
            min_size=0,
            max_size=2,
            idle_timeout_s=60,
        ),
    ):
        joblib.Parallel(pre_dispatch=2)(
            joblib.delayed(maybe_fail)(value) for value in range(8)
        )

    with joblib.parallel_backend(
        "ray",
        n_jobs=2,
        min_size=0,
        max_size=2,
        idle_timeout_s=60,
    ):
        values = joblib.Parallel(pre_dispatch=2)(
            joblib.delayed(abs)(value) for value in range(-8, 0)
        )

    assert values == list(range(8, 0, -1))


def test_zero_cpu_head_can_create_pending_pool_demand():
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
